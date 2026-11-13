// Dukascopy puller for Q Trade / qootsi.com — v2 (2026-11-13).
// Reads job.json, pulls OHLC+volume via dukascopy-node, writes gzipped yearly
// CSVs under data/<instrument>/<instrument>-<tf>-<year>.csv.gz + manifest.json.
//
// v2 (2026-11-13) — why the data stopped on 2026-10-08 and again on 2026-11-07:
//  - dukascopy-node 1.51.x reads m1 day by day and THROWS "Request returned an empty dataset" for a day with no trading
//    (every Saturday, holidays) when retryOnEmpty is on. One empty Saturday failed the whole month, so the current month
//    stopped updating from the first Saturday after the library change.
//  - a month counted as DONE at >= 5000 rows, so October 2026 (only Oct 1-8, 7,513 rows) was never re-pulled.
//  - manifest.generatedAt changed on every run, so every run committed even when it added nothing.
// v2 behaviour:
//  - months are pulled DAY BY DAY, Saturdays skipped; an empty day counts as 0 rows, not as a failure
//  - a month is COMPLETE only if it has >= minRowsPerWeekday rows per weekday (default 1000; a full gold day is ~1,380)
//    AND its last bar falls on its last or second-to-last weekday (a holiday may close the month early)
//  - a THIN month is re-pulled whole and merged; a complete-looking month that is still open (the current one) is pulled
//    only from the day of its last bar, so a normal run makes a handful of requests
//  - a real failure (network, WAF) stops that month; rows already fetched are kept and merged, the next run continues
//  - manifest.json is rewritten only when data or errors change, so a run that adds nothing makes no commit
//  - instruments in job.json order (gold first), years newest-first, at most maxMonthsPerRun months per instrument per run
//  - the job NEVER exits non-zero on partial/throttle failures (no red CI emails); partial progress is committed
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { getHistoricalRates } = require('dukascopy-node');

const job = JSON.parse(fs.readFileSync('job.json', 'utf8'));
const tf = job.timeframe || 'm1';
const vol = job.volumes !== false;
const outRoot = job.outDir || 'data';
const batchSize = job.batchSize || 6;
const pauseBetweenBatches = job.pauseBetweenBatches != null ? job.pauseBetweenBatches : 500;
const maxMonthsPerRun = job.maxMonthsPerRun || 10;
const MIN_PER_WEEKDAY = job.minRowsPerWeekday || 1000;
const DAY = 86400000;
const fromD = new Date(job.from + 'T00:00:00Z');
const toD = (job.to && job.to !== 'now') ? new Date(job.to + 'T00:00:00Z') : new Date();

const iso = (ts) => new Date(ts).toISOString().replace('.000Z', 'Z');
const causeOf = (e) => (e && e.cause && (e.cause.code || e.cause.message)) || (e && e.message) || String(e);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ym = (dt) => dt.slice(0, 7);                // 'YYYY-MM' from ISO string
const curYM = iso(toD.getTime()).slice(0, 7);
const header = vol ? 'dt,o,h,l,c,v' : 'dt,o,h,l,c';
const dayStart = (d) => Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());

// Mon-Fri dates in [a, b): count, last and second-to-last date ('YYYY-MM-DD')
function weekdaysIn(a, b) {
  let n = 0; const list = [];
  for (let t = dayStart(a); t < b.getTime(); t += DAY) {
    const d = new Date(t).getUTCDay();
    if (d >= 1 && d <= 5) { n++; list.push(new Date(t).toISOString().slice(0, 10)); }
  }
  return { n, last: list[list.length - 1] || null, prevLast: list[list.length - 2] || list[list.length - 1] || null };
}

function readExisting(file) {
  const byMonthCount = {}; const lastByMonth = {}; const rowsByDt = {};
  if (!fs.existsSync(file)) return { byMonthCount, lastByMonth, rowsByDt };
  let txt;
  try { txt = zlib.gunzipSync(fs.readFileSync(file)).toString('utf8'); }
  catch (e) { console.warn('corrupt gz, re-pulling as empty: ' + file + ' (' + ((e && e.message) || e) + ')'); return { byMonthCount, lastByMonth, rowsByDt }; }
  for (const line of txt.split('\n')) {
    if (!line || line[0] === 'd') continue;        // skip header/empty
    const dt = line.slice(0, line.indexOf(','));
    if (!dt) continue;
    rowsByDt[dt] = line;
    const k = ym(dt); byMonthCount[k] = (byMonthCount[k] || 0) + 1;
    if (!lastByMonth[k] || dt > lastByMonth[k]) lastByMonth[k] = dt;
  }
  return { byMonthCount, lastByMonth, rowsByDt };
}

// one range (one day); an empty day returns [] instead of failing
async function pullChunk(inst, from, to) {
  let lastErr = 'unknown';
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      const rows = await getHistoricalRates({
        instrument: inst, dates: { from, to }, timeframe: tf, format: 'array', volumes: vol,
        retries: 4, pauseBetweenRetries: 2500, retryOnEmpty: true,
        batchSize, pauseBetweenBatches, useCache: false
      });
      return rows || [];
    } catch (e) {
      lastErr = causeOf(e);
      if (/empty dataset/i.test(lastErr)) return [];   // no trading that day (holiday / weekend edge): not a failure
    }
    await sleep(4000 * attempt);
  }
  throw new Error(lastErr);
}

// day by day over [from, to), Saturdays skipped; stops at the first real failure and returns what it has
async function pullDays(inst, from, to) {
  const rows = []; let err = null;
  for (let t = dayStart(from); t < to.getTime(); t += DAY) {
    if (new Date(t).getUTCDay() === 6) continue;     // Saturday: FX, metals and index CFDs are closed all day (UTC)
    const a = new Date(Math.max(t, from.getTime())); const b = new Date(Math.min(t + DAY, to.getTime()));
    if (a >= b) continue;
    try { for (const r of await pullChunk(inst, a, b)) rows.push(r); }
    catch (e) { err = new Date(t).toISOString().slice(0, 10) + ':' + e.message; break; }
  }
  return { rows, err };
}

(async () => {
  let prev = null;
  try { prev = JSON.parse(fs.readFileSync('manifest.json', 'utf8')); } catch (_) { prev = null; }
  const manifest = {
    generatedAt: new Date().toISOString(),
    puller: 'v2 (day by day, Saturdays skipped, completeness by weekday count)',
    resolved: { from: fromD.toISOString(), to: toD.toISOString() },
    timeframe: tf, volumes: vol, node: process.version, batchSize, maxMonthsPerRun, minRowsPerWeekday: MIN_PER_WEEKDAY,
    instruments: {}
  };
  let monthsPulled = 0;
  for (const inst of job.instruments) {
    let instMonths = 0;   // per-instrument cap: late instruments (e.g. usa500) do not starve
    const m = { yearsComplete: [], monthsAdded: [], monthsThin: [], rows: 0, first: null, last: null, errors: [] };
    manifest.instruments[inst] = m;
    try {
      const dir = path.join(outRoot, inst);
      fs.mkdirSync(dir, { recursive: true });
      for (let y = toD.getUTCFullYear(); y >= fromD.getUTCFullYear(); y--) {
        const file = path.join(dir, inst + '-' + tf + '-' + y + '.csv.gz');
        const { byMonthCount, lastByMonth, rowsByDt } = readExisting(file);
        let addedThisYear = 0;
        for (let mo = 0; mo < 12; mo++) {
          const cFrom = new Date(Math.max(fromD.getTime(), Date.UTC(y, mo, 1)));
          const cTo = new Date(Math.min(toD.getTime(), Date.UTC(y, mo + 1, 1)));
          if (cFrom >= cTo) continue;
          const key = y + '-' + String(mo + 1).padStart(2, '0');
          const wd = weekdaysIn(cFrom, cTo);
          if (!wd.n) continue;
          const have = byMonthCount[key] || 0;
          const lastHave = (lastByMonth[key] || '').slice(0, 10);
          const thin = have < wd.n * MIN_PER_WEEKDAY;
          const reachesEnd = !!lastHave && lastHave >= wd.prevLast;
          if (!thin && reachesEnd && key !== curYM) continue;           // complete and closed
          if (instMonths >= maxMonthsPerRun) { m.errors.push(key + ':deferred(cap)'); continue; }
          // a thin month is re-pulled whole; otherwise only from the day of its last bar
          let pFrom = cFrom;
          if (!thin && lastHave) { const t = Date.parse(lastHave + 'T00:00:00Z'); if (t > pFrom.getTime()) pFrom = new Date(t); }
          const { rows, err } = await pullDays(inst, pFrom, cTo);
          if (err) m.errors.push(key + ':' + err);
          let added = 0;
          for (const r of rows) {
            const line = iso(r[0]) + ',' + r.slice(1).join(',');
            const dt = line.slice(0, line.indexOf(','));
            if (!rowsByDt[dt]) added++;
            rowsByDt[dt] = line;
          }
          instMonths++;
          if (added > 0) { monthsPulled++; addedThisYear += added; m.monthsAdded.push(key + ':+' + added); }
        }
        const dts = Object.keys(rowsByDt).sort();
        if (dts.length === 0) continue;
        if (addedThisYear > 0) {
          const out = [header].concat(dts.map((k) => rowsByDt[k])).join('\n') + '\n';
          fs.writeFileSync(file, zlib.gzipSync(Buffer.from(out), { level: 9 }));
          console.log(inst + ' ' + y + ': +' + addedThisYear + ' rows, total ' + dts.length + ' rows');
        }
        // coverage after this run
        const cnt = {};
        for (const d of dts) { const k = ym(d); cnt[k] = (cnt[k] || 0) + 1; }
        if (Object.keys(cnt).length >= 11) m.yearsComplete.push(y);
        for (const [k, c] of Object.entries(cnt)) {
          const [yy, mm] = k.split('-').map(Number);
          const a = new Date(Math.max(fromD.getTime(), Date.UTC(yy, mm - 1, 1))); const b = new Date(Math.min(toD.getTime(), Date.UTC(yy, mm, 1)));
          const w = weekdaysIn(a, b);
          if (w.n && c < w.n * MIN_PER_WEEKDAY && k !== curYM) m.monthsThin.push(k + ':' + c);
        }
        m.rows += dts.length;
        if (!m.first || dts[0] < m.first) m.first = dts[0];
        if (!m.last || dts[dts.length - 1] > m.last) m.last = dts[dts.length - 1];
      }
    } catch (e) { m.errors.push('instrument-fatal:' + ((e && e.message) || String(e))); console.error('instrument error', inst, e); }
    console.log(inst + ': last ' + m.last + ', rows ' + m.rows + (m.errors.length ? ', errors ' + m.errors.join(' | ') : ''));
  }
  manifest.monthsPulledThisRun = monthsPulled;
  // rewrite the manifest only when data or errors changed, so an empty run makes no commit
  const sameAsBefore = prev && prev.puller === manifest.puller && JSON.stringify(prev.instruments) === JSON.stringify(manifest.instruments);
  if (sameAsBefore && monthsPulled === 0) {
    console.log('DONE (no change; manifest left as is)');
  } else {
    fs.writeFileSync('manifest.json', JSON.stringify(manifest, null, 2));
    console.log('DONE (' + monthsPulled + ' months with new rows)');
  }
})().catch((e) => {
  console.error('run error (non-fatal, partial progress kept):', e);
  try { if (!fs.existsSync('manifest.json')) { fs.writeFileSync('manifest.json', JSON.stringify({ generatedAt: new Date().toISOString(), fatal: String((e && e.message) || e) }, null, 2)); } } catch (_) {}
  process.exit(0);
});
