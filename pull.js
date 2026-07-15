// Dukascopy puller for Q Trade / qootsi.com.
// Reads job.json, pulls OHLC+volume via dukascopy-node, writes gzipped yearly
// CSVs under data/<instrument>/<instrument>-<tf>-<year>.csv.gz + manifest.json.
//
// Month-aware resumability (self-healing under Dukascopy throttling):
//  - a month inside a year file is considered DONE only if it has >= MIN_ROWS rows
//  - the CURRENT month is always re-pulled (kept fresh)
//  - missing / partial / failed months are re-pulled and MERGED into the year file
//  - bounded: at most maxMonthsPerRun NEW months per run (gentle on Dukascopy)
//  - instruments processed in job.json order (PRIORITY: gold first)
//  - years newest-first
//  - RESILIENT: corrupt gz -> treated empty (re-pull); per-instrument errors isolated;
//    the job NEVER exits non-zero on partial/throttle failures (avoids red CI emails);
//    partial progress is committed by the workflow (Commit step: if: always()).
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
const MIN_ROWS = job.minRowsPerMonth || 5000;   // a full m1 trading month is ~30k; <5k = partial
const fromD = new Date(job.from + 'T00:00:00Z');
const toD = (job.to && job.to !== 'now') ? new Date(job.to + 'T00:00:00Z') : new Date();

const iso = (ts) => new Date(ts).toISOString().replace('.000Z', 'Z');
const causeOf = (e) => (e && e.cause && (e.cause.code || e.cause.message)) || (e && e.message) || String(e);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ym = (dt) => dt.slice(0, 7);                // 'YYYY-MM' from ISO string
const curYM = iso(toD.getTime()).slice(0, 7);
const header = vol ? 'dt,o,h,l,c,v' : 'dt,o,h,l,c';

function readExisting(file) {
  const byMonthCount = {}; const rowsByDt = {};
  if (!fs.existsSync(file)) return { byMonthCount, rowsByDt };
  let txt;
  try { txt = zlib.gunzipSync(fs.readFileSync(file)).toString('utf8'); }
  catch (e) { console.warn('corrupt gz, re-pulling as empty: ' + file + ' (' + ((e && e.message) || e) + ')'); return { byMonthCount, rowsByDt }; }
  for (const line of txt.split('\n')) {
    if (!line || line[0] === 'd') continue;        // skip header/empty
    const dt = line.slice(0, line.indexOf(','));
    if (!dt) continue;
    rowsByDt[dt] = line;
    const k = ym(dt); byMonthCount[k] = (byMonthCount[k] || 0) + 1;
  }
  return { byMonthCount, rowsByDt };
}

async function pullChunk(inst, from, to) {
  let lastErr = 'empty';
  for (let attempt = 1; attempt <= 6; attempt++) {
    try {
      const rows = await getHistoricalRates({
        instrument: inst, dates: { from, to }, timeframe: tf, format: 'array', volumes: vol,
        retries: 6, pauseBetweenRetries: 2500, retryOnEmpty: true,
        batchSize, pauseBetweenBatches, useCache: false
      });
      if (rows && rows.length) return rows;
      lastErr = 'empty';
    } catch (e) { lastErr = causeOf(e); }
    await sleep(4000 * attempt);
  }
  throw new Error(lastErr);
}

(async () => {
  const manifest = {
    generatedAt: new Date().toISOString(),
    resolved: { from: fromD.toISOString(), to: toD.toISOString() },
    timeframe: tf, volumes: vol, node: process.version, batchSize, maxMonthsPerRun,
    instruments: {}
  };
  let monthsPulled = 0;
  for (const inst of job.instruments) {
    let instMonths = 0;   // per-instrument cap: late instruments (e.g. usa500) no longer starve the global budget
    const m = { yearsComplete: [], monthsAdded: [], rows: 0, first: null, last: null, sample: null, errors: [] };
    manifest.instruments[inst] = m;
    try {
    const dir = path.join(outRoot, inst);
    fs.mkdirSync(dir, { recursive: true });
    for (let y = toD.getUTCFullYear(); y >= fromD.getUTCFullYear(); y--) {
      const file = path.join(dir, inst + '-' + tf + '-' + y + '.csv.gz');
      const { byMonthCount, rowsByDt } = readExisting(file);
      let addedThisYear = 0;
      for (let mo = 0; mo < 12; mo++) {
        const cFrom = new Date(Math.max(fromD.getTime(), Date.UTC(y, mo, 1)));
        const cTo = new Date(Math.min(toD.getTime(), Date.UTC(y, mo + 1, 1)));
        if (cFrom >= cTo) continue;
        const key = y + '-' + String(mo + 1).padStart(2, '0');
        const done = (byMonthCount[key] || 0) >= MIN_ROWS && key !== curYM;
        if (done) continue;
        if (instMonths >= maxMonthsPerRun) { m.errors.push(key + ':deferred(cap)'); continue; }
        let rows;
        try { rows = await pullChunk(inst, cFrom, cTo); }
        catch (e) { m.errors.push(key + ':' + e.message); continue; }
        for (const r of rows) { const line = iso(r[0]) + ',' + r.slice(1).join(','); rowsByDt[line.slice(0, line.indexOf(','))] = line; }
        monthsPulled++; instMonths++; addedThisYear++; m.monthsAdded.push(key);
        if (!m.sample) { m.sample = { header: header, head: [iso(rows[0][0]) + ',' + rows[0].slice(1).join(','), iso(rows[1][0]) + ',' + rows[1].slice(1).join(',')], tail: iso(rows[rows.length - 1][0]) + ',' + rows[rows.length - 1].slice(1).join(',') }; }
      }
      const dts = Object.keys(rowsByDt).sort();
      if (dts.length === 0) continue;
      if (addedThisYear > 0) {
        const out = [header].concat(dts.map((k) => rowsByDt[k])).join('\n') + '\n';
        fs.writeFileSync(file, zlib.gzipSync(Buffer.from(out), { level: 9 }));
        console.log(inst + ' ' + y + ': +' + addedThisYear + ' months, total ' + dts.length + ' rows');
      }
      // report coverage
      const monthsPresent = new Set(dts.map((d) => ym(d))).size;
      if (monthsPresent >= 11) m.yearsComplete.push(y);
      m.rows += dts.length;
      if (!m.first || dts[0] < m.first) m.first = dts[0];
      if (!m.last || dts[dts.length - 1] > m.last) m.last = dts[dts.length - 1];
    }
    } catch (e) { m.errors.push('instrument-fatal:' + ((e && e.message) || String(e))); console.error('instrument error', inst, e); }
  }
  manifest.monthsPulledThisRun = monthsPulled;
  fs.writeFileSync('manifest.json', JSON.stringify(manifest, null, 2));
  console.log('DONE (' + monthsPulled + ' months pulled)');
})().catch((e) => {
  console.error('run error (non-fatal, partial progress kept):', e);
  try { if (!fs.existsSync('manifest.json')) { fs.writeFileSync('manifest.json', JSON.stringify({ generatedAt: new Date().toISOString(), fatal: String((e && e.message) || e) }, null, 2)); } } catch (_) {}
  process.exit(0);
});
