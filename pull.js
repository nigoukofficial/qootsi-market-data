// Dukascopy puller for Q Trade / qootsi.com.
// Reads job.json, pulls OHLC+volume via dukascopy-node, writes gzipped yearly
// CSVs under data/<instrument>/, plus manifest.json.
//
// Design for gradual, throttle-safe backfill:
//  - instruments processed in job.json order (PRIORITY: gold first, then indices/fx)
//  - years newest-first (most useful data lands first)
//  - resumable: skips year files that already exist
//  - bounded: writes at most maxFilesPerRun new year-files per run, then stops
//  - robust: month-chunked, retryOnEmpty, per-chunk retry with backoff (rides Dukascopy throttling)
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
const maxFilesPerRun = job.maxFilesPerRun || 3;
const fromD = new Date(job.from + 'T00:00:00Z');
const toD = (job.to && job.to !== 'now') ? new Date(job.to + 'T00:00:00Z') : new Date();

const iso = (ts) => new Date(ts).toISOString().replace('.000Z', 'Z');
const causeOf = (e) => (e && e.cause && (e.cause.code || e.cause.message)) || (e && e.message) || String(e);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const monthStart = (y, mo) => new Date(Date.UTC(y, mo, 1));

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
    timeframe: tf, volumes: vol, node: process.version, batchSize, maxFilesPerRun, instruments: {}
  };
  const header = vol ? 'dt,o,h,l,c,v' : 'dt,o,h,l,c';
  let filesWritten = 0;
  for (const inst of job.instruments) {
    const m = { files: [], rows: 0, first: null, last: null, sample: null, errors: [] };
    manifest.instruments[inst] = m;
    const dir = path.join(outRoot, inst);
    fs.mkdirSync(dir, { recursive: true });
    for (let y = toD.getUTCFullYear(); y >= fromD.getUTCFullYear(); y--) {   // newest first
      const file = path.join(dir, inst + '-' + tf + '-' + y + '.csv.gz');
      if (fs.existsSync(file)) { m.files.push(path.basename(file) + '(exists)'); continue; }
      if (filesWritten >= maxFilesPerRun) { m.errors.push(y + ':deferred(cap)'); continue; }
      const yearLines = [header];
      let yearRows = 0, yFirst = null, yLast = null;
      for (let mo = 0; mo < 12; mo++) {
        const cFrom = new Date(Math.max(fromD.getTime(), monthStart(y, mo).getTime()));
        const cTo = new Date(Math.min(toD.getTime(), monthStart(y, mo + 1).getTime()));
        if (cFrom >= cTo) continue;
        let rows;
        try { rows = await pullChunk(inst, cFrom, cTo); }
        catch (e) { m.errors.push(y + '-' + (mo + 1) + ':' + e.message); continue; }
        for (const r of rows) yearLines.push(iso(r[0]) + ',' + r.slice(1).join(','));
        yearRows += rows.length;
        const f0 = iso(rows[0][0]); const f1 = iso(rows[rows.length - 1][0]);
        if (!yFirst || f0 < yFirst) yFirst = f0;
        if (!yLast || f1 > yLast) yLast = f1;
        if (!m.sample) m.sample = { header: header, head: [yearLines[1], yearLines[2]], tail: yearLines[yearLines.length - 1] };
      }
      if (yearRows === 0) { m.errors.push(y + ':empty-year'); continue; }
      fs.writeFileSync(file, zlib.gzipSync(Buffer.from(yearLines.join('\n') + '\n'), { level: 9 }));
      filesWritten++;
      m.files.push(path.basename(file));
      m.rows += yearRows;
      if (!m.first || yFirst < m.first) m.first = yFirst;
      if (!m.last || yLast > m.last) m.last = yLast;
      console.log(inst + ' ' + y + ': ' + yearRows + ' rows (file ' + filesWritten + '/' + maxFilesPerRun + ')');
    }
  }
  manifest.filesWrittenThisRun = filesWritten;
  fs.writeFileSync('manifest.json', JSON.stringify(manifest, null, 2));
  console.log('DONE (' + filesWritten + ' new files)');
})().catch((e) => { console.error(e); process.exit(1); });
