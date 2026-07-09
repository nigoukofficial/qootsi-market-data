// Dukascopy puller — reads job.json, pulls OHLC+volume via dukascopy-node,
// writes gzipped yearly CSVs under data/<instrument>/, plus manifest.json.
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { getHistoricalRates } = require('dukascopy-node');

const job = JSON.parse(fs.readFileSync('job.json', 'utf8'));
const tf = job.timeframe || 'm1';
const vol = job.volumes !== false;
const outRoot = job.outDir || 'data';
const batchSize = job.batchSize || 1;
const pauseBetweenBatches = job.pauseBetweenBatches != null ? job.pauseBetweenBatches : 400;
const fromD = new Date(job.from + 'T00:00:00Z');
const toD = (job.to && job.to !== 'now') ? new Date(job.to + 'T00:00:00Z') : new Date();

const iso = (ts) => new Date(ts).toISOString().replace('.000Z', 'Z');
const causeOf = (e) => (e && e.cause && (e.cause.code || e.cause.message)) || (e && e.message) || String(e);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function pull(inst, from, to) {
  return await getHistoricalRates({
    instrument: inst, dates: { from, to }, timeframe: tf, format: 'array', volumes: vol,
    retries: 10, pauseBetweenRetries: 2000, retryOnEmpty: true,
    batchSize, pauseBetweenBatches, useCache: false
  });
}

// Year-level guard: if a year comes back empty, wait and retry the whole year a few times.
async function pullYear(inst, from, to) {
  for (let attempt = 1; attempt <= 4; attempt++) {
    const rows = await pull(inst, from, to);
    if (rows && rows.length) return rows;
    await sleep(3000 * attempt);
  }
  return [];
}

(async () => {
  const manifest = {
    generatedAt: new Date().toISOString(),
    resolved: { from: fromD.toISOString(), to: toD.toISOString() },
    timeframe: tf, volumes: vol, node: process.version, batchSize, instruments: {}
  };
  for (const inst of job.instruments) {
    const m = { files: [], rows: 0, first: null, last: null, sample: null, errors: [] };
    manifest.instruments[inst] = m;
    const dir = path.join(outRoot, inst);
    fs.mkdirSync(dir, { recursive: true });
    for (let y = fromD.getUTCFullYear(); y <= toD.getUTCFullYear(); y++) {
      const cFrom = new Date(Math.max(fromD.getTime(), Date.UTC(y, 0, 1)));
      const cTo = new Date(Math.min(toD.getTime(), Date.UTC(y + 1, 0, 1)));
      if (cFrom >= cTo) continue;
      let rows;
      try {
        rows = await pullYear(inst, cFrom, cTo);
      } catch (e) {
        console.error('FAIL ' + inst + ' ' + y + ': ' + causeOf(e));
        m.errors.push(y + ':' + causeOf(e));
        continue;
      }
      if (!rows || !rows.length) { m.errors.push(y + ':empty'); continue; }
      const header = vol ? 'dt,o,h,l,c,v' : 'dt,o,h,l,c';
      const lines = [header];
      for (const r of rows) lines.push(iso(r[0]) + ',' + r.slice(1).join(','));
      const csv = lines.join('\n') + '\n';
      const file = path.join(dir, inst + '-' + tf + '-' + y + '.csv.gz');
      fs.writeFileSync(file, zlib.gzipSync(Buffer.from(csv), { level: 9 }));
      m.files.push(path.basename(file));
      m.rows += rows.length;
      const f0 = iso(rows[0][0]);
      const f1 = iso(rows[rows.length - 1][0]);
      if (!m.first || f0 < m.first) m.first = f0;
      if (!m.last || f1 > m.last) m.last = f1;
      if (!m.sample) m.sample = { header: header, head: [lines[1], lines[2]], tail: lines[lines.length - 1] };
      console.log(inst + ' ' + y + ': ' + rows.length + ' rows -> ' + path.basename(file));
    }
  }
  fs.writeFileSync('manifest.json', JSON.stringify(manifest, null, 2));
  console.log('DONE');
})().catch((e) => { console.error(e); process.exit(1); });
