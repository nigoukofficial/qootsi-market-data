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
const fromD = new Date(job.from + 'T00:00:00Z');
const toD = (job.to && job.to !== 'now') ? new Date(job.to + 'T00:00:00Z') : new Date();

const iso = (ts) => new Date(ts).toISOString().replace('.000Z', 'Z');

(async () => {
  const manifest = {
    generatedAt: new Date().toISOString(),
    runnerNow: new Date().toISOString(),
    resolved: { from: fromD.toISOString(), to: toD.toISOString() },
    timeframe: tf, volumes: vol, instruments: {}
  };
  for (const inst of job.instruments) {
    const m = { files: [], rows: 0, first: null, last: null, errors: [] };
    manifest.instruments[inst] = m;
    const dir = path.join(outRoot, inst);
    fs.mkdirSync(dir, { recursive: true });
    for (let y = fromD.getUTCFullYear(); y <= toD.getUTCFullYear(); y++) {
      const cFrom = new Date(Math.max(fromD.getTime(), Date.UTC(y, 0, 1)));
      const cTo = new Date(Math.min(toD.getTime(), Date.UTC(y + 1, 0, 1)));
      if (cFrom >= cTo) continue;
      let rows;
      try {
        rows = await getHistoricalRates({
          instrument: inst,
          dates: { from: cFrom, to: cTo },
          timeframe: tf,
          format: 'array',
          volumes: vol,
          retries: 5,
          pauseBetweenRetries: 800,
          batchSize: 20,
          pauseBetweenBatches: 400,
          useCache: false
        });
      } catch (e) {
        console.error('FAIL ' + inst + ' ' + y + ': ' + e.message);
        m.errors.push(y + ':' + e.message);
        continue;
      }
      if (!rows || !rows.length) { console.log('empty ' + inst + ' ' + y); m.errors.push(y + ':empty'); continue; }
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
      console.log(inst + ' ' + y + ': ' + rows.length + ' rows -> ' + path.basename(file));
    }
  }
  fs.writeFileSync('manifest.json', JSON.stringify(manifest, null, 2));
  console.log('DONE');
})().catch((e) => { console.error(e); process.exit(1); });
