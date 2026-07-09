# qootsi-market-data

Private market-data pipeline for **Q Trade / qootsi.com**.

GitHub Actions pulls OHLC + volume from Dukascopy (via [`dukascopy-node`](https://github.com/Leo4815162342/dukascopy-node)) and commits gzipped CSVs under `data/`.

## How it works

1. Edit **`job.json`** (what to pull). Committing it triggers the workflow.
2. The workflow runs `pull.js`, writes `data/<instrument>/<instrument>-<tf>-<year>.csv.gz`, and commits the result back with `[skip ci]`.
3. `manifest.json` summarizes row counts and date coverage per instrument.

You can also run it manually: **Actions → pull-dukascopy → Run workflow**.

### job.json format

```json
{
  "instruments": ["xauusd"],
  "timeframe": "m1",
  "from": "2026-07-01",
  "to": "now",
  "volumes": true
}
```

CSV columns: `dt,o,h,l,c,v` (UTC timestamp, open, high, low, close, volume).

> Note: Dukascopy volume is broker/ECN tick volume (a proxy), not centralized-exchange volume.
