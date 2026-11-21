'use strict';
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const readline = require('readline');
// Snapshot format: gzipped NDJSON, shipped as ordered parts
// data/entities-1.ndjson.gz, entities-2.ndjson.gz, ... (the single file
// would be several hundred MB gzipped — far over GitHub's 25 MB
// browser-upload limit, and this portfolio publishes via browser upload).
// Concatenating the gunzipped parts in filename order reproduces the
// original stream byte-exactly (split on line boundaries by
// scripts/split-snapshot.js, SHA-256-verified at split time). Line 1 of
// part 1 is {"__meta__": {...}}; each following line is one entity row.
//
// Deliberate deviation from B9's loader: B9 concatenates all gunzipped
// parts into a single Buffer before parsing. At ~3.3M rows the
// uncompressed stream is >2 GB, so this loader streams — gunzip per part,
// line-wise parse, one row materialised at a time. The row array still
// accumulates (the Actor filters in memory, like B6/B9), but no second
// full-size buffer is ever held.
function snapshotPartPaths(dataDir) {
  return fs.readdirSync(dataDir)
    .filter((n) => /^entities-\d+\.ndjson\.gz$/.test(n))
    .sort((a, b) => Number(a.match(/\d+/)[0]) - Number(b.match(/\d+/)[0]))
    .map((n) => path.join(dataDir, n));
}

async function loadSnapshotParts(gzPaths, onProgress) {
  if (!gzPaths.length) throw new Error('loadSnapshotParts: no snapshot part files given');
  let meta = null;
  const rows = [];
  for (const p of gzPaths) {
    const rl = readline.createInterface({
      input: fs.createReadStream(p).pipe(zlib.createGunzip()),
      crlfDelay: Infinity,
    });
    for await (const line of rl) {
      if (!line) continue;
      if (!meta) meta = JSON.parse(line).__meta__;
      else rows.push(JSON.parse(line));
    }
    if (onProgress) onProgress(p, rows.length);
  }
  return { meta, rows };
}

function loadBundledSnapshot(dataDir, onProgress) {
  return loadSnapshotParts(snapshotPartPaths(dataDir), onProgress);
}

// Synchronous loader for a SINGLE small gzipped NDJSON file (test fixtures
// and tooling only — the bundled snapshot is always multi-part).
function loadSnapshot(gzPath) {
  const buf = zlib.gunzipSync(fs.readFileSync(gzPath));
  let meta = null;
  const rows = [];
  let start = 0;
  for (let i = 0; i <= buf.length; i++) {
    if (i === buf.length || buf[i] === 0x0a) {
      if (i > start) {
        const line = buf.toString('utf8', start, i);
        if (!meta) meta = JSON.parse(line).__meta__;
        else rows.push(JSON.parse(line));
      }
      start = i + 1;
    }
  }
  return { meta, rows };
}

module.exports = { loadSnapshot, loadSnapshotParts, loadBundledSnapshot, snapshotPartPaths };
