'use strict';
// Split the bundled snapshot (data/entities.ndjson.gz) into ordered parts
// data/entities-1.ndjson.gz, entities-2.ndjson.gz, ... each under 20 MB
// gzipped (GitHub's browser-upload limit is 25 MB). The split is on line
// boundaries of the UNCOMPRESSED NDJSON stream, in the original order,
// byte-exact: concatenating the gunzipped parts reproduces the original
// uncompressed stream bit for bit — verified by SHA-256 before the single
// file is removed.
//
// STREAMING implementation (deviation from the B9 original, forced by
// scale): B9's splitter gunzips the whole snapshot into one Buffer, which
// Node caps at 4 GiB — B11's uncompressed NDJSON is 4,327,952,085 bytes, so
// the whole-buffer approach cannot run here at all. This version streams:
// it hashes the uncompressed stream while cutting parts of ~140 MB
// uncompressed (compression varies 10-12% across the register → ≤ ~17 MB
// gz per part; any part that still lands >= 20 MB fails the run loudly),
// and compares the reassembled hash. Same contract, same verification.
// Run after scripts/ingest.js (scripts/refresh.js runs it after every
// ingest); safe to re-run when only the single file or only parts exist.
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');
const readline = require('readline');

const DATA_DIR = path.join(__dirname, '..', 'data');
const SINGLE = path.join(DATA_DIR, 'entities.ndjson.gz');
const PART_RE = /^entities-(\d+)\.ndjson\.gz$/;
const MAX_PART_BYTES = 20 * 1024 * 1024;
const TARGET_UNCOMPRESSED = 140 * 1024 * 1024;

function partPaths() {
  if (!fs.existsSync(DATA_DIR)) return [];
  return fs.readdirSync(DATA_DIR)
    .map((name) => { const m = name.match(PART_RE); return m ? { n: Number(m[1]), p: path.join(DATA_DIR, name) } : null; })
    .filter(Boolean).sort((a, b) => a.n - b.n).map((x) => x.p);
}

function hashStream(stream) {
  return new Promise((resolve, reject) => {
    const h = crypto.createHash('sha256');
    stream.on('data', (c) => h.update(c));
    stream.on('end', () => resolve(h.digest('hex')));
    stream.on('error', reject);
  });
}

async function verifyParts(parts) {
  const { PassThrough } = require('stream');
  const merged = new PassThrough();
  const hashPromise = hashStream(merged);
  for (const p of parts) {
    await new Promise((resolve, reject) => {
      fs.createReadStream(p).pipe(zlib.createGunzip())
        .on('data', (c) => merged.write(c))
        .on('end', resolve).on('error', reject);
    });
  }
  merged.end();
  return hashPromise;
}

async function split() {
  const parts = [];
  const hash = crypto.createHash('sha256');
  let idx = 0;
  let buf = [];
  let bufBytes = 0;
  async function flush() {
    if (!buf.length) return;
    idx += 1;
    const p = path.join(DATA_DIR, `entities-${idx}.ndjson.gz`);
    const gz = zlib.gzipSync(Buffer.concat(buf), { level: 9 });
    if (gz.length >= MAX_PART_BYTES) {
      throw new Error(`split: part ${idx} is ${gz.length} bytes (>= ${MAX_PART_BYTES}) — lower TARGET_UNCOMPRESSED`);
    }
    fs.writeFileSync(p, gz);
    parts.push(p);
    console.log(`split: part ${idx}: ${bufBytes} uncompressed -> ${gz.length} gz bytes`);
    buf = []; bufBytes = 0;
  }
  const rl = readline.createInterface({
    input: fs.createReadStream(SINGLE).pipe(zlib.createGunzip()),
    crlfDelay: Infinity,
  });
  for await (const line of rl) {
    const b = Buffer.from(line + '\n', 'utf8');
    hash.update(b);
    buf.push(b); bufBytes += b.length;
    if (bufBytes >= TARGET_UNCOMPRESSED) await flush();
  }
  await flush();
  return { parts, sha256: hash.digest('hex') };
}

async function main() {
  const existing = partPaths();
  if (!fs.existsSync(SINGLE)) {
    if (!existing.length) throw new Error('split: neither entities.ndjson.gz nor parts found in data/');
    console.log(`split: single file already removed; ${existing.length} parts present (left as-is)`);
    return;
  }
  for (const p of existing) fs.unlinkSync(p);
  const { parts, sha256 } = await split();
  const reassembled = await verifyParts(parts);
  if (reassembled !== sha256) {
    throw new Error(`split: VERIFY FAILED — reassembled sha256 ${reassembled} != original ${sha256}; single file kept`);
  }
  fs.unlinkSync(SINGLE);
  console.log(`split: OK — ${parts.length} parts, reassembled sha256 verified (${sha256.slice(0, 16)}…), single file removed`);
}

if (require.main === module) main().catch((e) => { console.error(e); process.exit(1); });
module.exports = { partPaths, MAX_PART_BYTES };
