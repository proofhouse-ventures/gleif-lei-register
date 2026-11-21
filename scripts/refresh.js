'use strict';
// Refresh: fetch the latest GLEIF Golden Copy publish (LEI-CDF, RR-CDF and
// Reporting Exceptions, CSV format), re-ingest, re-split. GLEIF republishes
// the Golden Copy three times daily; this product refreshes daily — every
// row carries the snapshot date so the vintage is never ambiguous.
// Access is keyless and public: the file list comes from the same public
// JSON API that gleif.org's own Golden Copy download page uses
// (leidata-preview.gleif.org/api/v2/golden-copies/publishes), and the files
// are served from GLEIF's storage CDN. No sign-in, no API key, no payment.
// Note: downloads occasionally drop mid-file — downloads go to a .part file
// with retries and are renamed only when the byte size matches the size
// GLEIF publishes for the file.
// Run manually or from a scheduled job: node scripts/refresh.js [rawDir]
const https = require('https');
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

const UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36';
const API = 'https://leidata-preview.gleif.org/api/v2/golden-copies/publishes?page=1&per_page=1';

function getJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': UA } }, (res) => {
      let b = '';
      res.on('data', (c) => (b += c));
      res.on('end', () => { try { resolve(JSON.parse(b)); } catch (e) { reject(e); } });
    }).on('error', reject);
  });
}
function download(url, dest) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': UA } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400) return download(res.headers.location, dest).then(resolve, reject);
      if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      const ws = fs.createWriteStream(dest);
      res.pipe(ws);
      ws.on('finish', () => ws.close(resolve));
      ws.on('error', reject);
    }).on('error', reject);
  });
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const rawDir = process.argv[2] || path.join(__dirname, '..', '..', 'data', 'raw', 'gleif');
  fs.mkdirSync(rawDir, { recursive: true });
  const list = await getJson(API);
  const pub = list.data && list.data[0];
  if (!pub) throw new Error('refresh: publish list empty');
  const stamp = `${pub.publish_date} UTC`;
  console.log(`refresh: latest Golden Copy publish ${stamp}`);
  for (const kind of ['lei2', 'rr', 'repex']) {
    const info = pub[kind] && pub[kind].full_file && pub[kind].full_file.csv;
    if (!info) throw new Error(`refresh: no CSV full file for ${kind} in publish ${stamp}`);
    const zipName = path.basename(new URL(info.url).pathname);
    const zipPath = path.join(rawDir, zipName);
    let done = false;
    for (let attempt = 0; attempt < 5 && !done; attempt++) {
      try {
        await download(info.url, zipPath + '.part');
        const size = fs.statSync(zipPath + '.part').size;
        if (size !== info.size) throw new Error(`size mismatch for ${zipName}: got ${size}, expected ${info.size}`);
        fs.renameSync(zipPath + '.part', zipPath);
        done = true;
        console.log(`refresh: downloaded ${zipName} (${size} bytes, ${info.record_count} records)`);
      } catch (e) {
        console.log(`refresh: ${kind} attempt ${attempt} failed: ${e.message}`);
        await sleep(5000 * (attempt + 1));
      }
    }
    if (!done) throw new Error(`refresh: download failed for ${zipName} after 5 attempts`);
    execFileSync('unzip', ['-o', zipPath, '-d', rawDir], { stdio: 'inherit' });
  }
  const csvName = fs.readdirSync(rawDir)
    .filter((n) => n.includes('-gleif-goldencopy-lei2-golden-copy.csv'))
    .sort().pop();
  const dm = csvName.match(/^(\d{4})(\d{2})(\d{2})-/);
  const snapshotDate = `${dm[1]}-${dm[2]}-${dm[3]}`;
  execFileSync('node', ['--max-old-space-size=5120', path.join(__dirname, 'ingest.js'), rawDir, snapshotDate, stamp], { stdio: 'inherit' });
  execFileSync('node', [path.join(__dirname, 'split-snapshot.js')], { stdio: 'inherit' });
  console.log('refresh: complete — re-run `npm run verify` before any re-publication');
})().catch((e) => { console.error(e); process.exit(1); });
