'use strict';
// Ingest: build the bundled snapshot from the three GLEIF Golden Copy CSVs
// (LEI-CDF Level 1, RR-CDF Level 2 relationships, Reporting Exceptions).
//   node scripts/ingest.js [rawDir] [snapshotDate] [publishStamp]
// rawDir defaults to ../../data/raw/gleif resolved from scripts/ — i.e. the
// goal-level build/data/raw/gleif, one level above this build directory —
// holding the unzipped CSVs named
//   <yyyymmdd>-<hhmm>-gleif-goldencopy-{lei2,rr,repex}-golden-copy.csv
// snapshotDate defaults to the date parsed from the LEI filename;
// publishStamp is the Golden Copy publish time (e.g. "2026-11-21 16:00 UTC")
// recorded in the snapshot metadata.
//
// Pipeline (streaming throughout; the LEI file is 5.1 GB):
//   pass 1 — RR-CDF: ACTIVE relationship maps (direct/ultimate parent,
//            fund manager, subfund-of, head office) + consolidation pairs.
//   pass 2 — RepEx: per-LEI direct/ultimate parent exception reasons.
//   pass 3 — LEI-CDF: scope rule + normalise + overlay; rows written to a
//            temp NDJSON; the shipped LEI set is collected.
//   pass 4 — children counts (shipped start-nodes per parent) injected by
//            streaming the temp NDJSON into the final NDJSON.
// The snapshot is then split into <20 MB gzipped parts by
// scripts/split-snapshot.js (invoked from scripts/refresh.js and manually).
// The ingest self-checks: written rows == shipped count from the scope
// pass, or it exits non-zero.
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { streamCsvRecords, exclusionReason, normaliseRecord } = require('../src/lib');

const LEI_WANTED = new Set([
  'LEI', 'Entity.LegalName',
  'Entity.OtherEntityNames.OtherEntityName.1', 'Entity.OtherEntityNames.OtherEntityName.1.type',
  'Entity.OtherEntityNames.OtherEntityName.2', 'Entity.OtherEntityNames.OtherEntityName.2.type',
  'Entity.OtherEntityNames.OtherEntityName.3', 'Entity.OtherEntityNames.OtherEntityName.3.type',
  'Entity.OtherEntityNames.OtherEntityName.4', 'Entity.OtherEntityNames.OtherEntityName.4.type',
  'Entity.OtherEntityNames.OtherEntityName.5', 'Entity.OtherEntityNames.OtherEntityName.5.type',
  'Entity.LegalAddress.FirstAddressLine', 'Entity.LegalAddress.AddressNumber',
  'Entity.LegalAddress.AddressNumberWithinBuilding', 'Entity.LegalAddress.MailRouting',
  'Entity.LegalAddress.AdditionalAddressLine.1', 'Entity.LegalAddress.AdditionalAddressLine.2',
  'Entity.LegalAddress.AdditionalAddressLine.3', 'Entity.LegalAddress.City',
  'Entity.LegalAddress.Region', 'Entity.LegalAddress.Country', 'Entity.LegalAddress.PostalCode',
  'Entity.HeadquartersAddress.FirstAddressLine', 'Entity.HeadquartersAddress.AddressNumber',
  'Entity.HeadquartersAddress.AddressNumberWithinBuilding', 'Entity.HeadquartersAddress.MailRouting',
  'Entity.HeadquartersAddress.AdditionalAddressLine.1', 'Entity.HeadquartersAddress.AdditionalAddressLine.2',
  'Entity.HeadquartersAddress.AdditionalAddressLine.3', 'Entity.HeadquartersAddress.City',
  'Entity.HeadquartersAddress.Region', 'Entity.HeadquartersAddress.Country', 'Entity.HeadquartersAddress.PostalCode',
  'Entity.RegistrationAuthority.RegistrationAuthorityID',
  'Entity.RegistrationAuthority.RegistrationAuthorityEntityID',
  'Entity.LegalJurisdiction', 'Entity.EntityCategory', 'Entity.EntitySubCategory',
  'Entity.LegalForm.EntityLegalFormCode', 'Entity.LegalForm.OtherLegalForm',
  'Entity.EntityStatus', 'Entity.EntityCreationDate',
  'Entity.SuccessorEntity.1.SuccessorLEI', 'Entity.SuccessorEntity.2.SuccessorLEI',
  'Entity.SuccessorEntity.3.SuccessorLEI', 'Entity.SuccessorEntity.4.SuccessorLEI',
  'Entity.SuccessorEntity.5.SuccessorLEI',
  'Registration.InitialRegistrationDate', 'Registration.LastUpdateDate',
  'Registration.RegistrationStatus', 'Registration.NextRenewalDate',
  'Registration.ManagingLOU', 'ConformityFlag',
]);
const LEI_EXPECTED = [
  'LEI', 'Entity.LegalName', 'Entity.EntityCategory', 'Entity.LegalJurisdiction',
  'Entity.EntityStatus', 'Registration.RegistrationStatus', 'Registration.ManagingLOU',
];
const RR_WANTED = new Set([
  'Relationship.StartNode.NodeID', 'Relationship.EndNode.NodeID',
  'Relationship.RelationshipType', 'Relationship.RelationshipStatus',
]);
const REPEX_WANTED = new Set([
  'LEI', 'Exception.Category', 'Exception.Reason.1', 'Exception.Reason.2',
  'Exception.Reason.3', 'Exception.Reason.4', 'Exception.Reason.5',
]);

function findFile(rawDir, kind) {
  const hit = fs.readdirSync(rawDir).find((n) => n.includes(`-gleif-goldencopy-${kind}-golden-copy.csv`));
  if (!hit) throw new Error(`ingest: no ${kind} Golden Copy CSV in ${rawDir}`);
  return path.join(rawDir, hit);
}

async function main() {
  const rawDir = process.argv[2] || path.join(__dirname, '..', '..', 'data', 'raw', 'gleif');
  const leiPath = findFile(rawDir, 'lei2');
  const rrPath = findFile(rawDir, 'rr');
  const repexPath = findFile(rawDir, 'repex');
  const base = path.basename(leiPath);
  const m = base.match(/^(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})-/);
  if (!m) throw new Error(`ingest: cannot parse snapshot date from ${base}`);
  const snapshotDate = process.argv[3] || `${m[1]}-${m[2]}-${m[3]}`;
  const publishStamp = process.argv[4] || `${snapshotDate} ${m[4]}:${m[5]} UTC`;
  console.log(`ingest: snapshot_date=${snapshotDate} publish=${publishStamp}`);
  console.log(`ingest: lei=${leiPath}\n        rr=${rrPath}\n        repex=${repexPath}`);

  // ---- pass 1: RR-CDF -----------------------------------------------------
  const directParent = new Map(); const ultimateParent = new Map();
  const fundManager = new Map(); const subfundOf = new Map(); const headOffice = new Map();
  const directPairs = []; const ultimatePairs = [];
  const rrByType = {}; let rrRows = 0; let rrActive = 0; let rrConflicts = 0;
  const typeToMap = {
    IS_DIRECTLY_CONSOLIDATED_BY: directParent,
    IS_ULTIMATELY_CONSOLIDATED_BY: ultimateParent,
    IS_FUND_MANAGED_BY: fundManager,
    IS_SUBFUND_OF: subfundOf,
    IS_INTERNATIONAL_BRANCH_OF: headOffice,
  };
  // The Golden Copy CSV spells the fund-manager type with a hyphen:
  // IS_FUND-MANAGED_BY. Normalise hyphens to underscores for the map lookup.
  for await (const r of streamCsvRecords(rrPath, RR_WANTED, [...RR_WANTED])) {
    rrRows += 1;
    rrByType[r['Relationship.RelationshipType']] = (rrByType[r['Relationship.RelationshipType']] || 0) + 1;
    if (r['Relationship.RelationshipStatus'] !== 'ACTIVE') continue;
    rrActive += 1;
    const start = r['Relationship.StartNode.NodeID'];
    const end = r['Relationship.EndNode.NodeID'];
    const type = r['Relationship.RelationshipType'].replace(/-/g, '_');
    const map = typeToMap[type];
    if (!map || !start || !end) continue;
    if (map.has(start) && map.get(start) !== end) rrConflicts += 1;
    else map.set(start, end);
    if (type === 'IS_DIRECTLY_CONSOLIDATED_BY') directPairs.push([start, end]);
    if (type === 'IS_ULTIMATELY_CONSOLIDATED_BY') ultimatePairs.push([start, end]);
  }
  console.log(`ingest: RR rows=${rrRows} active=${rrActive} directParents=${directParent.size} ultimateParents=${ultimateParent.size} conflicts=${rrConflicts}`);

  // ---- pass 2: RepEx ------------------------------------------------------
  // Reasons are interned as a \u0001-joined string, not an array: ~6.5M
  // exception records cover nearly every entity, and array values in two
  // Maps this size OOM'd the first ingest run. Strings keep the two maps
  // near ~0.7 GB combined.
  const directEx = new Map(); const ultimateEx = new Map();
  let repexRows = 0;
  for await (const r of streamCsvRecords(repexPath, REPEX_WANTED, [...REPEX_WANTED])) {
    repexRows += 1;
    const reasons = [1, 2, 3, 4, 5].map((i) => r[`Exception.Reason.${i}`]).filter(Boolean);
    if (!reasons.length) continue;
    if (r['Exception.Category'] === 'DIRECT_ACCOUNTING_CONSOLIDATION_PARENT') directEx.set(r['LEI'], reasons.join('\u0001'));
    else if (r['Exception.Category'] === 'ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT') ultimateEx.set(r['LEI'], reasons.join('\u0001'));
  }
  console.log(`ingest: RepEx rows=${repexRows} directExceptions=${directEx.size} ultimateExceptions=${ultimateEx.size}`);

  // ---- pass 3: LEI-CDF ----------------------------------------------------
  const dataDir = path.join(__dirname, '..', 'data');
  fs.mkdirSync(dataDir, { recursive: true });
  const tmpPath = path.join(dataDir, '.entities.tmp.ndjson');
  const out = fs.createWriteStream(tmpPath);
  const shipped = new Set();
  const excluded = {};
  let rawRows = 0; let shippedRows = 0;
  let withDirect = 0; let withUltimate = 0;
  for await (const r of streamCsvRecords(leiPath, LEI_WANTED, LEI_EXPECTED)) {
    rawRows += 1;
    const reason = exclusionReason(r);
    if (reason) { excluded[reason] = (excluded[reason] || 0) + 1; continue; }
    const lei = r['LEI'];
    const row = normaliseRecord(r, {
      directParent: directParent.get(lei) || null,
      ultimateParent: ultimateParent.get(lei) || null,
      fundManager: fundManager.get(lei) || null,
      subfundOf: subfundOf.get(lei) || null,
      headOffice: headOffice.get(lei) || null,
      directExceptionReasons: directEx.has(lei) ? directEx.get(lei).split('\u0001') : [],
      ultimateExceptionReasons: ultimateEx.has(lei) ? ultimateEx.get(lei).split('\u0001') : [],
    }, snapshotDate);
    shipped.add(lei);
    if (row.direct_parent_lei) withDirect += 1;
    if (row.ultimate_parent_lei) withUltimate += 1;
    out.write(JSON.stringify(row) + '\n');
    shippedRows += 1;
    if (rawRows % 500000 === 0) console.log(`ingest: LEI pass at ${rawRows} raw, ${shippedRows} shipped`);
  }
  await new Promise((res) => out.end(res));
  console.log(`ingest: LEI rows=${rawRows} shipped=${shippedRows} excluded=${JSON.stringify(excluded)}`);

  // ---- pass 4: children counts --------------------------------------------
  const directChildren = new Map(); const ultimateChildren = new Map();
  for (const [s, e] of directPairs) if (shipped.has(s)) directChildren.set(e, (directChildren.get(e) || 0) + 1);
  for (const [s, e] of ultimatePairs) if (shipped.has(s)) ultimateChildren.set(e, (ultimateChildren.get(e) || 0) + 1);
  const meta = {
    snapshot_date: snapshotDate,
    golden_copy_publish: publishStamp,
    source_files: { lei2: base, rr: path.basename(rrPath), repex: path.basename(repexPath) },
    raw_rows: rawRows,
    rows: shippedRows,
    excluded,
    rr_rows: rrRows,
    rr_by_type: rrByType,
    repex_rows: repexRows,
    entities_with_direct_parent: withDirect,
    entities_with_ultimate_parent: withUltimate,
    licence: 'CC0 1.0 Universal (GLEIF LEI Data Terms of Use)',
    source_dataset: 'GLEIF Golden Copy (LEI-CDF, RR-CDF, Reporting Exceptions)',
  };
  const finalPath = path.join(dataDir, 'entities.ndjson');
  const fin = fs.createWriteStream(finalPath);
  fin.write(JSON.stringify({ __meta__: meta }) + '\n');
  let written = 0;
  const rl = readline.createInterface({ input: fs.createReadStream(tmpPath), crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    const row = JSON.parse(line);
    row.direct_children_count = directChildren.get(row.lei) || 0;
    row.ultimate_children_count = ultimateChildren.get(row.lei) || 0;
    fin.write(JSON.stringify(row) + '\n');
    written += 1;
  }
  await new Promise((res) => fin.end(res));
  fs.unlinkSync(tmpPath);
  if (written !== shippedRows) {
    throw new Error(`ingest: SELF-CHECK FAILED — wrote ${written} rows, scope pass shipped ${shippedRows}`);
  }
  // Gzip the single NDJSON (split-snapshot.js then parts it, verifying the
  // reassembled hash before removing the single file).
  const zlib = require('zlib');
  await new Promise((res, rej) => {
    fs.createReadStream(finalPath).pipe(zlib.createGzip({ level: 9 }))
      .pipe(fs.createWriteStream(path.join(dataDir, 'entities.ndjson.gz')))
      .on('finish', res).on('error', rej);
  });
  fs.unlinkSync(finalPath);
  console.log(`ingest: OK — ${written} rows written; meta=${JSON.stringify(meta)}`);
}

if (require.main === module) main().catch((e) => { console.error(e); process.exit(1); });
module.exports = { LEI_WANTED, RR_WANTED, REPEX_WANTED };
