'use strict';
// Unit tests for B11 lib + snapshot loading. Run: npm test
// The end-to-end ingest over the full Golden Copy is verified separately by
// scripts/verify.py (independent Python recomputation); these tests pin the
// semantics both implementations share.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const zlib = require('zlib');
const {
  parseCsv, streamCsvRecords, exclusionReason, normaliseRecord,
  filterEntities, computeAnalytics, SOURCE_DATASET,
} = require('../src/lib');
const { loadSnapshotParts } = require('../src/snapshot');

const FIX = path.join(__dirname, 'fixtures');
let passed = 0;
function ok(name, fn) {
  return Promise.resolve().then(fn).then(() => { passed += 1; console.log(`ok ${passed} - ${name}`); });
}

const P = '549300PARENT00000001';
const S = '549300SUBSID00000002';
const F = '549300FUND0000000004';
const SF = '549300SUBFUND0000005';
const EX = '549300EXCEPT00000006';
const Q = '549300QUOTED00000007';
const BR = '549300BRANCH00000008';

async function buildRows() {
  // Rebuild the overlay exactly as scripts/ingest.js does (same semantics;
  // the production run is cross-checked by scripts/verify.py).
  const typeToMap = {};
  const maps = { direct: new Map(), ultimate: new Map(), fund: new Map(), sub: new Map(), head: new Map() };
  typeToMap.IS_DIRECTLY_CONSOLIDATED_BY = maps.direct;
  typeToMap.IS_ULTIMATELY_CONSOLIDATED_BY = maps.ultimate;
  typeToMap.IS_FUND_MANAGED_BY = maps.fund;
  typeToMap.IS_SUBFUND_OF = maps.sub;
  typeToMap.IS_INTERNATIONAL_BRANCH_OF = maps.head;
  let conflicts = 0;
  for await (const r of streamCsvRecords(path.join(FIX, 'rr.csv'), null, null)) {
    if (r['Relationship.RelationshipStatus'] !== 'ACTIVE') continue;
    const map = typeToMap[r['Relationship.RelationshipType'].replace(/-/g, '_')];
    const s = r['Relationship.StartNode.NodeID']; const e = r['Relationship.EndNode.NodeID'];
    if (map.has(s) && map.get(s) !== e) conflicts += 1; else map.set(s, e);
  }
  const directEx = new Map(); const ultimateEx = new Map();
  for await (const r of streamCsvRecords(path.join(FIX, 'repex.csv'), null, null)) {
    const reasons = [1, 2].map((i) => r[`Exception.Reason.${i}`]).filter(Boolean);
    if (!reasons.length) continue;
    if (r['Exception.Category'] === 'DIRECT_ACCOUNTING_CONSOLIDATION_PARENT') directEx.set(r['LEI'], reasons);
    else if (r['Exception.Category'] === 'ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT') ultimateEx.set(r['LEI'], reasons);
  }
  const rows = [];
  const excluded = {};
  for await (const r of streamCsvRecords(path.join(FIX, 'lei.csv'), null, null)) {
    const reason = exclusionReason(r);
    if (reason) { excluded[reason] = (excluded[reason] || 0) + 1; continue; }
    const lei = r['LEI'];
    rows.push(normaliseRecord(r, {
      directParent: maps.direct.get(lei) || null,
      ultimateParent: maps.ultimate.get(lei) || null,
      fundManager: maps.fund.get(lei) || null,
      subfundOf: maps.sub.get(lei) || null,
      headOffice: maps.head.get(lei) || null,
      directExceptionReasons: directEx.get(lei) || [],
      ultimateExceptionReasons: ultimateEx.get(lei) || [],
    }, '2026-11-21'));
  }
  return { rows, excluded, conflicts };
}

(async () => {
  await ok('parseCsv handles quoted comma, doubled quote, embedded newline, CRLF', () => {
    const recs = parseCsv('A,B\r\n"x,""y""",plain\r\n"line1\nline2",z\r\n');
    assert.strictEqual(recs.length, 2);
    assert.strictEqual(recs[0].A, 'x,"y"');
    assert.strictEqual(recs[1].A, 'line1\nline2');
  });

  const { rows, excluded, conflicts } = await buildRows();
  const byLei = Object.fromEntries(rows.map((r) => [r.lei, r]));

  await ok('scope: 9 fixture rows -> 7 shipped, sole proprietor + unclassified excluded', () => {
    assert.strictEqual(rows.length, 7);
    assert.deepStrictEqual(excluded, { sole_proprietor: 1, unclassified_or_other: 1 });
  });

  await ok('overlay: subsidiary has direct+ultimate parent P; conflict kept first parent', () => {
    assert.strictEqual(conflicts, 1);
    assert.strictEqual(byLei[S].direct_parent_lei, P);
    assert.strictEqual(byLei[S].ultimate_parent_lei, P);
  });

  await ok('overlay: fund manager, subfund-of and branch head office resolve', () => {
    assert.strictEqual(byLei[F].fund_manager_lei, S);
    assert.strictEqual(byLei[SF].subfund_of_lei, F);
    assert.strictEqual(byLei[BR].head_office_lei, P);
  });

  await ok('overlay: inactive relationship ignored (quoted-name entity has no parent)', () => {
    assert.strictEqual(byLei[Q].direct_parent_lei, null);
  });

  await ok('exceptions attach as reasons; parent with empty reasons gets none', () => {
    assert.deepStrictEqual(byLei[EX].direct_parent_exception_reasons, ['NATURAL_PERSONS']);
    assert.deepStrictEqual(byLei[EX].ultimate_parent_exception_reasons, ['NO_KNOWN_PERSON']);
    assert.deepStrictEqual(byLei[P].direct_parent_exception_reasons, []);
  });

  await ok('normalise: literal NULL entity status -> null; successors collected', () => {
    assert.strictEqual(byLei[EX].entity_status, null);
    assert.deepStrictEqual(byLei[EX].successor_leis, [P]);
  });

  await ok('normalise: HQ same as legal -> null + flag true; different -> object + flag false', () => {
    assert.strictEqual(byLei[P].headquarters_same_as_legal, true);
    assert.strictEqual(byLei[P].headquarters_address, null);
    assert.strictEqual(byLei[S].headquarters_same_as_legal, false);
    assert.strictEqual(byLei[S].headquarters_address.city, 'Wilmington');
  });

  await ok('normalise: other names, quoted legal name, snapshot date + source on every row', () => {
    assert.deepStrictEqual(byLei[S].other_names, [{ name: 'Acme Sub', type: 'ALTERNATIVE_LANGUAGE_LEGAL_NAME' }]);
    assert.strictEqual(byLei[Q].legal_name, 'Weird "Quoted", Name\nHoldings GmbH');
    for (const r of rows) {
      assert.strictEqual(r.snapshot_date, '2026-11-21');
      assert.strictEqual(r.source_dataset, SOURCE_DATASET);
    }
  });

  await ok('filters: country, hasDirectParent, nameContains, registration window (limit/mode are applied in main.js)', () => {
    assert.deepStrictEqual(filterEntities(rows, { country: 'SG' }).map((r) => r.lei).sort(), [BR, S].sort());
    assert.deepStrictEqual(filterEntities(rows, { hasDirectParent: true }).map((r) => r.lei), [S]);
    assert.deepStrictEqual(filterEntities(rows, { hasDirectParent: false }).length, 6);
    assert.deepStrictEqual(filterEntities(rows, { nameContains: 'acme opportunities' }).map((r) => r.lei).sort(), [F, SF].sort());
    assert.deepStrictEqual(filterEntities(rows, { registeredFrom: '2020-01-01', registeredTo: '2020-12-31' }).map((r) => r.lei), [S]);
  });

  await ok('analytics: totals, coverage, top parents', () => {
    // Simulate ingest pass 4 (children counts) before analytics.
    byLei[P].direct_children_count = 1; byLei[P].ultimate_children_count = 1;
    const a = computeAnalytics(rows);
    assert.strictEqual(a.rows_analysed, 7);
    assert.strictEqual(a.relationship_coverage.with_direct_parent_lei, 1);
    assert.strictEqual(a.relationship_coverage.with_ultimate_parent_lei, 1);
    assert.strictEqual(a.relationship_coverage.with_direct_parent_exception, 1);
    assert.strictEqual(a.by_country.find((c) => c.country === 'SG').entities, 2);
    assert.strictEqual(a.by_entity_category.FUND, 2);
    assert.strictEqual(a.top_parents_by_direct_children[0].lei, P);
    assert.strictEqual(a.top_parents_by_direct_children[0].direct_children, 1);
  });

  await ok('snapshot loader: parts read in order, meta extracted', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'b11-snap-'));
    const meta = { __meta__: { rows: 3, snapshot_date: '2026-11-21' } };
    const lines = [JSON.stringify(meta), JSON.stringify({ lei: 'A' }), JSON.stringify({ lei: 'B' }), JSON.stringify({ lei: 'C' })];
    fs.writeFileSync(path.join(dir, 'entities-1.ndjson.gz'), zlib.gzipSync(lines.slice(0, 2).join('\n') + '\n'));
    fs.writeFileSync(path.join(dir, 'entities-2.ndjson.gz'), zlib.gzipSync(lines.slice(2).join('\n') + '\n'));
    const snap = await loadSnapshotParts([path.join(dir, 'entities-1.ndjson.gz'), path.join(dir, 'entities-2.ndjson.gz')]);
    assert.strictEqual(snap.meta.snapshot_date, '2026-11-21');
    assert.deepStrictEqual(snap.rows.map((r) => r.lei), ['A', 'B', 'C']);
  });

  await ok('streamCsvRecords throws on a missing expected column', async () => {
    const bad = path.join(os.tmpdir(), 'b11-bad.csv');
    fs.writeFileSync(bad, 'LEI,Entity.LegalName\nX,Y\n');
    let threw = false;
    try {
      for await (const r of streamCsvRecords(bad, null, ['Entity.EntityCategory'])) { void r; }
    } catch (e) { threw = /expected column missing/.test(e.message); }
    assert.strictEqual(threw, true);
  });

  console.log(`\n${passed} tests passed`);
})().catch((e) => { console.error(e); process.exit(1); });
