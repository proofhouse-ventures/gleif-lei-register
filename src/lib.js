'use strict';
// B11 core logic — pure functions, no Apify dependency. Shared by main.js,
// ingest and tests.
// Scope: GLEIF Golden Copy (LEI-CDF Level 1 + RR-CDF Level 2 + Reporting
// Exceptions), published by the Global Legal Entity Identifier Foundation
// under CC0 1.0. PERSON-LEVEL SCOPE RULE (Gate-0-lite, 2026-11-22, from the
// international-expansion research conditions): records with
// EntityCategory = SOLE_PROPRIETOR are excluded at ingest — those records
// name natural persons (the business owner) and can carry residential
// addresses. Every other category (GENERAL, FUND, BRANCH,
// RESIDENT_GOVERNMENT_ENTITY, INTERNATIONAL_ORGANIZATION) names a legal
// person or public body and ships. Any category value outside the known
// shipped set is excluded and counted, never silently shipped.
const fs = require('fs');
const { StringDecoder } = require('string_decoder');

const SHIPPED_CATEGORIES = [
  'GENERAL',
  'FUND',
  'BRANCH',
  'RESIDENT_GOVERNMENT_ENTITY',
  'INTERNATIONAL_ORGANIZATION',
];
const SHIPPED_SET = new Set(SHIPPED_CATEGORIES);
const SOURCE_DATASET = 'GLEIF Golden Copy (LEI-CDF, RR-CDF, Reporting Exceptions)';
const SNAPSHOT_DATE_PLACEHOLDER = '__SNAPSHOT_DATE__';

// The source CSVs use the literal string "NULL" for some absent values
// (notably EntityStatus and RelationshipStatus).
function naToNull(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === '' || s === 'NULL' ? null : s;
}

// Why a raw LEI record does not ship; null when it ships.
function exclusionReason(r) {
  const c = r['Entity.EntityCategory'];
  if (SHIPPED_SET.has(c)) return null;
  if (c === 'SOLE_PROPRIETOR') return 'sole_proprietor';
  return 'unclassified_or_other';
}

function isShippedCategory(c) { return SHIPPED_SET.has(c); }

// ---------------------------------------------------------------------------
// CSV parsing (RFC4180). The Golden Copy CSVs are far too large to load as
// text (LEI file: 5.1 GB), and fields can contain embedded commas, quotes
// and newlines — so parsing is a streaming state machine over file chunks,
// yielding one record at a time. Only whitelisted columns are materialised
// into the record object (the LEI file has 338 columns; we ship ~45).
// ---------------------------------------------------------------------------
function parseCsvRows(text) {
  // String-based core used by parseCsv and the tests. Yields arrays of
  // values aligned to the header, header first. No column filtering (the
  // streaming reader does its own whitelisting).
  let field = '';
  let row = [];
  let inQ = false;
  const rows = [];
  function endField() { row.push(field); field = ''; }
  function endRow() {
    endField();
    if (row.length > 1 || row[0] !== '') rows.push(row);
    row = [];
  }
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; } else inQ = false;
      } else field += c;
    } else if (c === '"') inQ = true;
    else if (c === ',') endField();
    else if (c === '\n') endRow();
    else if (c !== '\r') field += c;
  }
  if (field !== '' || row.length) endRow();
  return rows;
}

// Parse CSV text (small inputs: fixtures, tests). Returns record objects
// keyed by header.
function parseCsv(text) {
  const rows = parseCsvRows(text.replace(/^\uFEFF/, ''));
  const header = rows[0].map((h) => h.trim());
  return rows.slice(1).map((vals) => {
    const rec = {};
    header.forEach((h, i) => { rec[h] = vals[i] !== undefined ? vals[i].trim() : ''; });
    return rec;
  });
}

// Streaming CSV reader for the full Golden Copy files. Async-iterates
// record objects keyed by header, keeping only `wanted` columns. Throws on
// a missing expected column (source drift must fail loudly, never ship a
// silently wrong snapshot — the B9 rule). The state machine carries a
// pending-quote flag across chunk boundaries so an escaped quote ("")
// split between two chunks is still read as one escaped quote.
async function* streamCsvRecords(filePath, wanted, expectedColumns) {
  const decoder = new StringDecoder('utf8');
  let field = '';
  let row = [];
  let inQ = false;
  let pendingQuote = false; // a '"' seen inside quotes at a chunk edge
  let header = null;
  let keep = null; // [{name, idx}] in header order
  const queue = [];
  function endRow() {
    row.push(field); field = '';
    if (!header) {
      header = row.map((h) => h.trim().replace(/^\uFEFF/, ''));
      if (expectedColumns) {
        for (const col of expectedColumns) {
          if (!header.includes(col)) throw new Error(`streamCsvRecords: expected column missing from ${filePath}: ${col}`);
        }
      }
      keep = [];
      header.forEach((h, i) => { if (!wanted || wanted.has(h)) keep.push({ name: h, idx: i }); });
    } else if (row.length > 1 || row[0] !== '') {
      const rec = {};
      for (const k of keep) rec[k.name] = row[k.idx] !== undefined ? row[k.idx].trim() : '';
      queue.push(rec);
    }
    row = [];
  }
  function feed(text) {
    for (let i = 0; i < text.length; i++) {
      const c = text[i];
      if (pendingQuote) {
        pendingQuote = false;
        if (c === '"') { field += '"'; continue; }
        inQ = false; // the pending quote was the closing quote; reprocess c outside quotes
      }
      if (inQ) {
        if (c === '"') {
          if (i + 1 < text.length) { if (text[i + 1] === '"') { field += '"'; i++; } else inQ = false; }
          else pendingQuote = true;
        } else field += c;
      } else if (c === '"') inQ = true;
      else if (c === ',') { row.push(field); field = ''; }
      else if (c === '\n') endRow();
      else if (c !== '\r') field += c;
    }
  }
  const stream = fs.createReadStream(filePath, { highWaterMark: 1 << 20 });
  for await (const chunk of stream) {
    feed(decoder.write(chunk));
    while (queue.length) yield queue.shift();
  }
  feed(decoder.end());
  if (pendingQuote) { pendingQuote = false; inQ = false; }
  if (field !== '' || row.length) endRow();
  while (queue.length) yield queue.shift();
}

// ---------------------------------------------------------------------------
// Address composition. CDF address blocks are flattened into line parts;
// we ship the non-empty lines in source order plus the structured tail
// (city / region / country / postal code).
// ---------------------------------------------------------------------------
function composeAddress(r, prefix) {
  const parts = [
    r[prefix + '.FirstAddressLine'],
    r[prefix + '.AddressNumber'],
    r[prefix + '.AddressNumberWithinBuilding'],
    r[prefix + '.MailRouting'],
    r[prefix + '.AdditionalAddressLine.1'],
    r[prefix + '.AdditionalAddressLine.2'],
    r[prefix + '.AdditionalAddressLine.3'],
  ].map(naToNull).filter(Boolean);
  const addr = {
    lines: parts,
    city: naToNull(r[prefix + '.City']),
    region: naToNull(r[prefix + '.Region']),
    country: naToNull(r[prefix + '.Country']),
    postal_code: naToNull(r[prefix + '.PostalCode']),
  };
  if (!addr.lines.length && !addr.city && !addr.country) return null;
  return addr;
}

function sameAddress(a, b) {
  return JSON.stringify(a) === JSON.stringify(b);
}

// Normalise one raw LEI-CDF record (object keyed by the flattened CDF
// column names) into the clean store shape. `overlay` carries the Level 2
// data resolved at ingest: { directParent, ultimateParent, fundManager,
// subfundOf, headOffice, directExceptionReasons, ultimateExceptionReasons }
// (LEI strings or null; reason arrays possibly empty). Children counts are
// NOT set here — they depend on the full shipped set and are injected by
// the ingest's second pass (see scripts/ingest.js); they default to 0.
// Returns null for excluded records (see the scope rule above).
function normaliseRecord(r, overlay, snapshotDate) {
  if (!SHIPPED_SET.has(r['Entity.EntityCategory'])) return null;
  const o = overlay || {};
  const otherNames = [];
  for (let i = 1; i <= 5; i++) {
    const name = naToNull(r[`Entity.OtherEntityNames.OtherEntityName.${i}`]);
    if (name) otherNames.push({ name, type: naToNull(r[`Entity.OtherEntityNames.OtherEntityName.${i}.type`]) });
  }
  const successors = [];
  for (let i = 1; i <= 5; i++) {
    const lei = naToNull(r[`Entity.SuccessorEntity.${i}.SuccessorLEI`]);
    if (lei) successors.push(lei);
  }
  const legal = composeAddress(r, 'Entity.LegalAddress');
  const hq = composeAddress(r, 'Entity.HeadquartersAddress');
  const hqSame = !hq || (legal !== null && sameAddress(legal, hq));
  return {
    lei: r['LEI'],
    legal_name: naToNull(r['Entity.LegalName']),
    other_names: otherNames,
    legal_jurisdiction: naToNull(r['Entity.LegalJurisdiction']),
    entity_category: r['Entity.EntityCategory'],
    entity_sub_category: naToNull(r['Entity.EntitySubCategory']),
    legal_form_code: naToNull(r['Entity.LegalForm.EntityLegalFormCode']),
    legal_form_other: naToNull(r['Entity.LegalForm.OtherLegalForm']),
    entity_status: naToNull(r['Entity.EntityStatus']),
    entity_creation_date: naToNull(r['Entity.EntityCreationDate']),
    successor_leis: successors,
    registration_authority_id: naToNull(r['Entity.RegistrationAuthority.RegistrationAuthorityID']),
    registration_authority_entity_id: naToNull(r['Entity.RegistrationAuthority.RegistrationAuthorityEntityID']),
    legal_address: legal,
    headquarters_address: hqSame ? null : hq,
    headquarters_same_as_legal: hqSame,
    initial_registration_date: naToNull(r['Registration.InitialRegistrationDate']),
    last_update_date: naToNull(r['Registration.LastUpdateDate']),
    registration_status: naToNull(r['Registration.RegistrationStatus']),
    next_renewal_date: naToNull(r['Registration.NextRenewalDate']),
    managing_lou: naToNull(r['Registration.ManagingLOU']),
    conformity_flag: naToNull(r['ConformityFlag']),
    direct_parent_lei: o.directParent || null,
    ultimate_parent_lei: o.ultimateParent || null,
    fund_manager_lei: o.fundManager || null,
    subfund_of_lei: o.subfundOf || null,
    head_office_lei: o.headOffice || null,
    direct_parent_exception_reasons: o.directExceptionReasons || [],
    ultimate_parent_exception_reasons: o.ultimateExceptionReasons || [],
    direct_children_count: 0,
    ultimate_children_count: 0,
    snapshot_date: snapshotDate || SNAPSHOT_DATE_PLACEHOLDER,
    source_dataset: SOURCE_DATASET,
  };
}

function filterEntities(rows, input, meta) {
  const country = input.country ? String(input.country).toUpperCase() : null;
  const jurisdiction = input.jurisdiction ? String(input.jurisdiction).toUpperCase() : null;
  const entityStatus = input.entityStatus ? String(input.entityStatus).toUpperCase() : null;
  const registrationStatus = input.registrationStatus ? String(input.registrationStatus).toUpperCase() : null;
  const entityCategory = input.entityCategory ? String(input.entityCategory).toUpperCase() : null;
  const legalForm = input.legalFormCode ? String(input.legalFormCode).toUpperCase() : null;
  const lou = input.managingLou ? String(input.managingLou) : null;
  const regAuth = input.registrationAuthorityId ? String(input.registrationAuthorityId).toUpperCase() : null;
  const nameContains = input.nameContains ? String(input.nameContains).toLowerCase() : null;
  return rows.filter((r) => {
    if (country && (!r.legal_address || r.legal_address.country !== country)) return false;
    if (jurisdiction && (r.legal_jurisdiction || '').toUpperCase() !== jurisdiction) return false;
    if (entityStatus && (r.entity_status || '').toUpperCase() !== entityStatus) return false;
    if (registrationStatus && (r.registration_status || '').toUpperCase() !== registrationStatus) return false;
    if (entityCategory && r.entity_category.toUpperCase() !== entityCategory) return false;
    if (legalForm && (r.legal_form_code || '').toUpperCase() !== legalForm) return false;
    if (lou && r.managing_lou !== lou) return false;
    if (regAuth && (r.registration_authority_id || '').toUpperCase() !== regAuth) return false;
    if (nameContains && !(r.legal_name || '').toLowerCase().includes(nameContains)) return false;
    if (input.hasDirectParent === true && !r.direct_parent_lei) return false;
    if (input.hasDirectParent === false && r.direct_parent_lei) return false;
    if (input.hasUltimateParent === true && !r.ultimate_parent_lei) return false;
    if (input.hasUltimateParent === false && r.ultimate_parent_lei) return false;
    if (input.registeredFrom && (!r.initial_registration_date || r.initial_registration_date < input.registeredFrom)) return false;
    if (input.registeredTo && (!r.initial_registration_date || r.initial_registration_date > input.registeredTo)) return false;
    return true;
  });
}

function bump(map, key) { map.set(key, (map.get(key) || 0) + 1); }

function topCounts(map, limit, keyName) {
  return [...map.entries()]
    .map(([k, v]) => ({ [keyName]: k, entities: v }))
    .sort((a, b) => b.entities - a.entities || String(a[keyName]).localeCompare(String(b[keyName])))
    .slice(0, limit);
}

function computeAnalytics(rows, meta) {
  const byCountry = new Map();
  const byJurisdiction = new Map();
  const byEntityStatus = {};
  const byRegistrationStatus = {};
  const byCategory = {};
  const byRegAuth = new Map();
  const byLou = new Map();
  const byLegalForm = new Map();
  let withDirect = 0; let withUltimate = 0; let withDirectEx = 0; let withUltimateEx = 0;
  const parents = [];
  for (const r of rows) {
    if (r.legal_address && r.legal_address.country) bump(byCountry, r.legal_address.country);
    if (r.legal_jurisdiction) bump(byJurisdiction, r.legal_jurisdiction);
    const es = r.entity_status || 'unknown';
    byEntityStatus[es] = (byEntityStatus[es] || 0) + 1;
    const rs = r.registration_status || 'unknown';
    byRegistrationStatus[rs] = (byRegistrationStatus[rs] || 0) + 1;
    byCategory[r.entity_category] = (byCategory[r.entity_category] || 0) + 1;
    if (r.registration_authority_id) bump(byRegAuth, r.registration_authority_id);
    if (r.managing_lou) bump(byLou, r.managing_lou);
    if (r.legal_form_code) bump(byLegalForm, r.legal_form_code);
    if (r.direct_parent_lei) withDirect += 1;
    if (r.ultimate_parent_lei) withUltimate += 1;
    if (r.direct_parent_exception_reasons.length) withDirectEx += 1;
    if (r.ultimate_parent_exception_reasons.length) withUltimateEx += 1;
    if (r.direct_children_count > 0) parents.push({ lei: r.lei, legal_name: r.legal_name, direct_children: r.direct_children_count, ultimate_children: r.ultimate_children_count });
  }
  parents.sort((a, b) => b.direct_children - a.direct_children || a.lei.localeCompare(b.lei));
  const n = rows.length;
  const pct = (x) => (n ? Math.round((x / n) * 10000) / 100 : 0);
  return {
    rows_analysed: n,
    by_country: topCounts(byCountry, 50, 'country'),
    by_jurisdiction: topCounts(byJurisdiction, 50, 'jurisdiction'),
    by_entity_status: byEntityStatus,
    by_registration_status: byRegistrationStatus,
    by_entity_category: byCategory,
    by_registration_authority: topCounts(byRegAuth, 30, 'registration_authority_id'),
    by_managing_lou: topCounts(byLou, 30, 'managing_lou'),
    top_legal_forms: topCounts(byLegalForm, 30, 'legal_form_code'),
    relationship_coverage: {
      with_direct_parent_lei: withDirect,
      with_direct_parent_pct: pct(withDirect),
      with_ultimate_parent_lei: withUltimate,
      with_ultimate_parent_pct: pct(withUltimate),
      with_direct_parent_exception: withDirectEx,
      with_ultimate_parent_exception: withUltimateEx,
      definition: 'Parent LEIs come from ACTIVE RR-CDF consolidation relationships (IS_DIRECTLY_CONSOLIDATED_BY / IS_ULTIMATELY_CONSOLIDATED_BY) in the same Golden Copy. Entities without a parent LEI normally carry a reporting exception (e.g. NON_CONSOLIDATING, NATURAL_PERSONS, NO_LEI) — the exception reasons ship on each row. Children counts count shipped entities only.',
    },
    top_parents_by_direct_children: parents.slice(0, 25),
    coverage: 'Legal entities only: GLEIF Golden Copy records with EntityCategory GENERAL, FUND, BRANCH, RESIDENT_GOVERNMENT_ENTITY or INTERNATIONAL_ORGANIZATION. SOLE_PROPRIETOR records are excluded by design (they name natural persons). All entity and registration statuses are included; use the filters for issued/active-only views.',
  };
}

module.exports = {
  SHIPPED_CATEGORIES,
  isShippedCategory,
  exclusionReason,
  naToNull,
  parseCsv,
  streamCsvRecords,
  composeAddress,
  normaliseRecord,
  filterEntities,
  computeAnalytics,
  SOURCE_DATASET,
};
