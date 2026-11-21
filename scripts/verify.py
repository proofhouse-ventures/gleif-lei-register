#!/usr/bin/env python3
"""Independent verification for B11 (GLEIF LEI Register & Ownership).
Recomputes everything from the raw Golden Copy CSVs with Python's stdlib
csv parser — a separate implementation from the Node ingest (different
language, different CSV parser, different normalisation code) — and compares
against the bundled snapshot (meta + rows). Exits non-zero on any mismatch.

Run: python3 scripts/verify.py [rawDir] [dataDir]
Defaults (run from the build directory): rawDir = ../data/raw/gleif,
dataDir = data (the bundled snapshot parts).
"""
import csv
import glob
import gzip
import json
import random
import sys
from collections import Counter

csv.field_size_limit(sys.maxsize)

RAW = sys.argv[1] if len(sys.argv) > 1 else '../data/raw/gleif'
DATA = sys.argv[2] if len(sys.argv) > 2 else 'data'

SHIPPED = {'GENERAL', 'FUND', 'BRANCH', 'RESIDENT_GOVERNMENT_ENTITY', 'INTERNATIONAL_ORGANIZATION'}
failures = []


def check(name, got, want):
    ok = got == want
    print(f"{'PASS' if ok else 'FAIL'}  {name}: got={got} want={want}")
    if not ok:
        failures.append(name)


def find(kind):
    hits = glob.glob(f'{RAW}/*-gleif-goldencopy-{kind}-golden-copy.csv')
    if not hits:
        raise SystemExit(f'verify: no {kind} CSV in {RAW}')
    return hits[0]


def na(v):
    if v is None:
        return None
    s = v.strip()
    return None if s in ('', 'NULL') else s


lei_path, rr_path, repex_path = find('lei2'), find('rr'), find('repex')
print(f'verify: lei={lei_path}\n         rr={rr_path}\n         repex={repex_path}')

# ---- pass 1: LEI counts + shipped set ------------------------------------
raw_rows = 0
cat = Counter()
excluded = Counter()
shipped = set()
with open(lei_path, newline='', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        raw_rows += 1
        c = row['Entity.EntityCategory']
        cat[c] += 1
        if c in SHIPPED:
            shipped.add(row['LEI'])
        elif c == 'SOLE_PROPRIETOR':
            excluded['sole_proprietor'] += 1
        else:
            excluded['unclassified_or_other'] += 1
print(f'verify: raw={raw_rows} shipped={len(shipped)} categories={dict(cat)}')

# ---- pass 2: RR maps + children counts ------------------------------------
def build_maps():
    direct, ultimate = {}, {}
    pairs_d, pairs_u = [], []
    conflicts = 0
    rows = 0
    with open(rr_path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            rows += 1
            if row['Relationship.RelationshipStatus'] != 'ACTIVE':
                continue
            t = row['Relationship.RelationshipType'].replace('-', '_')
            s, e = row['Relationship.StartNode.NodeID'], row['Relationship.EndNode.NodeID']
            if t == 'IS_DIRECTLY_CONSOLIDATED_BY':
                if s in direct and direct[s] != e:
                    conflicts += 1
                else:
                    direct[s] = e
                pairs_d.append((s, e))
            elif t == 'IS_ULTIMATELY_CONSOLIDATED_BY':
                if s in ultimate and ultimate[s] != e:
                    conflicts += 1
                else:
                    ultimate[s] = e
                pairs_u.append((s, e))
    children_d = Counter(e for s, e in pairs_d if s in shipped)
    children_u = Counter(e for s, e in pairs_u if s in shipped)
    return direct, ultimate, children_d, children_u, rows, conflicts


direct, ultimate, children_d, children_u, rr_rows, rr_conflicts = build_maps()
with_direct = sum(1 for s in direct if s in shipped)
with_ultimate = sum(1 for s in ultimate if s in shipped)
print(f'verify: rr_rows={rr_rows} with_direct={with_direct} with_ultimate={with_ultimate} conflicts={rr_conflicts}')

# ---- pass 3: RepEx coverage over shipped set ------------------------------
direct_ex = {}
ultimate_ex = {}
repex_rows = 0
with open(repex_path, newline='', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        repex_rows += 1
        lei = row['LEI']
        if lei not in shipped:
            continue
        reasons = [row[f'Exception.Reason.{i}'] for i in range(1, 6) if row[f'Exception.Reason.{i}']]
        if not reasons:
            continue
        if row['Exception.Category'] == 'DIRECT_ACCOUNTING_CONSOLIDATION_PARENT':
            direct_ex[lei] = reasons
        elif row['Exception.Category'] == 'ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT':
            ultimate_ex[lei] = reasons
print(f'verify: repex_rows={repex_rows} shipped_with_direct_ex={len(direct_ex)} shipped_with_ultimate_ex={len(ultimate_ex)}')

# ---- sample selection (seeded, deterministic) ------------------------------
rng = random.Random(20261122)
sample_leis = rng.sample(sorted(shipped), 300)
sample_set = set(sample_leis)

# ---- pass 4: full raw records for the sample -------------------------------
sample_raw = {}
with open(lei_path, newline='', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        if row['LEI'] in sample_set:
            sample_raw[row['LEI']] = row
print(f'verify: sample raw records found={len(sample_raw)}')

# ---- snapshot: meta + counts + sample rows ---------------------------------
parts = sorted(glob.glob(f'{DATA}/entities-*.ndjson.gz'),
               key=lambda p: int(p.split('-')[-1].split('.')[0]))
if not parts:
    raise SystemExit(f'verify: no snapshot parts in {DATA}')
meta = None
snap_rows = 0
snap_cats = Counter()
snap_sample = {}
snap_dates = set()
expected_keys = None
key_violations = 0
for p in parts:
    with gzip.open(p, 'rt', encoding='utf-8') as f:
        for line in f:
            if meta is None:
                meta = json.loads(line)['__meta__']
                continue
            snap_rows += 1
            row = json.loads(line)
            snap_cats[row['entity_category']] += 1
            snap_dates.add(row['snapshot_date'])
            if expected_keys is None:
                expected_keys = sorted(row.keys())
            elif sorted(row.keys()) != expected_keys:
                key_violations += 1
            if row['lei'] in sample_set:
                snap_sample[row['lei']] = row
print(f'verify: snapshot rows={snap_rows} parts={len(parts)}')

# ---- comparisons ------------------------------------------------------------
check('meta.raw_rows == recomputed raw rows', meta['raw_rows'], raw_rows)
check('meta.rows == recomputed shipped', meta['rows'], len(shipped))
check('snapshot rows == recomputed shipped', snap_rows, len(shipped))
check('meta.excluded.sole_proprietor', meta['excluded'].get('sole_proprietor', 0), excluded['sole_proprietor'])
check('meta.excluded.unclassified_or_other', meta['excluded'].get('unclassified_or_other', 0), excluded['unclassified_or_other'])
check('no SOLE_PROPRIETOR in snapshot', snap_cats.get('SOLE_PROPRIETOR', 0), 0)
check('snapshot category mix == recomputed shipped mix',
      dict(snap_cats), {c: cat[c] for c in SHIPPED})
check('meta.rr_rows', meta['rr_rows'], rr_rows)
check('meta.repex_rows', meta['repex_rows'], repex_rows)
check('meta.entities_with_direct_parent', meta['entities_with_direct_parent'], with_direct)
check('meta.entities_with_ultimate_parent', meta['entities_with_ultimate_parent'], with_ultimate)
check('snapshot_date uniform', snap_dates, {meta['snapshot_date']})
check('row key set uniform (violations)', key_violations, 0)

field_errors = 0
for lei in sample_leis:
    raw = sample_raw[lei]
    snap = snap_sample.get(lei)
    if snap is None:
        print(f'FAIL  sample {lei}: missing from snapshot')
        field_errors += 1
        continue
    pairs = [
        ('legal_name', snap['legal_name'], na(raw['Entity.LegalName'])),
        ('legal_jurisdiction', snap['legal_jurisdiction'], na(raw['Entity.LegalJurisdiction'])),
        ('entity_category', snap['entity_category'], raw['Entity.EntityCategory']),
        ('entity_status', snap['entity_status'], na(raw['Entity.EntityStatus'])),
        ('registration_status', snap['registration_status'], na(raw['Registration.RegistrationStatus'])),
        ('registration_authority_entity_id', snap['registration_authority_entity_id'],
         na(raw['Entity.RegistrationAuthority.RegistrationAuthorityEntityID'])),
        ('managing_lou', snap['managing_lou'], na(raw['Registration.ManagingLOU'])),
        ('initial_registration_date', snap['initial_registration_date'], na(raw['Registration.InitialRegistrationDate'])),
        ('next_renewal_date', snap['next_renewal_date'], na(raw['Registration.NextRenewalDate'])),
        ('legal_form_code', snap['legal_form_code'], na(raw['Entity.LegalForm.EntityLegalFormCode'])),
        ('direct_parent_lei', snap['direct_parent_lei'], direct.get(lei)),
        ('ultimate_parent_lei', snap['ultimate_parent_lei'], ultimate.get(lei)),
        ('direct_children_count', snap['direct_children_count'], children_d.get(lei, 0)),
        ('ultimate_children_count', snap['ultimate_children_count'], children_u.get(lei, 0)),
        ('direct_parent_exception_reasons', snap['direct_parent_exception_reasons'], direct_ex.get(lei, [])),
        ('ultimate_parent_exception_reasons', snap['ultimate_parent_exception_reasons'], ultimate_ex.get(lei, [])),
        ('legal_address.country', (snap['legal_address'] or {}).get('country'), na(raw['Entity.LegalAddress.Country'])),
        ('legal_address.city', (snap['legal_address'] or {}).get('city'), na(raw['Entity.LegalAddress.City'])),
        ('legal_address.postal_code', (snap['legal_address'] or {}).get('postal_code'), na(raw['Entity.LegalAddress.PostalCode'])),
    ]
    for field, got, want in pairs:
        if got != want:
            print(f'FAIL  sample {lei} field {field}: got={got!r} want={want!r}')
            field_errors += 1
check('sample field errors (300 entities x 19 fields)', field_errors, 0)

print(f"\n{'ALL CHECKS PASSED' if not failures else 'FAILURES: ' + ', '.join(failures)}")
sys.exit(1 if failures else 0)
