# GLEIF LEI Register & Ownership — Global Legal Entity Index with Parent Hierarchies

The global Legal Entity Identifier register — the same LEI data US compliance teams must use for CFTC, Federal Reserve and SEC counterparty reporting — with direct and ultimate parent relationships for entity resolution and KYB.

The complete global **Legal Entity Identifier (LEI)** register — **3,368,438 legal entities** across 308 jurisdictions — with GLEIF's Level 2 relationship data resolved onto every record: direct and ultimate parents, fund managers, sub-fund links, branch head offices, reporting exceptions, and children counts. Filter it, slice it, or pull portfolio analytics in one run.

**Snapshot: GLEIF Golden Copy published 2026-11-21 16:00 UTC.** Every row carries its `snapshot_date`. The register is refreshed from GLEIF's thrice-daily Golden Copy publications.

## What you get

One row per entity:

- **Identity** — LEI, legal name, other/translated names, legal jurisdiction, entity category and sub-category, ISO 20275 legal-form code, entity creation date, successor LEIs
- **Registration** — registration authority (the national register) **and the entity's local registration number in that register** — the crosswalk key that joins the LEI world to national company registers — plus initial registration date, last update, registration status (ISSUED / LAPSED / RETIRED / …), next renewal date, managing LOU and conformity flag
- **Addresses** — legal address in full; headquarters address when it differs from the legal address (`headquarters_same_as_legal` says which)
- **Ownership overlay (Level 2)** — `direct_parent_lei`, `ultimate_parent_lei`, `fund_manager_lei`, `subfund_of_lei`, `head_office_lei`; where no parent is reported, the GLEIF **reporting-exception reasons** ship on the row (`NATURAL_PERSONS`, `NON_CONSOLIDATING`, `NO_LEI`, …) so "no parent" is never ambiguous; `direct_children_count` / `ultimate_children_count` size every parent's subtree within the register
- **Analytics mode** — registrations by country, jurisdiction, entity status, registration status, entity category, registration authority and managing LOU; top legal forms; relationship coverage; the 25 largest corporate parents by direct children

## Scope — stated honestly

- **Legal entities only.** Records with GLEIF EntityCategory `GENERAL`, `FUND`, `BRANCH`, `RESIDENT_GOVERNMENT_ENTITY` and `INTERNATIONAL_ORGANIZATION` ship. **SOLE_PROPRIETOR records (134,102 in this snapshot) are excluded by design** — they name natural persons.
- **All statuses included** (ACTIVE and INACTIVE entities; ISSUED, LAPSED, RETIRED, … registrations). Use the filters for live-only views.
- **Parent coverage is what GLEIF's Level 2 data supports:** ~3.8% of entities report a direct consolidating parent and ~3.9% an ultimate parent — most legal entities legitimately have no consolidating parent, and the exception reasons on each row say why. This product does not invent hierarchy data GLEIF does not publish.

## Why not just use GLEIF's free API?

GLEIF's own API is an excellent **per-entity lookup** service — and it stays free. This product is the other shape: the **whole register as one clean, join-ready dataset** with the relationship graph pre-resolved onto every row, plus register-wide analytics — the form you need for screening, enrichment, KYB pipelines and hierarchy analysis without crawling 3.4 million API records yourself. A commercial vendor (DataForge) sells the same transformation off-Apify at $59–$299/month per slice.

## Inputs

| Input | Meaning |
|---|---|
| `mode` | `entities` (rows) or `analytics` (single ANALYTICS record in the key-value store) |
| `country` | Legal-address country, ISO alpha-2 (e.g. `SG`, `US`, `GB`, `DE`) |
| `jurisdiction` | GLEIF legal jurisdiction, exact (e.g. `SG`, `US-DE`) |
| `entityStatus` / `registrationStatus` / `entityCategory` | Exact-match status/category filters |
| `legalFormCode` | ISO 20275 ELF code |
| `managingLou` | Managing Local Operating Unit (its LEI) |
| `registrationAuthorityId` | GLEIF registration-authority ID (e.g. `RA000001`) |
| `nameContains` | Case-insensitive substring of the legal name |
| `hasDirectParent` / `hasUltimateParent` | Tri-state: only-with / only-without / all |
| `registeredFrom` / `registeredTo` | Initial LEI registration date window (YYYY-MM-DD) |
| `limit` | Max rows returned (default 5,000; the prefilled run — Singapore, 5,000 rows — completes in seconds. Full-register pulls take longer; a full-register `entities` run with `limit` removed returns ~3.37M rows) |

## Pricing

**$20 per 1,000 results**, with the first **250 results free** on every run. Analytics mode is a single ANALYTICS record in the key-value store — the 25 largest parents, the coverage statement and the full register breakdown. The 250 free results apply per run.

## Source & licence

Source: GLEIF Golden Copy files — LEI-CDF (Level 1), RR-CDF (Level 2 relationships) and Reporting Exceptions, downloaded from GLEIF at 2026-11-21 16:00 UTC. Data licensed under the **CC0 1.0 Universal (Public Domain Dedication)**. Contains information published by GLEIF; see https://www.gleif.org/en/lei-data/gleif-data-quality-management.

Independent tool by Proofhouse Ventures; not affiliated with or endorsed by GLEIF.
