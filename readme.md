# ⚽ FIFA World Cup Data Platform

> **Zero tokens · Zero secrets · Zero Databricks at ingestion · 100% idempotent**
>
> An end-to-end data platform on Azure — parameterized ADF ingestion, Medallion architecture, DLT, Unity Catalog governance and ML — built on 90+ years of FIFA World Cup data (1930–2026).

A portfolio project by [**datamaieutic**](www.linkedin.com/in/ben-noché) — every component is documented with the *why* behind each architecture decision, including **the real errors encountered** and their fixes. Because a pipeline that works on the first try teaches nobody anything.

---

## 🗺️ Overview

```
GITHUB SOURCES — 2 open repos
├─ jfjelstul/worldcup ················· 1930–2022 history (27 datasets)
└─ mominullptr/FIFA-World-Cup-2026 ···· live 2026 tournament (daily updates)
   │  anonymous HTTP · change detection via ETag
   ▼
ADF — Parameterized & idempotent ingestion         ← UC1 ✅ (this repo)
   │  bronze/ (binary copy, dated partitions)
   ▼
Databricks DLT — Medallion Bronze → Silver → Gold  ← UC2 (in progress)
   │  AUTO CDC · @dlt.expect · SCD2
   ▼
Unity Catalog — RBAC · lineage · masking           ← UC5
   ▼
AI/BI Dashboards · MLflow — 2026 predictions       ← UC6
```

**A deliberate architecture boundary**: ADF orchestrates and journals the entire Bronze ingestion; Databricks is reserved exclusively for transformations from Silver onward. No cluster ever spins up to ingest a CSV.

---

## 🏗️ UC1 — Parameterized ADF Ingestion Pipeline (ETag Edition)

### The principle

27 CSV files ingested from GitHub into ADLS Gen2, **with no hardcoded filenames, no tokens, no Key Vault**:

| Decision | Choice | Why |
|---|---|---|
| Change detection | **Anonymous ETag** from the raw CDN (`GET` + `Range: bytes=0-0`) | No PAT, no 60 req/h GitHub API limit, 1 byte transferred |
| Configuration | `ingestion_config.json` — **purely declarative** | Versioned in Git — states *what* to ingest, never the state |
| State | `logs/{id}.json` — **one file per dataset** | Parallel ForEach concurrency eliminated *by design* |
| Orchestration | Minimal master → **autonomous child** in 3 phases | The child reads its own state, decides, acts, journals — 100% testable in isolation |
| Copy | **Binary** (byte-to-byte) | Bronze = the raw truth; counting rows is Silver's job |
| Log writes | Web Activity `PUT` + **Managed Identity** (blob endpoint) | Zero SAS, zero secrets, a single HTTP call |

### The child pipeline's 3 phases

```
PHASE 1 · READ YOUR STATE    Get Metadata (Exists) → If → Lookup → lastETag
PHASE 2 · CAPTURE            anonymous GET (Range: bytes=0-0) → normalized ETag
PHASE 3 · DECIDE AND ACT     ETag ≠ lastETag ? → binary Copy + INGESTED log
                                              : → SKIPPED log (zero writes)
```

### The rule that makes failure harmless

> **The log's `etag` = the version we are CERTAIN is in ADLS.**
> It is an acknowledgment of receipt, not a statement of intent.

| Status | etag written | Effect on the next run |
|---|---|---|
| `INGESTED` | the new one (`currentETag`) | compares against the new ETag |
| `SKIPPED` | the same one (timestamp refreshed) | SKIPPED again if unchanged |
| `FAILED` | **the old one, preserved** | re-detects the gap → **retries on its own** |

Copy failed → the old ETag stays in place → the next run retries automatically. **Self-healing without a single line of retry code.** The rejected new ETag is journaled separately (`rejected_etag`) for diagnostics — never in the decision path.

### Idempotency cycle validated in production

```
Run 1 : lastETag=""        → INGESTED  (first ingestion)
Run 2 : identical ETags    → SKIPPED   (zero writes, zero DIU)
Run 3 : file modified      → INGESTED  (new dated partition)
```

Replayable indefinitely between two GitHub changes — always SKIPPED, never a duplicate.

---

## 🐛 The 5 real errors encountered (and their lessons)

Nobody documents them. Everybody hits them.

| # | Error | Lesson |
|---|---|---|
| 1 | `MissingRequiredHeader` | **blob** endpoint (not dfs) for a single PUT + `x-ms-blob-type: BlockBlob` |
| 2 | `property 'etag' doesn't exist` | `?.` handles a **missing** property, `coalesce` handles **null** — you need both |
| 3 | `rowsCopied = null` | Binary copy doesn't parse rows — log `dataWritten`, count in Silver |
| 4 | `Bearer is not supported in this version` | `x-ms-version: 2021-08-06` is mandatory with Managed Identity — and propagate the fix to all 4 sibling activities |
| 5 | `output is over limit (around 4MB)` | **The latent bug**: without `Range: bytes=0-0`, every GET downloaded the entire file — invisible while files stayed < 4 MB. *A passing test doesn't prove the mechanism works — it proves it hasn't failed yet.* |

---

## 📡 Monitoring & Alerting

Two complementary levels, validated in production:

- **Metric alert** (`Failed pipeline runs` > 0) — wakes you up within 1-5 min
- **Log Analytics alert** with `Split by dimension = Dataset` — the notification tells you *which* dataset failed, extracted from the `p_id` parameter via `parse_json(Parameters)` in `ADFPipelineRun`
- **Logic App → Teams**: on alert, it reads `bronze/logs/{dataset}.json` (Managed Identity, Reader role) and posts an adaptive card with the error, the `rejected_etag`, and a direct link to the run. Red card on failure, **green card on Resolved — with no human intervention**: self-healing made visible.

```kql
ADFPipelineRun
| where Status == "Failed"
| extend p = parse_json(Parameters)
| project TimeGenerated, Dataset = tostring(p.p_id), RunId, FailureType
```

---

## 📁 Repository structure

```
├── pipeline/                  # ADF pipeline JSON sources
│   ├── pl_master_ingestion    #   master: Lookup → Filter → ForEach → Execute
│   └── pl_child_ingest_datasets  # autonomous child: 3 phases, 4 logs
├── dataset/                   # 2 parameterized datasets (not 54) + DS_Log_JSON
├── linkedService/             # anonymous HTTP (GitHub) · ADLS (Managed Identity)
├── factory/                   # factory definition (identifiers redacted)
└── README.md
```

> 🔒 **Sanitized mirror**: this repo is a read-only showcase, mirrored from Azure DevOps. History is rewritten in-flight (`git filter-repo --replace-text`) — tenant, subscription, principal and storage identifiers are redacted across **every commit**. The source of truth stays private; the transformation happens in the flow. *Same philosophy as Bronze: never modify the source.*

---

## 🧰 Stack

`Azure Data Factory` · `ADLS Gen2` · `Managed Identity` · `Azure Monitor` · `Log Analytics (KQL)` · `Logic Apps` · `Databricks (DLT, Unity Catalog)` · `Delta Lake` · `Terraform` · `Databricks Asset Bundles` · `Azure DevOps` · `MLflow`

---

## 🗺️ Roadmap

| UC | Topic | Status |
|---|---|---|
| **UC1** | **Parameterized ADF ingestion — ETag, idempotency, self-healing** | ✅ **Validated in production** |
| UC2 | Medallion Bronze→Silver→Gold — DLT, AUTO CDC, SCD2, `@dlt.expect` | 🔨 In progress |
| UC3 | Terraform + DABs + Azure DevOps — decoupled IaC & CI/CD | 📋 Specified |
| UC4 | Data Quality Framework — expectations + quarantine | 📋 Specified |
| UC5 | Unity Catalog — RBAC, lineage, masking | 📋 Specified |
| UC6 | ML — World Cup 2026 predictions, MLflow, Model Serving | 📋 Specified |
| UC7 | Spark optimization — skew, salting, broadcast | 📋 Specified |
| UC8 | Observability — freshness, volume, schema, lineage, distribution | 📋 Specified |
| UC9 | End-to-end idempotency — Bronze ETag · Silver MERGE · Gold REPLACE WHERE | 📋 Specified |

---

## 📊 Data

Two open sources, ingested by the same parameterized pipeline:

**[`jfjelstul/worldcup`](https://github.com/jfjelstul/worldcup)** — the history. 27 CSV datasets covering every World Cup from 1930 to 2022: matches, goals, players, teams, referees, penalty kicks, awards.

**[`mominullptr/FIFA-World-Cup-2026-Dataset`](https://github.com/mominullptr/FIFA-World-Cup-2026-Dataset)** — the present. A relational dataset for the 2026 tournament (June 11 – July 19), **updated daily throughout the competition**: the first-ever 48-team format (12 groups A–L), 1,248 players with market values, real match results, minute-by-minute events, xG, per-team match stats, and all 16 venues with coordinates and altitude. Zero synthetic data — every stat is sourced and traceable (CC0 license).

This second repo is what makes the architecture earn its keep: **a daily-updated source is exactly the use case ETag change detection was built for** — during the tournament, the pipeline detects and ingests new results every day, and SKIPs everything else. It also feeds UC6: the model trained on 90 years of history gets confronted with real 2026 results, with advanced features (xG, Elo ratings, stadium altitude) ready to use.

---

## 👤 Author

**Ben N** — Data & Analytics Engineer · [**datamaieutic**](www.linkedin.com/in/ben-noché)

Bilingual FR/EN data engineering educational content built on the Socratic method: start from the question you ask yourself in front of your error screen.

*"A pipeline that heals itself — let's build it together."*# ⚽ FIFA World Cup Data Platform