# Runbook

How this pipeline is put together, and how to run it. Decisions and trade-offs are in `PROCESS.md`. The assignment brief stays in `README.md`.

## What this is

One Databricks pipeline that turns two public sources into answers for three questions:

1. Mean and standard deviation of US population, 2013 to 2018.
2. Best year per BLS `series_id` (largest quarterly sum, with a readable label).
3. `PRS30006032` / `Q01` values by year, joined to population where it exists.

Ingest runs on your laptop. The pipeline runs in the workspace. Catalog `pr_population`, schemas `bronze`, `silver`, and `gold`.

```text
BLS pr folder ──┐
                ├── laptop ingest (hash skip) ──► Volume bronze.raw
DataUSA JSON ───┘                                      │
                                                       ▼
                                              bronze tables
                                                       │
                    ┌──────────────────────────────────┼──────────────────────────┐
                    ▼                                  ▼                          ▼
         silver AllData / population          silver series labels        Current vs AllData
                    │                                  │
                    └──────────────┬───────────────────┘
                                   ▼
                    gold SQL (primary) and gold PySpark
                                   ▼
                          sql_pyspark_match
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
           gold dashboard                  Genie on gold SQL
```

## Repo

| path | role |
|---|---|
| `src/ingest/ingest.py` | Download sources, upload a dated snapshot only when sha256 changed |
| `src/pipeline/bronze/` | Typed copies of the volume files |
| `src/pipeline/silver/` | Current AllData and population (SCD type 1), labels, subset check |
| `src/pipeline/gold/gold.sql` | Primary answers |
| `src/pipeline/gold/gold.py` | PySpark answers plus the match table |
| `src/dashboard/gold.lvdash.json` | AI/BI dashboard on the three gold SQL tables |
| `src/genie/gold.geniespace.json` | Genie space definition (created with the CLI, not the bundle) |
| `src/grants/analyst.sql` | Read-only Unity Catalog grants for gold |
| `databricks.yml` | Bundle: workspace host and CLI profile |
| `resources/` | Pipeline, job, and dashboard |
| `docs/screenshots/` | Pipeline graph, catalog, gold answers, job run, dashboard, Genie, grants |

## Before the first run

1. Databricks CLI logged in to the workspace in `databricks.yml`.
2. A Python venv with `src/ingest/requirements.txt`.
3. A contact email for BLS. They 403 requests with no contact in `User-Agent`.

```bash
export BLS_CONTACT_EMAIL='<your_email>'
```

If this laptop has another Databricks profile, unset host/token/profile so the CLI cannot land in the wrong workspace:

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE
```

## First run

From the repo root:

```bash
export BLS_CONTACT_EMAIL='<your_email>'
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/python src/ingest/ingest.py

env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE databricks bundle deploy --profile <your_profile> -t dev
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE databricks bundle run pr_population --profile <your_profile> -t dev
```

Ingest creates the catalog, the three schemas, and volume `pr_population.bronze.raw` if they are missing. The pipeline is serverless Advanced. Free Edition allows one pipeline of this type.

Ingest stays on the laptop. Free Edition serverless outbound internet is limited, so the workspace cannot pull BLS or DataUSA.

## After a source change

Run ingest first. Same hashes upload nothing. If it uploaded, deploy when the code changed, then run the pipeline.

Job `pr_population_refresh` is the same pipeline from the workspace UI. It has no schedule. A daily pipeline run with no new files would rebuild bronze for no reason.

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE databricks bundle run pr_population --profile <your_profile> -t dev
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE databricks bundle run pr_population_refresh --profile <your_profile> -t dev
```

If silver CDC errors on snapshot version type after a CDC source change, full-refresh those two tables:

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE databricks bundle run pr_population --profile <your_profile> -t dev --full-refresh pr_population.silver.pr_all_data,pr_population.silver.population
```

## Gold dashboard, Genie, and grants

The dashboard is in the bundle. Deploy after the gold tables exist. Queries use the viewer identity, so the analyst grants below are what let someone else open it.

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE databricks bundle deploy --profile <your_profile> -t dev
```

Apply the grants from a SQL warehouse that can `MANAGE` the catalog:

```bash
# contents of src/grants/analyst.sql
GRANT USE CATALOG ON CATALOG pr_population TO `account users`;
GRANT BROWSE ON CATALOG pr_population TO `account users`;
GRANT USE SCHEMA ON SCHEMA pr_population.gold TO `account users`;
GRANT SELECT ON SCHEMA pr_population.gold TO `account users`;
```

That is read-only gold. There is no `SELECT` on bronze or silver, and no `MODIFY`.

Genie is not in the bundle. Bundle Genie needs the direct deployment engine. Create it once from the JSON:

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE databricks genie create-space \
  7f8b0e8d7650a3ff \
  "$(cat src/genie/gold.geniespace.json)" \
  --title "PR Population Gold" \
  --description "Ask the three gold questions in English. SQL tables only." \
  --parent-path /Shared \
  --profile <your_profile>
```

Warehouse id `7f8b0e8d7650a3ff` is the Serverless Starter Warehouse in this workspace. Change it if yours is different.

This workspace already has:

- Dashboard `01f1e80700911fc8b330792597dd9fb6` at `/dashboardsv3/01f1e80700911fc8b330792597dd9fb6/published`
- Genie space `01f1e8070777137ab2d63ad8317db835` at `/genie/rooms/01f1e8070777137ab2d63ad8317db835`

A tested Genie question, "What is the mean US population from 2013 to 2018?", returned 322,069,808 over 6 years from `pr_population.gold.q1_population_2013_2018`.

## Tables

| layer | objects |
|---|---|
| volume | `/Volumes/pr_population/bronze/raw` (`pr/` and `population/`) |
| bronze | `pr_all_data`, `pr_current`, `pr_series`, code tables, `population` |
| silver | `pr_all_data`, `population`, `pr_series_label`, `current_outside_all_data` |
| gold SQL | `q1_population_2013_2018`, `q2_series_best_year`, `q3_prs30006032_q01` |
| gold PySpark | the same names with `_pyspark` |
| match | `gold.sql_pyspark_match` |

A good run:

- `current_outside_all_data.missing_keys` is 0
- Q1 is 6 years
- Q2 is one labeled row per series
- Q3 keeps years with no population
- match is true, true, true

## If it fails

- BLS HTTP 403: `BLS_CONTACT_EMAIL` is missing.
- Writes went to the wrong workspace: host, token, or profile was still set in the environment.
- Pipeline catalog in YAML changed after the pipeline already existed: delete the old pipeline and deploy again. YAML does not move tables across catalogs.
