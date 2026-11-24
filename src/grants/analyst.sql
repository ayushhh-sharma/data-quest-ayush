-- Read-only analyst on gold. No bronze, no silver, no write.
-- USE CATALOG is required to reach gold. BROWSE is metadata only.
-- SELECT on the schema covers current and future gold tables.

GRANT USE CATALOG ON CATALOG pr_population TO `account users`;
GRANT BROWSE ON CATALOG pr_population TO `account users`;
GRANT USE SCHEMA ON SCHEMA pr_population.gold TO `account users`;
GRANT SELECT ON SCHEMA pr_population.gold TO `account users`;
