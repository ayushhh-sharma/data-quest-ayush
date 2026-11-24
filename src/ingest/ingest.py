"""Land raw BLS files and the population JSON in a Unity Catalog volume.

1. Log in with the data-quest profile and stop if that is not the personal workspace.
2. Create catalog pr_population, then bronze, silver, and gold schemas if they
   are missing. The volume lives in bronze.
3. Read the BLS directory page and take file names from its links.
4. Download those files, plus the population JSON.
5. Upload a file only when its sha256 changed. The snapshot path is dated,
   so an older day's copy is left in place.
6. Record the latest hash in _manifest.json.
"""

import argparse
import hashlib
import io
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound, ResourceAlreadyExists
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout

log = logging.getLogger("ingest")

# Assignment README, Step 1.
BLS_DIRECTORY_URL = "https://download.bls.gov/pub/time.series/pr/"
POPULATION_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)
# Catalog is the data product. Schemas are medallion layers.
CATALOG = "pr_population"
CATALOG_COMMENT = "BLS productivity (PR) and ACS population."
# Medallion layers are schemas. Table names do not repeat the layer.
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
LAYER_SCHEMAS = (
    (BRONZE_SCHEMA, "Landing: raw volume, then typed copies of BLS PR and ACS population."),
    (SILVER_SCHEMA, "Current row per business key after snapshot CDC."),
    (GOLD_SCHEMA, "Answers to the three assignment questions."),
)
VOLUME = "raw"
# Host from the data-quest CLI login. The default profile on this laptop is
# zg-pa-lab, so this check stops a write to the work workspace.
EXPECTED_HOST = "https://dbc-37debd15-633d.cloud.databricks.com"


def parse_bls_filenames(html: str, directory_url: str) -> list[str]:
    """File names linked directly in the BLS folder.

    BLS has no file-list API. The folder is an HTML page. Parent-directory
    links, nested paths, and ?query sort links are not files to download.
    """
    base = directory_url if directory_url.endswith("/") else directory_url + "/"
    base_path = urlparse(base).path
    names: list[str] = []
    for href in re.findall(r"""href=["']([^"']+)["']""", html, flags=re.IGNORECASE):
        parsed = urlparse(urljoin(base, href))
        if parsed.query or not parsed.path.startswith(base_path):
            log.debug("skip href %s", href)
            continue
        name = parsed.path[len(base_path) :]
        if name and "/" not in name and name not in names:
            names.append(name)
    return names


def download(url: str, contact_email: str) -> bytes:
    # BLS returns 403 unless User-Agent includes a contact.
    request = urllib.request.Request(
        url,
        headers={"User-Agent": f"DataQuestIngestion/1.0 (contact: {contact_email})"},
    )
    log.debug("GET %s", url)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        raise SystemExit(f"Download failed for {url}: HTTP {exc.code} {detail}") from exc
    if not body:
        raise SystemExit(f"Download failed for {url}: empty body")
    log.debug("got %s (%s bytes)", url, len(body))
    return body


def connect(profile: str) -> WorkspaceClient:
    client = WorkspaceClient(profile=profile)
    host = (client.config.host or "").rstrip("/")
    user = client.current_user.me().user_name
    if host != EXPECTED_HOST:
        raise SystemExit(f"Refusing to write. Connected as {user} at {host}.")
    log.info("connected as %s at %s", user, host)
    return client


def _warehouse_id(client: WorkspaceClient) -> str:
    warehouses = [w for w in client.warehouses.list() if w.id]
    if not warehouses:
        raise SystemExit("CREATE CATALOG on Default Storage needs a SQL warehouse. None found.")
    running = [w for w in warehouses if "RUNNING" in str(w.state).upper()]
    chosen = running[0] if running else warehouses[0]
    log.info("using SQL warehouse %s", chosen.id)
    return chosen.id


def run_sql(client: WorkspaceClient, warehouse_id: str, statement: str) -> None:
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )
    deadline = time.time() + 300
    while True:
        state = str(getattr(response.status, "state", "")).upper()
        if "SUCCEEDED" in state:
            return
        if any(tag in state for tag in ("FAILED", "CANCELED", "CLOSED")):
            error = getattr(response.status, "error", None)
            raise SystemExit(f"SQL failed ({state}): {error or statement}")
        if time.time() > deadline:
            raise SystemExit(f"SQL timed out: {statement}")
        time.sleep(5)
        if not response.statement_id:
            raise SystemExit(f"SQL returned no statement id: {statement}")
        response = client.statement_execution.get_statement(response.statement_id)


def ensure_catalog(client: WorkspaceClient) -> None:
    try:
        client.catalogs.get(name=CATALOG)
        log.debug("catalog exists: %s", CATALOG)
        return
    except NotFound:
        pass

    warehouse_id = _warehouse_id(client)
    log.info("creating catalog %s via SQL", CATALOG)
    run_sql(
        client,
        warehouse_id,
        f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT '{CATALOG_COMMENT}'",
    )
    log.info("created catalog %s", CATALOG)


def ensure_schema(client: WorkspaceClient, name: str, comment: str) -> None:
    full_name = f"{CATALOG}.{name}"
    try:
        client.schemas.get(full_name=full_name)
        log.debug("schema exists: %s", full_name)
    except NotFound:
        client.schemas.create(name=name, catalog_name=CATALOG, comment=comment)
        log.info("created schema %s", full_name)


def ensure_volume(client: WorkspaceClient) -> str:
    ensure_catalog(client)
    for name, comment in LAYER_SCHEMAS:
        ensure_schema(client, name, comment)
    full_volume = f"{CATALOG}.{BRONZE_SCHEMA}.{VOLUME}"
    try:
        client.volumes.read(name=full_volume)
        log.debug("volume exists: %s", full_volume)
    except NotFound:
        try:
            # Managed volume: Free Edition does not support custom storage locations.
            client.volumes.create(
                catalog_name=CATALOG,
                schema_name=BRONZE_SCHEMA,
                name=VOLUME,
                volume_type=VolumeType.MANAGED,
            )
            log.info("created volume %s", full_volume)
        except ResourceAlreadyExists:
            log.debug("volume already created: %s", full_volume)
    log.info("volume %s", full_volume)
    return f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME}"


def read_manifest(client: WorkspaceClient, path: str) -> dict:
    try:
        response = client.files.download(path)
    except DatabricksError as exc:
        # Files API has returned NotFound on a first run, and a generic 404.
        if isinstance(exc, NotFound) or getattr(exc, "status_code", None) == 404:
            log.info("no manifest yet at %s", path)
            return {"files": {}}
        raise
    raw = response.contents.read()
    if not raw:
        log.info("empty manifest at %s", path)
        return {"files": {}}
    parsed = json.loads(raw)
    log.debug("manifest has %s file keys", len(parsed.get("files") or {}))
    return parsed


def upload(client: WorkspaceClient, path: str, body: bytes) -> None:
    # The SDK may log 403 while picking a Files API endpoint, then fall back.
    # That warning is the SDK, not BLS. The upload still completes.
    client.files.create_directory(path.rsplit("/", 1)[0])
    client.files.upload(path, io.BytesIO(body), overwrite=True)
    log.debug("wrote %s (%s bytes)", path, len(body))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="data-quest")
    parser.add_argument(
        "--log-level",
        default=os.environ.get("INGEST_LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    if args.log_level != "DEBUG":
        logging.getLogger("databricks").setLevel(logging.WARNING)

    # Email stays out of the repo. BLS uses it to identify the caller.
    contact_email = os.environ.get("BLS_CONTACT_EMAIL", "").strip()
    if not contact_email:
        raise SystemExit("Set BLS_CONTACT_EMAIL. No request was sent.")

    client = connect(args.profile)
    root = ensure_volume(client)
    manifest_path = f"{root}/_manifest.json"

    listing_html = download(BLS_DIRECTORY_URL, contact_email).decode("utf-8", errors="replace")
    names = parse_bls_filenames(listing_html, BLS_DIRECTORY_URL)
    if not names:
        raise SystemExit("BLS listing parsed to zero files. Nothing was uploaded.")
    log.info("bls files (%s): %s", len(names), ", ".join(names))

    # Download every source before the first upload so a bad population
    # response leaves the volume untouched.
    files_to_land: list[tuple[str, bytes, str]] = []
    for name in names:
        url = urljoin(BLS_DIRECTORY_URL, name)
        files_to_land.append((f"pr/{name}", download(url, contact_email), url))
    population = download(POPULATION_URL, contact_email)
    try:
        parsed = json.loads(population)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Population response is not JSON: {exc}") from exc
    if not isinstance(parsed, dict) or not isinstance(parsed.get("data"), list):
        raise SystemExit("Population response has no data list. Nothing was uploaded.")
    log.info("population rows: %s", len(parsed["data"]))
    files_to_land.append(("population/population.json", population, POPULATION_URL))

    manifest = read_manifest(client, manifest_path)
    known = manifest.setdefault("files", {})
    run_at = datetime.now(timezone.utc).isoformat()
    snapshot_date = run_at[:10]
    uploads = 0
    for key, body, source_url in files_to_land:
        digest = hashlib.sha256(body).hexdigest()
        current = known.get(key) or {}
        if current.get("sha256") == digest and not current.get("removed_at"):
            log.info("skip %s sha256=%s", key, digest[:12])
            continue
        filename = key.rsplit("/", 1)[-1]
        folder = "population" if key.startswith("population/") else "pr"
        # Date in the path keeps older days. Same-day republish overwrites that day's file.
        snapshot_path = f"{root}/{folder}/snapshot_date={snapshot_date}/{filename}"
        log.info("upload %s (%s bytes) -> %s", key, len(body), snapshot_path)
        upload(client, snapshot_path, body)
        known[key] = {
            "sha256": digest,
            "size_bytes": len(body),
            "snapshot_path": snapshot_path,
            "source_url": source_url,
            "ingested_at": run_at,
            "removed_at": None,
        }
        uploads += 1

    removed = 0
    for key, entry in known.items():
        if not key.startswith("pr/"):
            continue
        filename = key.split("/", 1)[1]
        if filename not in names and not entry.get("removed_at"):
            entry["removed_at"] = run_at
            removed += 1
            log.info("no longer listed: %s", key)

    skipped = len(files_to_land) - uploads
    if uploads or removed:
        manifest["updated_at"] = run_at
        upload(
            client,
            manifest_path,
            json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8") + b"\n",
        )
        log.info("manifest updated")
    else:
        log.info("manifest unchanged")
    log.info("done: uploads=%s skipped=%s removed=%s", uploads, skipped, removed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
