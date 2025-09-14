# batch_flows.py
import os
import sys
import shutil
import psycopg2
import json
import getpass
import tempfile
import time
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Any
from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from prefect.client.orchestration import get_client

# Import the individual flow modules
from flows.convert_to_parquet_flow import convert_to_parquet_flow
from flows.validate_mapping_flow import validate_mapping_flow
from flows.validate_row_flow import validate_row_flow
from flows.load_to_bronze_flow import load_to_bronze_flow

# -----------------------------
# DB connection (unchanged)
# -----------------------------
load_dotenv()

db_port = os.getenv("DB_PORT")
if db_port is None:
    raise ValueError("DB_PORT not set in .env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(db_port),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


def get_connection():
    missing = [k for k, v in DB_CONFIG.items() if v in (None, "")]
    if missing:
        raise ValueError(f"Missing DB config values: {missing}")
    return psycopg2.connect(**DB_CONFIG)


# -----------------------------
# Utilities (unchanged)
# -----------------------------
BATCH_SUFFIX_PREFIX = "_BATCH"


def increment_batch_id(batch_id):
    prefix = batch_id[:-6] if batch_id and len(batch_id) > 6 else "BATCH"
    try:
        number = int(batch_id[-6:]) if batch_id and len(batch_id) >= 6 else 0
    except Exception:
        number = 0
    new_number = number + 1
    return f"{prefix}{new_number:06d}"


def normalize_name(s: str):
    if s is None:
        return ""
    base = os.path.splitext(s)[0]
    return base.strip().lower().replace(" ", "_").replace("-", "_")


def strip_batch_suffix(filename: str) -> str:
    if not filename:
        return filename
    name, ext = os.path.splitext(filename)
    up = name.upper()
    if up.endswith(BATCH_SUFFIX_PREFIX) and len(name) >= len(BATCH_SUFFIX_PREFIX) + 6:
        return name[: -len(BATCH_SUFFIX_PREFIX)] + ext
    if "_BATCH" in up:
        base, suf = name.rsplit("_", 1)
        if suf.upper().startswith("BATCH") and len(suf) >= 11 and suf[5:].isdigit():
            return base + ext
    return filename


def ensure_raw_dirs(client_schema, source_system):
    base = f"raw/{client_schema}/{source_system}"
    for kind in ("incoming", "success", "failed", "archive"):
        os.makedirs(os.path.join(base, kind), exist_ok=True)
    os.makedirs(f"data/{client_schema}/{source_system}/incoming", exist_ok=True)


def write_json_atomic(path, data):
    dirn = os.path.dirname(path)
    os.makedirs(dirn, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_batchinfo_", dir=dirn, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        raise


def read_json_retry(path, retries: int = 10, delay: float = 0.15):
    last_exc = None
    for i in range(retries):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            last_exc = e
            if i < retries - 1:
                time.sleep(delay)
                continue
            raise last_exc


def wait_for_parquet_name(
    batch_info_path, physical_file_name, timeout=30.0, poll_interval=0.25
):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            bi = read_json_retry(batch_info_path, retries=3, delay=0.05)
        except Exception:
            time.sleep(poll_interval)
            continue
        if not isinstance(bi, dict):
            time.sleep(poll_interval)
            continue
        files = bi.get("files", []) or []
        for f in files:
            if f.get("physical_file_name") == physical_file_name:
                if f.get("parquet_name"):
                    return True
                break
        time.sleep(poll_interval)
    return False


def upsert_file_entry_batch_info(
    batch_info_path, new_entry, max_attempts=6, delay=0.08
):
    allowed_keys = {
        "physical_file_name",
        "logical_source_file",
        "source_system",
        "source_type",
        "target_schema",
        "target_table",
        "source_config",
        "parquet_name",
    }

    clean_entry = {k: v for k, v in new_entry.items() if k in allowed_keys}

    for attempt in range(max_attempts):
        try:
            if os.path.exists(batch_info_path):
                current = read_json_retry(batch_info_path)
            else:
                current = {
                    "client_schema": None,
                    "client_id": None,
                    "batch_id": None,
                    "files": [],
                }
        except Exception:
            current = {
                "client_schema": None,
                "client_id": None,
                "batch_id": None,
                "files": [],
            }

        if not isinstance(current, dict):
            current = {
                "client_schema": None,
                "client_id": None,
                "batch_id": None,
                "files": [],
            }

        files = current.setdefault("files", [])
        found = False
        for f in files:
            if str(f.get("physical_file_name")) == str(
                clean_entry.get("physical_file_name")
            ):
                existing_parquet = f.get("parquet_name", None)
                for k, v in clean_entry.items():
                    if k == "parquet_name" and (v is None) and existing_parquet:
                        continue
                    f[k] = v
                found = True
                break

        if not found:
            files.append(clean_entry)

        try:
            write_json_atomic(batch_info_path, current)
            return True
        except Exception:
            time.sleep(delay * (1 + attempt * 0.3))
            continue

    return False


# Utility function untuk update flow run name
async def update_flow_run_name(new_name: str):
    logger = get_run_logger()
    run_id = getattr(flow_run, "id", None)  # runtime.flow_run.id

    if not run_id:
        logger.warning(
            "flow_run.id kosong — flow ini tampaknya dijalankan di luar Prefect runtime (mis. 'python script.py'). "
            "Skip update nama atau jalankan lewat deployment/agent."
        )
        return False

    try:
        async with get_client() as client:
            # pakai 'name' bukan FlowRunUpdate flow_run=...
            await client.update_flow_run(flow_run_id=run_id, name=new_name)
        return True
    except Exception as e:
        logger.error(f"Failed to update flow run name: {e}")
        return False


# -----------------------------
# DB helpers (unchanged)
# -----------------------------
def get_client_info(cur, client_schema):
    cur.execute(
        "SELECT client_id, last_batch_id FROM tools.client_reference WHERE client_schema = %s",
        (client_schema,),
    )
    row = cur.fetchone()
    if not row:
        raise Exception(
            f"client_schema '{client_schema}' tidak ditemukan di client_reference"
        )
    return {"client_id": row[0], "last_batch_id": row[1]}


def find_client_configs(cur, client_id):
    cur.execute(
        """
        SELECT config_id, source_type, target_schema, target_table, source_config, logical_source_file, source_system
        FROM tools.client_config
        WHERE client_id = %s AND is_active = true
        """,
        (client_id,),
    )
    rows = cur.fetchall()
    configs = []
    for r in rows:
        configs.append(
            {
                "config_id": r[0],
                "source_type": (r[1] or "").lower(),
                "target_schema": r[2],
                "target_table": r[3],
                "source_config": r[4],
                "logical_source_file": r[5],
                "source_system": r[6],
                "logical_norm": normalize_name(r[5]) if r[5] else None,
            }
        )
    return configs


def insert_file_audit(cur, conn, rec):
    cur.execute(
        """
        INSERT INTO tools.file_audit_log
        (convert_status, mapping_validation_status, row_validation_status, load_status, total_rows, valid_rows, invalid_rows,
         processed_by, logical_source_file, physical_file_name, batch_id, file_received_time, source_type, source_system, config_validation_status, client_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            rec.get("convert_status"),
            rec.get("mapping_validation_status"),
            rec.get("row_validation_status"),
            rec.get("load_status"),
            rec.get("total_rows"),
            rec.get("valid_rows"),
            rec.get("invalid_rows"),
            rec.get("processed_by"),
            rec.get("logical_source_file"),
            rec.get("physical_file_name"),
            rec.get("batch_id"),
            rec.get("file_received_time"),
            rec.get("source_type"),
            rec.get("source_system"),
            rec.get("config_validation_status"),
            rec.get("client_id"),
        ),
    )
    conn.commit()


def log_batch_status(
    client_id, status, batch_id, job_name, error_message=None, start_time=None
):
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now()

    cur.execute(
        """
        INSERT INTO tools.job_execution_log 
            (job_name, client_id, status, start_time, end_time, error_message, file_name, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            job_name,
            client_id,
            status,
            start_time or now,
            now,
            error_message,
            None,
            batch_id,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


def get_batch_id_from_incoming_batch_info(client_schema: str):
    """
    Get batch_id from existing batch_info file in batch_info/{client_schema}/incoming
    Ensures only one batch_info file exists to avoid logic errors

    Returns:
        tuple: (success: bool, batch_id: str, batch_info_path: str, error_message: str)
    """
    batch_info_dir = f"batch_info/{client_schema}/incoming"

    if not os.path.exists(batch_info_dir):
        return False, None, None, f"Directory {batch_info_dir} does not exist"

    # Find all batch_info files
    batch_info_files = []
    for filename in os.listdir(batch_info_dir):
        if filename.startswith("batch_output_") and filename.endswith(".json"):
            batch_info_files.append(filename)

    if len(batch_info_files) == 0:
        return False, None, None, f"No batch_info file found in {batch_info_dir}"

    if len(batch_info_files) > 1:
        return (
            False,
            None,
            None,
            f"Multiple batch_info files found in {batch_info_dir}: {batch_info_files}. Please ensure only one batch_info file exists.",
        )

    # Read the single batch_info file
    batch_info_filename = batch_info_files[0]
    batch_info_path = os.path.join(batch_info_dir, batch_info_filename)

    try:
        batch_info = read_json_retry(batch_info_path)
        if not isinstance(batch_info, dict):
            return (
                False,
                None,
                batch_info_path,
                f"Invalid batch_info format in {batch_info_filename}",
            )

        batch_id = batch_info.get("batch_id")
        if not batch_id:
            return (
                False,
                None,
                batch_info_path,
                f"batch_id not found in {batch_info_filename}",
            )

        return True, batch_id, batch_info_path, None

    except Exception as e:
        return (
            False,
            None,
            batch_info_path,
            f"Failed to read {batch_info_filename}: {str(e)}",
        )


# -----------------------------
# Individual Flow Orchestration Functions
# -----------------------------
async def process_file_pipeline(
    client_schema: str, file_info: Dict[str, Any], batch_id: str, mode: str
):
    """Process a single file through the complete pipeline"""
    logger = get_run_logger()
    orig_name = file_info["orig_name"]

    try:
        if mode == "reprocessing":
            # Reprocessing flow: validate_mapping -> validate_row -> load_to_bronze
            logger.info(f"Starting reprocessing pipeline for {orig_name}")

            await validate_mapping_flow(client_schema, orig_name)
            await validate_row_flow(client_schema, orig_name)
            await load_to_bronze_flow(client_schema, orig_name)

            # Move from success to archive after successful processing
            ss = file_info.get("ss", "unknown")
            if ss and ss != "unknown":
                # First, move from failed to archive if exists (for previous failed attempts)
                failed_path = f"raw/{client_schema}/{ss}/failed/{orig_name}"
                archive_path = f"raw/{client_schema}/{ss}/archive/{orig_name}"
                if os.path.exists(failed_path):
                    os.makedirs(os.path.dirname(archive_path), exist_ok=True)
                    shutil.move(failed_path, archive_path)

                # Then, move from success to archive (the main processing)
                success_path = f"raw/{client_schema}/{ss}/success/{orig_name}"
                if os.path.exists(success_path):
                    os.makedirs(os.path.dirname(archive_path), exist_ok=True)
                    shutil.move(success_path, archive_path)
                    logger.info(f"Moved {orig_name} from success to archive")
                else:
                    # If not found in expected source_system, search in all source_systems
                    moved = False
                    for search_ss in ["crm", "erp", "api", "db"]:
                        search_success_path = (
                            f"raw/{client_schema}/{search_ss}/success/{orig_name}"
                        )
                        if os.path.exists(search_success_path):
                            search_archive_path = (
                                f"raw/{client_schema}/{search_ss}/archive/{orig_name}"
                            )
                            os.makedirs(
                                os.path.dirname(search_archive_path), exist_ok=True
                            )
                            shutil.move(search_success_path, search_archive_path)
                            logger.info(
                                f"Moved {orig_name} from {search_ss}/success to {search_ss}/archive"
                            )
                            moved = True
                            break
                    if not moved:
                        logger.warning(
                            f"File {orig_name} not found in any success directory for archiving"
                        )

        elif mode == "restart":
            # Restart flow: move files, upsert batch_info, then full pipeline
            ss = file_info["ss"] or "unknown"
            cfg = file_info["cfg"]

            logger.info(f"Starting restart pipeline for {orig_name}")

            # Move file to success and upsert batch_info
            raw_success = f"raw/{client_schema}/{ss}/success"
            raw_failed = f"raw/{client_schema}/{ss}/failed"
            raw_archive = f"raw/{client_schema}/{ss}/archive"
            os.makedirs(raw_success, exist_ok=True)
            os.makedirs(raw_failed, exist_ok=True)
            os.makedirs(raw_archive, exist_ok=True)

            orig_path = file_info["orig_path"]
            dest_success = os.path.join(raw_success, orig_name)
            try:
                if os.path.abspath(orig_path) != os.path.abspath(dest_success):
                    if os.path.exists(dest_success):
                        os.remove(dest_success)
                    shutil.move(orig_path, dest_success)
            except Exception:
                try:
                    shutil.copy2(orig_path, dest_success)
                except Exception:
                    pass

            batch_info_path = f"batch_info/{client_schema}/incoming/batch_output_{client_schema}_{batch_id}.json"
            new_entry = {
                "physical_file_name": orig_name,
                "logical_source_file": cfg.get("logical_source_file"),
                "source_system": ss,
                "source_type": file_info["ext"],
                "target_schema": cfg.get("target_schema"),
                "target_table": cfg.get("target_table"),
                "source_config": cfg.get("source_config"),
                "parquet_name": None,
            }
            ok = upsert_file_entry_batch_info(batch_info_path, new_entry)
            if not ok:
                logger.warning(f"Failed to upsert batch_info for {orig_name}")

            # Run pipeline based on file type
            if file_info["ext"].lower() == "csv":
                await convert_to_parquet_flow(client_schema, orig_name)

                ok = wait_for_parquet_name(
                    batch_info_path, orig_name, timeout=30.0, poll_interval=0.25
                )
                if not ok:
                    src = os.path.join(raw_success, orig_name)
                    if os.path.exists(src):
                        shutil.move(src, os.path.join(raw_failed, orig_name))
                    raise Exception(
                        "convert_to_parquet did not update batch_info with parquet_name within timeout"
                    )

            await validate_mapping_flow(client_schema, orig_name)
            await validate_row_flow(client_schema, orig_name)
            await load_to_bronze_flow(client_schema, orig_name)

            # Move to archive
            src = os.path.join(raw_success, orig_name)
            if os.path.exists(src):
                shutil.move(src, os.path.join(raw_archive, orig_name))

        else:  # start mode
            # Start flow: rename, audit, then full pipeline
            base, e = os.path.splitext(file_info["orig_name"])
            base_std = base.strip().replace(" ", "_").replace("-", "_")
            new_name = f"{base_std}_{batch_id}{e}"

            ss = file_info["ss"] or "unknown"
            cfg = file_info["cfg"]

            logger.info(f"Starting pipeline for {file_info['orig_name']} -> {new_name}")

            # Setup directories and rename file
            raw_in = f"raw/{client_schema}/{ss}/incoming"
            raw_success = f"raw/{client_schema}/{ss}/success"
            raw_failed = f"raw/{client_schema}/{ss}/failed"
            raw_archive = f"raw/{client_schema}/{ss}/archive"
            os.makedirs(raw_success, exist_ok=True)
            os.makedirs(raw_failed, exist_ok=True)
            os.makedirs(raw_archive, exist_ok=True)

            new_full = os.path.join(raw_in, new_name)
            os.rename(file_info["orig_path"], new_full)

            # Insert audit record
            conn = get_connection()
            cur = conn.cursor()
            audit_rec = {
                "client_id": file_info.get("client_id"),
                "processed_by": os.getenv("PROCESS_USER")
                or getpass.getuser()
                or "batch_processing",
                "logical_source_file": cfg.get("logical_source_file"),
                "physical_file_name": new_name,
                "batch_id": batch_id,
                "file_received_time": datetime.now(),
                "source_type": file_info["ext"],
                "source_system": ss,
                "config_validation_status": (
                    "SUCCESS" if cfg.get("logical_source_file") else "FAILED"
                ),
            }

            try:
                insert_file_audit(cur, conn, audit_rec)
            except Exception:
                try:
                    if os.path.exists(new_full):
                        os.rename(new_full, file_info["orig_path"])
                except Exception:
                    pass
                raise
            finally:
                cur.close()
                conn.close()

            shutil.move(new_full, os.path.join(raw_success, new_name))

            # Upsert batch_info
            batch_info_path = f"batch_info/{client_schema}/incoming/batch_output_{client_schema}_{batch_id}.json"
            new_entry = {
                "physical_file_name": new_name,
                "logical_source_file": cfg.get("logical_source_file"),
                "source_system": ss,
                "source_type": file_info["ext"],
                "target_schema": cfg.get("target_schema"),
                "target_table": cfg.get("target_table"),
                "source_config": cfg.get("source_config"),
                "parquet_name": None,
            }
            ok = upsert_file_entry_batch_info(batch_info_path, new_entry)
            if not ok:
                logger.warning(f"Failed to upsert batch_info for {new_name}")

            # Run pipeline
            if file_info["ext"].lower() == "csv":
                await convert_to_parquet_flow(client_schema, new_name)

                ok = wait_for_parquet_name(
                    batch_info_path, new_name, timeout=30.0, poll_interval=0.25
                )
                if not ok:
                    src = os.path.join(raw_success, new_name)
                    if os.path.exists(src):
                        shutil.move(src, os.path.join(raw_failed, new_name))
                    raise Exception(
                        "convert_to_parquet did not update batch_info with parquet_name within timeout"
                    )

            await validate_mapping_flow(client_schema, new_name)
            await validate_row_flow(client_schema, new_name)
            await load_to_bronze_flow(client_schema, new_name)

            # Move to archive
            src = os.path.join(raw_success, new_name)
            if os.path.exists(src):
                shutil.move(src, os.path.join(raw_archive, new_name))

        return True

    except Exception as e:
        logger.error(f"Failed to process {orig_name}: {e}")
        return False


# -----------------------------
# Main Batch Flows
# -----------------------------


@flow(name="Batch Start Flow")
async def batch_start_flow(client_schema: str):
    """Start mode: increment batch_id and process new files"""
    logger = get_run_logger()
    logger.info(f"Starting batch processing for client: {client_schema}")

    conn = get_connection()
    cur = conn.cursor()
    try:
        info = get_client_info(cur, client_schema)
        client_id = info["client_id"]
        last_batch = info["last_batch_id"] or "BATCH000000"
        configs = find_client_configs(cur, client_id)
        source_systems = ["crm", "erp", "api", "db"]

        for ss in source_systems:
            ensure_raw_dirs(client_schema, ss)

        # Increment batch_id
        new_batch_id = increment_batch_id(last_batch)
        # ===== CUSTOM NAME UPDATE - TAMBAHAN INI =====
        custom_name = f"{client_schema}-{new_batch_id}"
        success = await update_flow_run_name(custom_name)
        if success:
            logger.info(f"Flow run name updated to: {custom_name}")
        # ============================================
        cur.execute(
            "UPDATE tools.client_reference SET last_batch_id = %s WHERE client_schema = %s",
            (new_batch_id, client_schema),
        )
        conn.commit()

        job_name = "Batch Processing Start"
        batch_start = datetime.now()
        files_to_handle = []

        # Scan incoming files
        for ss in source_systems:
            incoming = f"raw/{client_schema}/{ss}/incoming"
            if not os.path.isdir(incoming):
                continue
            for fn in os.listdir(incoming):
                path = os.path.join(incoming, fn)
                if not os.path.isfile(path):
                    continue
                ext = os.path.splitext(fn)[1].lower().lstrip(".")
                logical_norm = normalize_name(strip_batch_suffix(fn))
                matched = None
                for cfg in configs:
                    if (
                        (cfg["source_system"] or "").lower() == ss.lower()
                        and cfg["source_type"] == ext
                        and cfg["logical_norm"] == logical_norm
                    ):
                        matched = cfg
                        break
                if matched:
                    matched["client_id"] = client_id
                    files_to_handle.append(
                        {
                            "orig_path": path,
                            "orig_name": fn,
                            "ss": ss,
                            "ext": ext,
                            "cfg": matched,
                            "client_id": client_id,
                        }
                    )
                else:
                    # Handle non-matching files (same logic as original)
                    base, e = os.path.splitext(fn)
                    base_std = base.strip().replace(" ", "_").replace("-", "_")
                    new_name = f"{base_std}_{new_batch_id}{e}"
                    raw_failed = f"raw/{client_schema}/{ss}/failed"
                    os.makedirs(raw_failed, exist_ok=True)
                    new_full = os.path.join(incoming, new_name)
                    try:
                        if os.path.exists(new_full):
                            os.remove(new_full)
                        os.rename(path, new_full)
                        shutil.move(new_full, os.path.join(raw_failed, new_name))
                        phys_to_record = new_name
                    except Exception:
                        phys_to_record = fn
                    audit_rec = {
                        "client_id": client_id,
                        "processed_by": os.getenv("PROCESS_USER")
                        or getpass.getuser()
                        or "autoloader",
                        "logical_source_file": None,
                        "physical_file_name": phys_to_record,
                        "batch_id": new_batch_id,
                        "file_received_time": datetime.now(),
                        "source_type": ext if ext else None,
                        "source_system": ss,
                        "config_validation_status": "FAILED",
                    }
                    try:
                        insert_file_audit(cur, conn, audit_rec)
                    except Exception as e:
                        logger.error(
                            f"Failed to insert audit for no-match file {fn}: {e}"
                        )
                    logger.info(
                        f"SKIP no config match: {fn} -> recorded as {audit_rec['physical_file_name']}"
                    )

        # Create batch_info skeleton
        batch_info_dir = f"batch_info/{client_schema}/incoming"
        os.makedirs(batch_info_dir, exist_ok=True)
        batch_info_path = os.path.join(
            batch_info_dir, f"batch_output_{client_schema}_{new_batch_id}.json"
        )
        skeleton = {
            "client_schema": client_schema,
            "client_id": client_id,
            "batch_id": new_batch_id,
            "files": [],
        }
        try:
            write_json_atomic(batch_info_path, skeleton)
        except Exception as e:
            log_batch_status(
                client_id=client_id,
                status="FAILED",
                batch_id=new_batch_id,
                job_name=job_name,
                error_message=f"BATCHINFO_WRITE_FAILED: {e}",
                start_time=batch_start,
            )
            logger.error(f"Failed to write batch_info: {e}")
            return

        if not files_to_handle:
            log_batch_status(
                client_id=client_id,
                status="FAILED",
                batch_id=new_batch_id,
                job_name=job_name,
                error_message="FILE CONFIG NOT FOUND",
                start_time=batch_start,
            )
            logger.warning(
                "No files match config. Batch info created but no processing needed."
            )
            return

        # Process files sequentially
        batch_status = "SUCCESS"
        batch_error_message = None
        success_files = []

        for item in files_to_handle:
            try:
                success = await process_file_pipeline(
                    client_schema, item, new_batch_id, "start"
                )
                if success:
                    # Use new_name for start mode
                    base, e = os.path.splitext(item["orig_name"])
                    base_std = base.strip().replace(" ", "_").replace("-", "_")
                    new_name = f"{base_std}_{new_batch_id}{e}"
                    success_files.append(new_name)
                else:
                    batch_status = "FAILED"
                    batch_error_message = f"{item['orig_name']} - processing failed"
            except Exception as e:
                logger.error(f"Failed to process {item['orig_name']}: {e}")
                batch_status = "FAILED"
                batch_error_message = f"{item['orig_name']} - {str(e)}"

        log_batch_status(
            client_id=client_id,
            status=batch_status,
            batch_id=new_batch_id,
            job_name=job_name,
            error_message=batch_error_message,
            start_time=batch_start,
        )
        logger.info(
            f"Done. batch_id={new_batch_id} status={batch_status} files_success={len(success_files)}"
        )

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


@flow(name="Batch Restart Flow")
async def batch_restart_flow(client_schema: str):
    """Restart mode: reprocess existing batch files"""
    logger = get_run_logger()
    logger.info(f"Starting batch restart for client: {client_schema}")

    conn = get_connection()
    cur = conn.cursor()
    try:
        info = get_client_info(cur, client_schema)
        client_id = info["client_id"]

        # Get batch_id from existing batch_info in incoming folder
        success, new_batch_id, batch_info_path, error_msg = (
            get_batch_id_from_incoming_batch_info(client_schema)
        )

        job_name = "Batch Processing Restart"
        batch_start = datetime.now()

        if not success or new_batch_id is None:
            log_batch_status(
                client_id=client_id,
                status="FAILED",
                batch_id="UNKNOWN",
                job_name=job_name,
                error_message=f"BATCH_INFO_ERROR: {error_msg}",
                start_time=batch_start,
            )
            logger.error(f"Failed to get batch_id from batch_info: {error_msg}")
            return

        # ===== CUSTOM NAME UPDATE - TAMBAHAN INI =====
        custom_name = f"{client_schema}-{new_batch_id}-restart"
        success_update = await update_flow_run_name(custom_name)
        if success_update:
            logger.info(f"Flow run name updated to: {custom_name}")
        # ============================================

        logger.info(f"Using batch_id: {new_batch_id} from {batch_info_path}")
        configs = find_client_configs(cur, client_id)

        try:
            batch_info = read_json_retry(batch_info_path)
            if not isinstance(batch_info, dict):
                raise ValueError("batch_info is not an object")
        except Exception:
            log_batch_status(
                client_id=client_id,
                status="FAILED",
                batch_id=new_batch_id,
                job_name=job_name,
                error_message=f"INVALID_BATCH_INFO_{new_batch_id}",
                start_time=batch_start,
            )
            logger.error(f"batch_info is not valid at {batch_info_path}")
            return
        try:
            batch_info = read_json_retry(batch_info_path)
            if not isinstance(batch_info, dict):
                raise ValueError("batch_info is not an object")
        except Exception:
            log_batch_status(
                client_id=client_id,
                status="FAILED",
                batch_id=new_batch_id,
                job_name=job_name,
                error_message=f"INVALID_BATCH_INFO_{new_batch_id}",
                start_time=batch_start,
            )
            logger.error("batch_info is not valid")
            return

        # Helper functions for restart (same as original)
        def match_config_for_physical(physical_file_name, source_system, source_type):
            phys_norm = normalize_name(strip_batch_suffix(physical_file_name))
            for cfg_item in configs:
                if (
                    (cfg_item.get("logical_norm") or "") == phys_norm
                    and (cfg_item.get("source_system") or "").lower()
                    == (source_system or "").lower()
                    and (cfg_item.get("source_type") or "") == (source_type or "")
                ):
                    return cfg_item
            return None

        def fetch_and_update_file_audit(
            client_id, batch_id, physical_file_name, source_system, source_type
        ):
            try:
                cur.execute(
                    """
                    SELECT logical_source_file, source_system, source_type
                    FROM tools.file_audit_log
                    WHERE client_id = %s AND batch_id = %s AND physical_file_name = %s
                    ORDER BY file_received_time DESC NULLS LAST, ctid DESC
                    LIMIT 1
                    """,
                    (client_id, batch_id, physical_file_name),
                )
                row = cur.fetchone()
                if not row:
                    return (False, None, None)
                logical_sf_db, ss_db, st_db = row

                matched_cfg = match_config_for_physical(
                    physical_file_name,
                    source_system or ss_db,
                    source_type or (st_db or ""),
                )
                new_status = "SUCCESS" if matched_cfg else "FAILED"

                if matched_cfg and not logical_sf_db:
                    cur.execute(
                        """
                        UPDATE tools.file_audit_log
                        SET logical_source_file = %s, config_validation_status = %s
                        WHERE client_id = %s AND batch_id = %s AND physical_file_name = %s
                        """,
                        (
                            matched_cfg.get("logical_source_file"),
                            new_status,
                            client_id,
                            batch_id,
                            physical_file_name,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE tools.file_audit_log
                        SET config_validation_status = %s
                        WHERE client_id = %s AND batch_id = %s AND physical_file_name = %s
                        """,
                        (
                            new_status,
                            client_id,
                            batch_id,
                            physical_file_name,
                        ),
                    )
                conn.commit()

                logical_sf_new = logical_sf_db or (
                    matched_cfg.get("logical_source_file") if matched_cfg else None
                )
                return (True, logical_sf_new, matched_cfg)
            except Exception:
                conn.rollback()
                raise

        # Build candidate list from batch_info.files
        candidates = []
        for f in batch_info.get("files") or []:
            if not isinstance(f, dict):
                continue
            phys = f.get("physical_file_name")
            ss = f.get("source_system")
            st = f.get("source_type")
            if phys:
                candidates.append(
                    {
                        "physical_file_name": phys,
                        "source_system": ss,
                        "source_type": st,
                    }
                )

        # Fallback: scan raw incoming for files
        if not candidates:
            source_systems = ["crm", "erp", "api", "db"]
            for ss in source_systems:
                incoming = f"raw/{client_schema}/{ss}/incoming"
                if not os.path.isdir(incoming):
                    continue
                for fn in os.listdir(incoming):
                    if not os.path.isfile(os.path.join(incoming, fn)):
                        continue
                    ext = os.path.splitext(fn)[1].lower().lstrip(".")
                    candidates.append(
                        {
                            "physical_file_name": fn,
                            "source_system": ss,
                            "source_type": ext,
                        }
                    )

        missing_records = []
        processed_candidates = []
        for c in candidates:
            phys = c.get("physical_file_name")
            ss = c.get("source_system") or None
            st = c.get("source_type") or None

            try:
                updated, logical_sf, matched_cfg = fetch_and_update_file_audit(
                    client_id, new_batch_id, phys, ss, st
                )
            except Exception as e:
                logger.error(f"ERROR updating file_audit for {phys}: {e}")
                log_batch_status(
                    client_id=client_id,
                    status="FAILED",
                    batch_id=new_batch_id,
                    job_name=job_name,
                    error_message=f"UPDATE_FILE_AUDIT_ERROR_{phys}: {e}",
                    start_time=batch_start,
                )
                return

            if not updated:
                missing_records.append((phys, ss, st))
            else:
                # Find file on disk
                found_path = None
                if ss:
                    for kind in ("incoming", "failed", "success"):
                        p = os.path.join(f"raw/{client_schema}/{ss}", kind, phys)
                        if os.path.exists(p):
                            found_path = p
                            break
                if not found_path:
                    for root_ss in ("crm", "erp", "api", "db"):
                        for kind in ("incoming", "failed", "success"):
                            p = os.path.join(
                                f"raw/{client_schema}/{root_ss}", kind, phys
                            )
                            if os.path.exists(p):
                                found_path = p
                                ss = root_ss
                                break
                        if found_path:
                            break

                cfg_meta = {
                    "physical_file_name": phys,
                    "source_type": st,
                    "source_system": ss,
                    "logical_source_file": logical_sf,
                    "existing_audit": True,
                }
                if matched_cfg:
                    cfg_meta.update(
                        {
                            "target_schema": matched_cfg.get("target_schema"),
                            "target_table": matched_cfg.get("target_table"),
                            "source_config": matched_cfg.get("source_config"),
                            "logical_source_file": matched_cfg.get(
                                "logical_source_file"
                            ),
                        }
                    )

                if found_path:
                    processed_candidates.append(
                        {
                            "orig_path": found_path,
                            "orig_name": phys,
                            "ss": ss,
                            "ext": st,
                            "cfg": cfg_meta,
                        }
                    )
                else:
                    logger.warning(
                        f"Audit updated but file not found on disk for {phys} (batch {new_batch_id})"
                    )

        if missing_records:
            log_batch_status(
                client_id=client_id,
                status="FAILED",
                batch_id=new_batch_id,
                job_name=job_name,
                error_message=f"FILE_AUDIT_RECORD_NOT_FOUND for {len(missing_records)} files",
                start_time=batch_start,
            )
            logger.error(
                f"Failed restart: files without audit records: {missing_records[:5]}"
            )
            return

        if not processed_candidates:
            log_batch_status(
                client_id=client_id,
                status="FAILED",
                batch_id=new_batch_id,
                job_name=job_name,
                error_message="NO_FILES_TO_PROCESS_ON_RESTART",
                start_time=batch_start,
            )
            logger.warning(f"No files to process on restart for batch {new_batch_id}")
            return

        # Process files sequentially
        batch_status = "SUCCESS"
        batch_error_message = None
        success_files = []

        for item in processed_candidates:
            try:
                success = await process_file_pipeline(
                    client_schema, item, new_batch_id, "restart"
                )
                if success:
                    success_files.append(item["orig_name"])
                else:
                    batch_status = "FAILED"
                    batch_error_message = f"{item['orig_name']} - processing failed"
            except Exception as e:
                logger.error(f"Failed to process {item['orig_name']}: {e}")
                batch_status = "FAILED"
                batch_error_message = f"{item['orig_name']} - {str(e)}"

        log_batch_status(
            client_id=client_id,
            status=batch_status,
            batch_id=new_batch_id,
            job_name=job_name,
            error_message=batch_error_message,
            start_time=batch_start,
        )
        logger.info(
            f"Done. batch_id={new_batch_id} status={batch_status} files_success={len(success_files)}"
        )

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


@flow(name="Batch Reprocess Flow")
async def batch_reprocess_flow(client_schema: str):
    """Reprocess mode: reprocess parquet files from data directory"""
    logger = get_run_logger()
    logger.info(f"Starting batch reprocessing for client: {client_schema}")

    conn = get_connection()
    cur = conn.cursor()
    try:
        info = get_client_info(cur, client_schema)
        client_id = info["client_id"]

        # Get batch_id from existing batch_info in incoming folder
        success, new_batch_id, batch_info_path, error_msg = (
            get_batch_id_from_incoming_batch_info(client_schema)
        )

        job_name = "Batch Reprocessing"
        batch_start = datetime.now()

        if not success or new_batch_id is None:
            log_batch_status(
                client_id=client_id,
                status="FAILED",
                batch_id="UNKNOWN",
                job_name=job_name,
                error_message=f"BATCH_INFO_ERROR: {error_msg}",
                start_time=batch_start,
            )
            logger.error(f"Failed to get batch_id from batch_info: {error_msg}")
            return

        # ===== CUSTOM NAME UPDATE - TAMBAHAN INI =====
        custom_name = f"{client_schema}-{new_batch_id}-reprocess"
        success_update = await update_flow_run_name(custom_name)
        if success_update:
            logger.info(f"Flow run name updated to: {custom_name}")
        # ============================================

        logger.info(f"Using batch_id: {new_batch_id} from {batch_info_path}")

        try:
            batch_info = read_json_retry(batch_info_path)
            if not isinstance(batch_info, dict):
                raise ValueError(
                    f"batch_info is not an object (type={type(batch_info).__name__})"
                )
        except Exception:
            log_batch_status(
                client_id=client_id,
                status="FAILED",
                batch_id=new_batch_id,
                job_name=job_name,
                error_message=f"INVALID_BATCH_INFO_{new_batch_id}",
                start_time=batch_start,
            )
            logger.error(f"Failed to read batch_info {batch_info_path}")
            return

        parquet_map = {}
        for f in batch_info.get("files") or []:
            if not isinstance(f, dict):
                continue
            pn = f.get("parquet_name")
            if pn:
                parquet_map[str(pn)] = f

        data_root = f"data/{client_schema}"
        candidates = []
        if os.path.isdir(data_root):
            for entry in os.listdir(data_root):
                p = os.path.join(data_root, entry)
                inc = os.path.join(p, "incoming")
                if os.path.isdir(inc):
                    candidates.append(inc)
            fallback = os.path.join(data_root, "incoming")
            if os.path.isdir(fallback):
                candidates.append(fallback)

        files_to_handle = []
        for d in candidates:
            for fn in os.listdir(d):
                if not fn.lower().endswith(".parquet"):
                    continue
                parquet_filename = fn
                parquet_path = os.path.join(d, fn)
                matched_entry = None

                if parquet_filename in parquet_map:
                    matched_entry = parquet_map[parquet_filename]
                else:
                    lower_map = {k.lower(): v for k, v in parquet_map.items()}
                    if parquet_filename.lower() in lower_map:
                        matched_entry = lower_map[parquet_filename.lower()]

                if not matched_entry:
                    base = os.path.splitext(parquet_filename)[0]
                    base_norm = base.lower().replace("_batch", "").strip()
                    for k, v in parquet_map.items():
                        kb = os.path.splitext(k)[0].lower()
                        if kb.replace("_batch", "").strip() == base_norm:
                            matched_entry = v
                            break

                if not matched_entry:
                    logger.warning(
                        f"SKIP parquet without manifest entry: {parquet_path}"
                    )
                    continue

                physical = matched_entry.get("physical_file_name")
                ss = matched_entry.get("source_system") or None
                if not physical:
                    logger.warning(
                        f"SKIP parquet {parquet_filename} because manifest missing physical_file_name"
                    )
                    continue

                files_to_handle.append(
                    {
                        "orig_path": parquet_path,
                        "orig_name": physical,
                        "ss": ss,
                        "ext": "parquet",
                        "cfg": matched_entry,
                    }
                )

        if not files_to_handle:
            log_batch_status(
                client_id=client_id,
                status="FAILED",
                batch_id=new_batch_id,
                job_name=job_name,
                error_message="FILES NOT FOUND TO REPROCESS",
                start_time=batch_start,
            )
            logger.warning(
                f"No parquet files found for reprocessing on batch {new_batch_id}"
            )
            return

        # Process files sequentially
        batch_status = "SUCCESS"
        batch_error_message = None
        success_files = []

        for item in files_to_handle:
            try:
                success = await process_file_pipeline(
                    client_schema, item, new_batch_id, "reprocessing"
                )
                if success:
                    success_files.append(item["orig_name"])
                else:
                    batch_status = "FAILED"
                    batch_error_message = f"{item['orig_name']} - processing failed"
            except Exception as e:
                logger.error(f"Failed to process {item['orig_name']}: {e}")
                batch_status = "FAILED"
                batch_error_message = f"{item['orig_name']} - {str(e)}"

        log_batch_status(
            client_id=client_id,
            status=batch_status,
            batch_id=new_batch_id,
            job_name=job_name,
            error_message=batch_error_message,
            start_time=batch_start,
        )
        logger.info(
            f"Done. batch_id={new_batch_id} status={batch_status} files_success={len(success_files)}"
        )

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


@flow(name="Batch Process All Clients")
async def batch_process_all_clients_flow():
    """Process all clients with start mode"""
    logger = get_run_logger()
    logger.info("Starting batch processing for all clients")

    # ===== CUSTOM NAME UPDATE - TAMBAHAN INI =====
    custom_name = f"all-clients-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    success_update = await update_flow_run_name(custom_name)
    if success_update:
        logger.info(f"Flow run name updated to: {custom_name}")
    # ============================================

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT client_schema FROM tools.client_reference")
        clients = cur.fetchall()

        for (client_schema,) in clients:
            logger.info(f"Processing client: {client_schema}")
            try:
                await batch_start_flow(client_schema)
            except Exception as e:
                logger.error(f"Failed to process client {client_schema}: {e}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    # For backward compatibility - can still run from command line
    import asyncio

    args = sys.argv[1:]
    if len(args) == 2:
        client, mode = args[0], args[1]
        if mode == "start":
            asyncio.run(batch_start_flow(client))
        elif mode == "restart":
            asyncio.run(batch_restart_flow(client))
        elif mode == "reprocessing":
            asyncio.run(batch_reprocess_flow(client))
        else:
            print("Use mode: start | restart | reprocessing")
    elif len(args) == 0:
        asyncio.run(batch_process_all_clients_flow())
    else:
        print("Format: python batch_flows.py [client] [start|restart|reprocessing]")
