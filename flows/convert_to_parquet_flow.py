import os
import sys
import re
import json
import tempfile
import shutil
import uuid
import time
import traceback
from datetime import datetime
import getpass

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from prefect.client.orchestration import get_client

load_dotenv()


# -----------------------------
# DB config & helper
# -----------------------------
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
# FS / JSON helpers
# -----------------------------
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


def read_json_retry(path, retries: int = 6, delay: float = 0.12):
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


# -----------------------------
# Utilities
# -----------------------------
def extract_batch_id(filename: str):
    m = re.search(r"(BATCH\d{6})", filename, re.IGNORECASE)
    return m.group(1).upper() if m else None


def find_file_entry(batch_info: dict, physical_file_name: str):
    files = batch_info.get("files") or []
    for f in files:
        if f.get("physical_file_name") == physical_file_name:
            return f
    return None


def _normalize_name_for_match(name: str):
    if not name:
        return ""
    base = os.path.splitext(name)[0]
    base = re.sub(r"_?BATCH\d{6}$", "", base, flags=re.IGNORECASE)
    return base.strip().lower()


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
# DB audit helpers
# -----------------------------
def update_file_audit_convert_status(
    conn,
    client_id,
    physical_file_name,
    source_system,
    source_type,
    logical_source_file,
    batch_id,
    status,
):
    cur = conn.cursor()
    sql = """
        UPDATE tools.file_audit_log
        SET convert_status = %s
        WHERE client_id = %s
          AND physical_file_name = %s
          AND source_system = %s
          AND source_type = %s
          AND logical_source_file = %s
          AND batch_id = %s
    """
    cur.execute(
        sql,
        (
            status,
            client_id,
            physical_file_name,
            source_system,
            source_type,
            logical_source_file,
            batch_id,
        ),
    )
    affected = cur.rowcount
    conn.commit()
    cur.close()
    return affected


def insert_job_execution_log(
    conn,
    client_id,
    job_name,
    status,
    error_message,
    file_name,
    batch_id,
    start_time,
    end_time,
):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tools.job_execution_log
         (client_id, job_name, status, error_message, file_name, batch_id, start_time, end_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            client_id,
            job_name,
            status,
            error_message,
            file_name,
            batch_id,
            start_time,
            end_time,
        ),
    )
    conn.commit()
    cur.close()


# -----------------------------
# Conversion
# -----------------------------
def convert_to_parquet(src_path, dest_path, src_type):
    # read input into pandas DataFrame
    if src_type == "csv":
        df = pd.read_csv(src_path, low_memory=False)
    elif src_type in ("xlsx", "xls", "excel"):
        df = pd.read_excel(src_path, sheet_name=0)
    elif src_type == "json":
        try:
            df = pd.read_json(src_path, lines=True)
        except ValueError:
            df = pd.read_json(src_path)
    elif src_type == "parquet":
        df = pd.read_parquet(src_path)
    else:
        raise Exception(f"Unsupported source type for convert: {src_type}")

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    tmp_name = f".tmp_{uuid.uuid4().hex}.parquet"
    tmp_path = os.path.join(os.path.dirname(dest_path), tmp_name)
    try:
        df.to_parquet(tmp_path, engine="pyarrow", compression="snappy", index=False)
        os.replace(tmp_path, dest_path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


# -----------------------------
# Main Prefect Flow
# -----------------------------
@flow(name="Convert to Parquet Flow")
async def convert_to_parquet_flow(client_schema: str, physical_file_name: str):
    """Convert file to parquet format"""
    logger = get_run_logger()
    start_time = datetime.now()
    job_name = "Convert to Parquet"

    try:
        logger.info(f"Starting convert to parquet for {physical_file_name}")
        print(f"Starting convert to parquet for {physical_file_name}")

        batch_id = extract_batch_id(physical_file_name)

        # ===== CUSTOM NAME UPDATE - TAMBAHAN INI =====
        custom_name = f"{client_schema}-{batch_id}-convert"
        success_update = await update_flow_run_name(custom_name)
        if success_update:
            logger.info(f"Flow run name updated to: {custom_name}")
        # ============================================

        if not batch_id:
            error_msg = f"Cannot extract batch_id from file name: {physical_file_name}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            return False

        batch_info_path = os.path.join(
            "batch_info",
            client_schema,
            "incoming",
            f"batch_output_{client_schema}_{batch_id}.json",
        )
        if not os.path.exists(batch_info_path):
            error_msg = f"Batch info not found: {batch_info_path}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            return False

        try:
            batch_info = read_json_retry(batch_info_path)
        except Exception as e:
            error_msg = f"Failed to parse batch_info {batch_info_path}: {e}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            return False

        if not isinstance(batch_info, dict):
            batch_info = {
                "client_schema": client_schema,
                "client_id": None,
                "batch_id": batch_id,
                "files": [],
            }

        client_id = batch_info.get("client_id")
        if client_id is None:
            error_msg = f"client_id not found in batch_info {batch_info_path}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            return False

        file_entry = find_file_entry(batch_info, physical_file_name)
        if not file_entry:
            logger.warning(
                f"File {physical_file_name} not explicitly found inside batch_info {batch_info_path}. Will try tolerant matching."
            )
            print(
                f" File {physical_file_name} not explicitly found inside batch_info {batch_info_path}. Will try tolerant matching."
            )
            # continue, but logical may be None
            logical = None
            source_system = None
            source_type = None
        else:
            logical = file_entry.get("logical_source_file")
            source_system = (file_entry.get("source_system") or "").lower()
            source_type = (file_entry.get("source_type") or "").lower()

        # If missing source_system/source_type from exact entry, try to infer from batch_info entries
        if not source_system or not source_type:
            # try a tolerant lookup
            for f in batch_info.get("files") or []:
                if (
                    str(f.get("physical_file_name", "")).lower()
                    == str(physical_file_name).lower()
                ):
                    source_system = (f.get("source_system") or "").lower()
                    source_type = (f.get("source_type") or "").lower()
                    logical = logical or f.get("logical_source_file")
                    break

        # final validation of inferred values
        if not source_system or not source_type:
            error_msg = f"Unable to determine source_system/source_type for {physical_file_name}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            return False

        src_path = os.path.join(
            "raw", client_schema, source_system, "success", physical_file_name
        )
        if not os.path.exists(src_path):
            error_msg = f"Source file not found at expected location: {src_path}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            try:
                conn = get_connection()
                insert_job_execution_log(
                    conn,
                    client_id,
                    job_name,
                    "FAILED",
                    error_msg,
                    physical_file_name,
                    batch_id,
                    start_time,
                    datetime.now(),
                )
                conn.close()
            except Exception:
                pass
            return False

        base = os.path.splitext(physical_file_name)[0]
        if logical:
            parquet_base = logical
        else:
            parquet_base = re.sub(r"_?BATCH\d{6}$", "", base, flags=re.IGNORECASE)

        parquet_name = f"{parquet_base}_{batch_id}.parquet"
        dest_path = os.path.join(
            "data", client_schema, source_system, "incoming", parquet_name
        )

        conn = None
        try:
            conn = get_connection()
        except Exception as e:
            error_msg = f"DB connection failed: {e}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            return False

        try:
            convert_to_parquet(src_path, dest_path, source_type)
        except Exception as e:
            err = str(e)
            logger.error(f"Conversion FAILED for {physical_file_name}: {err}")
            print(f"[FAIL] Conversion FAILED for {physical_file_name}: {err}")
            traceback.print_exc()
            try:
                affected = update_file_audit_convert_status(
                    conn,
                    client_id,
                    physical_file_name,
                    source_system,
                    source_type,
                    logical,
                    batch_id,
                    "FAILED",
                )
                if affected == 0:
                    print(
                        " Warning: file_audit_log update affected 0 rows (no exact match)."
                    )
            except Exception as e2:
                print(f" Failed to update file_audit_log: {e2}")
            try:
                insert_job_execution_log(
                    conn,
                    client_id,
                    job_name,
                    "FAILED",
                    err,
                    physical_file_name,
                    batch_id,
                    start_time,
                    datetime.now(),
                )
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            return False

        # success update DB audit
        try:
            affected = update_file_audit_convert_status(
                conn,
                client_id,
                physical_file_name,
                source_system,
                source_type,
                logical,
                batch_id,
                "SUCCESS",
            )
            if affected == 0:
                print(
                    " Warning: file_audit_log update affected 0 rows (no exact match)."
                )
        except Exception as e:
            print(f" Failed to update file_audit_log: {e}")

        # robust update batch_info with retries and tolerant matching
        max_attempts = 6
        sleep_backoff = 0.12
        write_ok = False
        for attempt in range(max_attempts):
            try:
                current = read_json_retry(batch_info_path)
            except Exception:
                current = {
                    "client_schema": client_schema,
                    "client_id": client_id,
                    "batch_id": batch_id,
                    "files": [],
                }

            if not isinstance(current, dict):
                current = {
                    "client_schema": client_schema,
                    "client_id": client_id,
                    "batch_id": batch_id,
                    "files": [],
                }

            files = current.setdefault("files", [])
            target_lower = (physical_file_name or "").lower()
            logical_lower = (logical or "").lower()
            base_target = _normalize_name_for_match(physical_file_name)
            updated = False

            # 1) try exact (case-insensitive) match
            for f in files:
                if str(f.get("physical_file_name", "")).lower() == target_lower:
                    f["parquet_name"] = parquet_name
                    updated = True
                    break

            # 2) try matching by logical_source_file (case-insensitive)
            if not updated and logical_lower:
                for f in files:
                    if str(f.get("logical_source_file", "")).lower() == logical_lower:
                        f["parquet_name"] = parquet_name
                        updated = True
                        break

            # 3) try matching by base name without BATCH suffix
            if not updated:
                for f in files:
                    f_base = _normalize_name_for_match(
                        str(f.get("physical_file_name", ""))
                    )
                    if f_base and f_base == base_target:
                        f["parquet_name"] = parquet_name
                        updated = True
                        break

            # 4) if still not found, append a merged / informative entry
            if not updated:
                files.append(
                    {
                        "physical_file_name": physical_file_name,
                        "logical_source_file": logical,
                        "source_system": source_system,
                        "source_type": source_type,
                        "parquet_name": parquet_name,
                    }
                )

            # attempt to write; if concurrent writer wins, retry
            try:
                write_json_atomic(batch_info_path, current)
                write_ok = True
                break
            except Exception:
                time.sleep(sleep_backoff)
                sleep_backoff *= 1.3
                continue

        if not write_ok:
            warn_msg = f"Failed to write batch_info after {max_attempts} attempts for {physical_file_name}"
            logger.warning(warn_msg)
            print(f" {warn_msg}")
            try:
                insert_job_execution_log(
                    conn,
                    client_id,
                    job_name,
                    "WARNING",
                    warn_msg,
                    parquet_name,
                    batch_id,
                    start_time,
                    datetime.now(),
                )
            except Exception:
                pass
        else:
            # success log
            try:
                insert_job_execution_log(
                    conn,
                    client_id,
                    job_name,
                    "SUCCESS",
                    None,
                    parquet_name,
                    batch_id,
                    start_time,
                    datetime.now(),
                )
            except Exception:
                pass
            logger.info(f"Converted {physical_file_name} -> {dest_path}")
            print(f"[OK] Converted {physical_file_name} -> {dest_path}")

        try:
            conn.close()
        except Exception:
            pass

        return True

    except Exception as e:
        logger.error(f"Failed to convert {physical_file_name}: {e}")
        print(f"[FAIL] Failed to convert {physical_file_name}: {e}")
        traceback.print_exc()
        return False


# -----------------------------
# CLI compatibility
# -----------------------------
def main():
    if len(sys.argv) != 3:
        print(
            "Usage: python convert_to_parquet.py <client_schema> <physical_file_name>"
        )
        sys.exit(2)

    client_schema = sys.argv[1]
    physical_file_name = sys.argv[2]

    import asyncio

    result = asyncio.run(convert_to_parquet_flow(client_schema, physical_file_name))

    if result:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
