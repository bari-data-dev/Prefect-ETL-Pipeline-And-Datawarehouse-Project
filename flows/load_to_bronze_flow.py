import os
import sys
import re
import json
import tempfile
import shutil
import duckdb
import psycopg2
import pyarrow.parquet as pq
import gc
import time
import traceback
from datetime import datetime
from dotenv import load_dotenv
from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from prefect.client.orchestration import get_client

# load environment variables
load_dotenv()

# -----------------------------
# DB config via dotenv
# -----------------------------
DB_PORT = os.getenv("DB_PORT")
if DB_PORT is None:
    raise ValueError("DB_PORT not set in .env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(DB_PORT),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


# -----------------------------
# Helpers
# -----------------------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def extract_batch_id(filename: str):
    m = re.search(r"(BATCH\d{6})", filename, re.IGNORECASE)
    return m.group(1).upper() if m else None


def normalize_name(s: str):
    if s is None:
        return ""
    return str(s).strip().lower().replace(" ", "_").replace("-", "_")


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_path_literal(path: str) -> str:
    return path.replace("'", "''")


# robust move that overwrites destination (tries os.replace, falls back to copy+remove)
def safe_move(src: str, dst: str, retries: int = 5, retry_delay: float = 0.25) -> bool:
    try:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        try:
            # atomic if possible
            os.replace(src, dst)
            return True
        except Exception:
            # fallback to copy + remove
            try:
                shutil.copy2(src, dst)
            except Exception as e:
                print(f" safe_move: copy failed {src} -> {dst}: {e}")
                traceback.print_exc()
                return False

            for i in range(retries):
                try:
                    if os.path.exists(src):
                        os.remove(src)
                    return True
                except Exception as e:
                    if i < retries - 1:
                        try:
                            gc.collect()
                        except Exception:
                            pass
                        time.sleep(retry_delay)
                    else:
                        print(
                            f" safe_move: failed to remove source after copy: {src} -> {dst}: {e}"
                        )
                        traceback.print_exc()
                        return False
    except Exception as e:
        print(f" safe_move: unexpected error moving {src} -> {dst}: {e}")
        traceback.print_exc()
        return False
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
# DB helpers
# -----------------------------
def fetch_column_mapping(
    cur, client_id, logical_source_file, source_system, source_type
):
    sql = """
        SELECT source_column, target_column
        FROM tools.column_mapping
        WHERE client_id = %s
          AND logical_source_file = %s
          AND source_system = %s
          AND source_type = %s
          AND is_active = true
        ORDER BY mapping_id
    """
    cur.execute(sql, (client_id, logical_source_file, source_system, source_type))
    return cur.fetchall()


def validate_target_table_columns(
    cur, target_schema, target_table, required_target_cols
):
    sql = """
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = %s AND table_name = %s
    """
    cur.execute(sql, (target_schema, target_table))
    rows = [(r[0], r[1]) for r in cur.fetchall()]
    existing = set([c.lower() for c, _ in rows])
    missing = [c for c in required_target_cols if c.lower() not in existing]
    # return missing and also a dict of types for later use
    col_types = {c.lower(): t.lower() for c, t in rows}
    return missing, col_types


def update_file_audit_load_status(
    conn,
    client_id,
    physical_file_name,
    source_system,
    source_type,
    logical_source_file,
    batch_id,
    status,
    total_rows=None,
):
    cur = conn.cursor()
    if total_rows is None:
        sql = """
            UPDATE tools.file_audit_log
            SET load_status = %s
            WHERE client_id=%s AND physical_file_name=%s AND source_system=%s AND source_type=%s AND logical_source_file=%s AND batch_id=%s
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
    else:
        sql = """
            UPDATE tools.file_audit_log
            SET load_status = %s, total_rows = %s
            WHERE client_id=%s AND physical_file_name=%s AND source_system=%s AND source_type=%s AND logical_source_file=%s AND batch_id=%s
        """
        cur.execute(
            sql,
            (
                status,
                total_rows,
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
    sql = """
      INSERT INTO tools.job_execution_log
      (client_id, job_name, status, error_message, file_name, batch_id, start_time, end_time)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cur.execute(
        sql,
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


def insert_load_error_log(conn, client_id, error_detail, stage, file_name, batch_id):
    cur = conn.cursor()
    sql = """
      INSERT INTO tools.load_error_log (client_id, error_detail, stage, file_name, batch_id)
      VALUES (%s,%s,%s,%s,%s)
    """
    cur.execute(sql, (client_id, error_detail, stage, file_name, batch_id))
    conn.commit()
    cur.close()


# -----------------------------
# ID detection helper (strict)
# -----------------------------
import re


def is_id_candidate(col_name: str) -> bool:
    if not col_name:
        return False
    c = col_name.lower().strip()
    # exact id
    if c == "id":
        return True
    # ends with _id (most common, safe)
    if c.endswith("_id"):
        return True
    # starts with id_ (id_... patterns)
    if c.startswith("id_"):
        return True
    # allow very short abbreviations like 'cid' or 'pid' (single-letter prefix + 'id')
    if re.match(r"^[a-z]{0,1}id$", c):
        return True
    return False


# -----------------------------
# Main Prefect Flow
# -----------------------------
@flow(name="Load to Bronze Flow")
async def load_to_bronze_flow(client_schema: str, physical_file_name: str):
    """Load data from parquet to bronze layer table"""
    logger = get_run_logger()
    start_time = datetime.now()
    job_name = "Load To Bronze"
    stage = "load_to_bronze"

    try:
        logger.info(f"Starting load to bronze for {physical_file_name}")
        print(f"Starting load to bronze for {physical_file_name}")

        batch_id = extract_batch_id(physical_file_name)

        # ===== CUSTOM NAME UPDATE - TAMBAHAN INI =====
        custom_name = f"{client_schema}-{batch_id}-load"
        success_update = await update_flow_run_name(custom_name)
        if success_update:
            logger.info(f"Flow run name updated to: {custom_name}")
        # ============================================


        if not batch_id:
            error_msg = "cannot extract batch_id"
            logger.error(error_msg)
            print("[FAIL] cannot extract batch_id")
            return False

        batch_info_path = os.path.join(
            "batch_info",
            client_schema,
            "incoming",
            f"batch_output_{client_schema}_{batch_id}.json",
        )
        if not os.path.exists(batch_info_path):
            error_msg = f"batch_info not found: {batch_info_path}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            return False

        with open(batch_info_path, "r") as f:
            batch_info = json.load(f)

        file_entry = next(
            (
                x
                for x in batch_info.get("files", [])
                if x.get("physical_file_name") == physical_file_name
            ),
            None,
        )
        if not file_entry:
            error_msg = f"file {physical_file_name} not found in batch_info"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            return False

        parquet_name = file_entry.get("parquet_name")
        logical_source_file = file_entry.get("logical_source_file")
        source_system = (file_entry.get("source_system") or "").lower()
        source_type = (file_entry.get("source_type") or "").lower()
        target_schema = file_entry.get("target_schema")
        target_table = file_entry.get("target_table")
        client_id = batch_info.get("client_id")

        if not parquet_name or not target_schema or not target_table:
            error_msg = "missing metadata (parquet_name/target_schema/target_table)"
            logger.error(error_msg)
            print("[FAIL] missing metadata (parquet_name/target_schema/target_table)")
            return False

        parquet_path = os.path.join(
            "data", client_schema, source_system, "incoming", parquet_name
        )
        if not os.path.exists(parquet_path):
            error_msg = f"parquet not found: {parquet_path}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            return False

        # initialize handles so finally can always cleanup
        conn = None
        cur = None
        dconn = None
        pf = None
        tmp_csv_path = None

        try:
            conn = get_connection()
            cur = conn.cursor()

            # 1) fetch mapping
            mappings = fetch_column_mapping(
                cur, client_id, logical_source_file, source_system, source_type
            )
            if not mappings:
                msg = "Column mapping not found for this file"
                logger.error(msg)
                print("[FAIL]", msg)
                insert_load_error_log(
                    conn, client_id, msg, stage, parquet_name, batch_id
                )
                update_file_audit_load_status(
                    conn,
                    client_id,
                    physical_file_name,
                    source_system,
                    source_type,
                    logical_source_file,
                    batch_id,
                    "FAILED",
                )
                insert_job_execution_log(
                    conn,
                    client_id,
                    job_name,
                    "FAILED",
                    msg,
                    parquet_name,
                    batch_id,
                    start_time,
                    datetime.now(),
                )
                cur.close()
                conn.close()
                return False

            source_cols = [m[0] for m in mappings]
            target_cols = [m[1] for m in mappings]
            normalized_source = [normalize_name(c) for c in source_cols]

            # 2) read parquet schema then release pf immediately (avoids locks)
            try:
                pf = pq.ParquetFile(parquet_path)
                parquet_actual_cols = list(pf.schema.names)
            except Exception as e:
                msg = f"Failed to read parquet schema: {e}"
                logger.error(msg)
                print("[FAIL]", msg)
                insert_load_error_log(
                    conn, client_id, msg, stage, parquet_name, batch_id
                )
                update_file_audit_load_status(
                    conn,
                    client_id,
                    physical_file_name,
                    source_system,
                    source_type,
                    logical_source_file,
                    batch_id,
                    "FAILED",
                )
                insert_job_execution_log(
                    conn,
                    client_id,
                    job_name,
                    "FAILED",
                    msg,
                    parquet_name,
                    batch_id,
                    start_time,
                    datetime.now(),
                )
                cur.close()
                conn.close()
                return False
            finally:
                # release pyarrow object right away
                pf = None
                try:
                    gc.collect()
                except Exception:
                    pass

            normalized_parquet_map = {normalize_name(c): c for c in parquet_actual_cols}
            required_to_actual = {}
            missing_sources = []
            for src_raw, src_norm in zip(source_cols, normalized_source):
                actual = normalized_parquet_map.get(src_norm)
                if actual is None:
                    missing_sources.append(src_raw)
                else:
                    required_to_actual[src_raw] = actual

            if missing_sources:
                msg = "Source columns from mapping missing in parquet: " + ",".join(
                    missing_sources
                )
                logger.error(msg)
                print("[FAIL]", msg)
                insert_load_error_log(
                    conn, client_id, msg, stage, parquet_name, batch_id
                )
                update_file_audit_load_status(
                    conn,
                    client_id,
                    physical_file_name,
                    source_system,
                    source_type,
                    logical_source_file,
                    batch_id,
                    "FAILED",
                )
                insert_job_execution_log(
                    conn,
                    client_id,
                    job_name,
                    "FAILED",
                    msg,
                    parquet_name,
                    batch_id,
                    start_time,
                    datetime.now(),
                )
                cur.close()
                conn.close()
                return False

            # 3) Validate target table columns exist AND fetch types
            required_target_cols = list(target_cols)
            required_target_cols.append("dwh_batch_id")
            missing_target, col_types = validate_target_table_columns(
                cur, target_schema, target_table, required_target_cols
            )
            if missing_target:
                msg = "Target table missing columns: " + ",".join(missing_target)
                logger.error(msg)
                print("[FAIL]", msg)
                insert_load_error_log(
                    conn, client_id, msg, stage, parquet_name, batch_id
                )
                update_file_audit_load_status(
                    conn,
                    client_id,
                    physical_file_name,
                    source_system,
                    source_type,
                    logical_source_file,
                    batch_id,
                    "FAILED",
                )
                insert_job_execution_log(
                    conn,
                    client_id,
                    job_name,
                    "FAILED",
                    msg,
                    parquet_name,
                    batch_id,
                    start_time,
                    datetime.now(),
                )
                cur.close()
                conn.close()
                return False

            # 4) Use DuckDB to export CSV
            try:
                dconn = duckdb.connect(database=":memory:")
            except Exception as e:
                msg = f"Failed to start DuckDB: {e}"
                logger.error(msg)
                print("[FAIL]", msg)
                insert_load_error_log(
                    conn, client_id, msg, stage, parquet_name, batch_id
                )
                update_file_audit_load_status(
                    conn,
                    client_id,
                    physical_file_name,
                    source_system,
                    source_type,
                    logical_source_file,
                    batch_id,
                    "FAILED",
                )
                insert_job_execution_log(
                    conn,
                    client_id,
                    job_name,
                    "FAILED",
                    msg,
                    parquet_name,
                    batch_id,
                    start_time,
                    datetime.now(),
                )
                cur.close()
                conn.close()
                return False

            abs_parquet = os.path.abspath(parquet_path)
            parquet_path_sql = quote_path_literal(abs_parquet)

            # BUILD SELECT WITH SMART CAST FOR ID-LIKE COLUMNS
            select_exprs = []
            for src_raw, tgt in zip(source_cols, target_cols):
                actual_col = required_to_actual[src_raw]
                tgt_lower = tgt.lower()
                # decide whether to cast: use stricter is_id_candidate
                if is_id_candidate(tgt_lower):
                    tgt_type = col_types.get(tgt_lower)
                    # if DB side is text/char, do not cast (preserve original)
                    if tgt_type and ("char" in tgt_type or "text" in tgt_type):
                        sel = f"{quote_ident(actual_col)} AS {quote_ident(tgt)}"
                    # if DB side is integer-like -> cast safely via ROUND(... AS DOUBLE) -> BIGINT
                    elif tgt_type and "int" in tgt_type:
                        sel = f"CAST(ROUND(CAST({quote_ident(actual_col)} AS DOUBLE)) AS BIGINT) AS {quote_ident(tgt)}"
                        print(
                            f" Casting column {actual_col} -> {tgt} as BIGINT (id-cast)"
                        )
                    # if DB side numeric/decimal -> cast to NUMERIC
                    elif tgt_type and ("numeric" in tgt_type or "decimal" in tgt_type):
                        sel = f"CAST({quote_ident(actual_col)} AS NUMERIC) AS {quote_ident(tgt)}"
                        print(
                            f" Casting column {actual_col} -> {tgt} as NUMERIC (id-like)"
                        )
                    else:
                        # fallback: try integer-cast (best-effort)
                        sel = f"CAST(ROUND(CAST({quote_ident(actual_col)} AS DOUBLE)) AS BIGINT) AS {quote_ident(tgt)}"
                        print(
                            f" Fallback casting column {actual_col} -> {tgt} as BIGINT (id-like, unknown DB type)"
                        )
                else:
                    sel = f"{quote_ident(actual_col)} AS {quote_ident(tgt)}"
                select_exprs.append(sel)

            # append dwh_batch_id literal (string)
            select_exprs.append(f"'{batch_id}' AS {quote_ident('dwh_batch_id')}")
            select_sql = ", ".join(select_exprs)

            # create tmp CSV and ensure closed
            csv_tmp = tempfile.NamedTemporaryFile(
                prefix="load_bronze_", suffix=".csv", delete=False
            )
            tmp_csv_path = csv_tmp.name
            csv_tmp.close()

            # write CSV via DuckDB
            copy_sql = f"COPY (SELECT {select_sql} FROM read_parquet('{parquet_path_sql}')) TO '{quote_path_literal(tmp_csv_path)}' (FORMAT CSV, DELIMITER ',', HEADER FALSE)"
            dconn.execute(copy_sql)

            # rows count
            res = dconn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_path_sql}')"
            ).fetchone()
            total_rows = int(res[0]) if res and res[0] is not None else 0

            # close duckdb early to release file handles
            try:
                dconn.close()
                dconn = None
                try:
                    gc.collect()
                except Exception:
                    pass
            except Exception:
                pass

            # 5) Delete existing rows for same dwh_batch_id (in same DB transaction)
            delete_sql = f"DELETE FROM {quote_ident(target_schema)}.{quote_ident(target_table)} WHERE dwh_batch_id = %s"
            cur.execute(delete_sql, (batch_id,))

            # 6) COPY CSV into target table from tmp file
            target_columns_order = target_cols + ["dwh_batch_id"]
            cols_list_sql = ",".join([quote_ident(c) for c in target_columns_order])
            copy_in_sql = f"COPY {quote_ident(target_schema)}.{quote_ident(target_table)} ({cols_list_sql}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')"

            # ensure file closed and readable
            with open(tmp_csv_path, "r", encoding="utf-8") as fh:
                cur.copy_expert(copy_in_sql, fh)

            # commit after successful copy
            conn.commit()

            # 7) success update
            update_file_audit_load_status(
                conn,
                client_id,
                physical_file_name,
                source_system,
                source_type,
                logical_source_file,
                batch_id,
                "SUCCESS",
                total_rows,
            )
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

            # move parquet to archive (use safe_move with retries)
            try:
                archive_dir = os.path.join(
                    "data", client_schema, source_system, "archive"
                )
                os.makedirs(archive_dir, exist_ok=True)
                dest = os.path.join(archive_dir, os.path.basename(parquet_path))
                moved = safe_move(parquet_path, dest, retries=8, retry_delay=0.25)
                if not moved:
                    print(
                        " Warning: failed to move parquet to archive after retries:",
                        parquet_path,
                    )
            except Exception as e:
                print(" Warning: failed to move parquet to archive (exception):", e)
                traceback.print_exc()

            # cleanup DB cursors and connection
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

            # remove temp csv
            try:
                if tmp_csv_path and os.path.exists(tmp_csv_path):
                    os.remove(tmp_csv_path)
            except Exception:
                pass

            logger.info(
                f"Loaded {total_rows} rows into {target_schema}.{target_table} (batch {batch_id})."
            )
            print(
                f"[OK] Loaded {total_rows} rows into {target_schema}.{target_table} (batch {batch_id})."
            )
            return True

        except Exception as e:
            err_msg = f"Unhandled error in load_to_bronze: {e}"
            logger.error(err_msg)
            print("[FAIL]", err_msg)
            traceback.print_exc()
            # best-effort logging
            try:
                if conn:
                    insert_load_error_log(
                        conn, client_id, err_msg, stage, parquet_name, batch_id
                    )
                    try:
                        update_file_audit_load_status(
                            conn,
                            client_id,
                            physical_file_name,
                            source_system,
                            source_type,
                            logical_source_file,
                            batch_id,
                            "FAILED",
                        )
                    except Exception:
                        pass
                    try:
                        insert_job_execution_log(
                            conn,
                            client_id,
                            job_name,
                            "FAILED",
                            err_msg,
                            parquet_name,
                            batch_id,
                            start_time,
                            datetime.now(),
                        )
                    except Exception:
                        pass
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    try:
                        conn.close()
                    except Exception:
                        pass
            except Exception:
                pass

            # cleanup duckdb and tmp csv; move parquet to failed
            try:
                if dconn:
                    try:
                        dconn.close()
                    except Exception:
                        pass
            except Exception:
                pass

            try:
                if tmp_csv_path and os.path.exists(tmp_csv_path):
                    os.remove(tmp_csv_path)
            except Exception:
                pass

            try:
                failed_dir = os.path.join(
                    "data", client_schema, source_system, "failed"
                )
                os.makedirs(failed_dir, exist_ok=True)
                destf = os.path.join(failed_dir, os.path.basename(parquet_path))
                if os.path.exists(parquet_path):
                    ok = safe_move(parquet_path, destf, retries=8, retry_delay=0.25)
                    if not ok:
                        print(
                            " Failed to move parquet to failed after retries:",
                            parquet_path,
                        )
            except Exception:
                pass

            return False

    except Exception as e:
        logger.error(f"Failed to load to bronze for {physical_file_name}: {e}")
        print(f"[FAIL] Failed to load to bronze for {physical_file_name}: {e}")
        traceback.print_exc()
        return False


# -----------------------------
# CLI compatibility
# -----------------------------
def main():
    if len(sys.argv) != 3:
        print(
            "Usage: python load_to_bronze_flow.py <client_schema> <physical_file_name>"
        )
        sys.exit(2)

    client_schema = sys.argv[1]
    physical_file_name = sys.argv[2]

    import asyncio

    result = asyncio.run(load_to_bronze_flow(client_schema, physical_file_name))

    if result:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
