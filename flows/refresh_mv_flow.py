import os
import sys
import json
import psycopg2
import shutil
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
    """
    Use DB_CONFIG (loaded from .env) to create a psycopg2 connection.
    """
    return psycopg2.connect(**DB_CONFIG)


def load_single_batch_file(client_schema):
    folder_path = os.path.join("batch_info", client_schema, "archive")
    json_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".json")]

    if len(json_files) == 0:
        raise FileNotFoundError(f"Tidak ada file JSON batch di folder {folder_path}")
    if len(json_files) > 1:
        raise RuntimeError(
            f"Lebih dari 1 file JSON batch ditemukan di folder {folder_path}, harap hanya ada 1 file."
        )

    file_name = json_files[0]
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, "r") as f:
        data = json.load(f)
    return data, file_name, file_path


def get_client_id(cur, client_schema):
    cur.execute(
        """
        SELECT client_id 
        FROM tools.client_reference 
        WHERE client_schema = %s
        """,
        (client_schema,),
    )
    row = cur.fetchone()
    if not row:
        raise Exception(
            f"client_schema '{client_schema}' tidak ditemukan di client_reference"
        )
    return row[0]


def get_active_mvs(client_id, conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT mv_proc_name
            FROM tools.mv_refresh_config
            WHERE client_id = %s
              AND is_active = true
            ORDER BY mv_id
            """,
            (client_id,),
        )
        rows = cur.fetchall()
        if not rows:
            raise ValueError(f"Tidak ditemukan MV aktif untuk client_id {client_id}")
        return [row[0] for row in rows]


def insert_job_execution_log(
    conn,
    job_name,
    client_id,
    status,
    start_time,
    end_time,
    error_message,
    file_name,
    batch_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tools.job_execution_log (
                job_name, client_id, status, start_time, end_time, error_message, file_name, batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                job_name,
                client_id,
                status,
                start_time,
                end_time,
                error_message,
                file_name,
                batch_id,
            ),
        )
    conn.commit()


def run_procedure(proc_name, client_schema, batch_id):
    proc_conn = None
    try:
        proc_conn = get_connection()
        proc_conn.autocommit = True
        with proc_conn.cursor() as cur:
            proc_fullname = f"tools.refresh_{proc_name}"
            print(
                f"Menjalankan procedure: {proc_fullname}({client_schema}, {batch_id})"
            )
            cur.execute(
                f"CALL {proc_fullname}(%s, %s, %s, %s, %s);",
                (client_schema, batch_id, proc_fullname, None, None),
            )
            return True, None
    except Exception as e:
        return False, str(e)
    finally:
        if proc_conn:
            proc_conn.close()


def update_batch_file_with_procs(file_path, proc_names):
    with open(file_path, "r") as f:
        data = json.load(f)

    key_base = "refresh_procedure"
    if key_base not in data:
        data[key_base] = proc_names
    else:
        i = 1
        while f"{key_base}_reprocess{i}" in data:
            i += 1
        data[f"{key_base}_reprocess{i}"] = proc_names

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)


def move_file(src_path, client_schema, status):
    base_folder = os.path.dirname(os.path.dirname(src_path))
    target_folder = os.path.join(base_folder, status.lower())
    os.makedirs(target_folder, exist_ok=True)

    file_name = os.path.basename(src_path)
    dest_path = os.path.join(target_folder, file_name)
    shutil.move(src_path, dest_path)
    print(f"File {file_name} dipindah ke {target_folder}")


# Utility function untuk update flow run name
async def update_flow_run_name(new_name: str):
    logger = get_run_logger()
    run_id = getattr(flow_run, "id", None)  # runtime.flow_run.id

    if not run_id:
        logger.warning(
            "flow_run.id kosong - flow ini tampaknya dijalankan di luar Prefect runtime (mis. 'python script.py'). "
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
# Main Prefect Flow
# -----------------------------
@flow(name="Refresh MV Flow")
async def refresh_mv_flow(client_schema: str):
    """Refresh materialized views for client schema"""
    logger = get_run_logger()
    start_time = datetime.now()
    job_name = "refresh_mv.py"
    client_id = None
    batch_id = None
    file_name = None
    file_path = None

    try:
        logger.info(f"Starting MV refresh for {client_schema}")
        print(f"Starting MV refresh for {client_schema}")

        # Load batch file from archive folder
        try:
            batch_info, file_name, file_path = load_single_batch_file(client_schema)
        except Exception as e:
            error_msg = f"Error membaca batch file: {e}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            return False

        batch_id = batch_info.get("batch_id")
        if not batch_id:
            error_msg = "batch_id tidak ditemukan di file batch info"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            return False

        # ===== CUSTOM NAME UPDATE =====
        custom_name = f"{client_schema}-{batch_id}-refresh-mv"
        success_update = await update_flow_run_name(custom_name)
        if success_update:
            logger.info(f"Flow run name updated to: {custom_name}")
        # ================================

        # Get database connection and client_id
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                client_id = get_client_id(cur, client_schema)
        except Exception as e:
            error_msg = f"Error getting client_id: {e}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            traceback.print_exc()
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            return False

        # Get active MVs
        try:
            proc_names = get_active_mvs(client_id, conn)
            logger.info(
                f"MV aktif untuk client '{client_schema}' (client_id={client_id}): {proc_names}"
            )
            print(
                f"MV aktif untuk client '{client_schema}' (client_id={client_id}): {proc_names}"
            )
        except Exception as e:
            error_msg = f"Error getting active MVs: {e}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            traceback.print_exc()
            try:
                insert_job_execution_log(
                    conn,
                    job_name,
                    client_id,
                    "FAILED",
                    start_time,
                    datetime.now(),
                    str(e),
                    file_name,
                    batch_id,
                )
                move_file(file_path, client_schema, "failed")
                conn.close()
            except Exception:
                pass
            return False

        # Execute MV refresh procedures
        all_success = True
        error_messages = []

        for proc_name in proc_names:
            logger.info(f"Executing MV refresh procedure: {proc_name}")
            is_success, error_message = run_procedure(
                proc_name, client_schema, batch_id
            )
            if not is_success:
                all_success = False
                error_messages.append(f"{proc_name} gagal: {error_message}")
                logger.error(f"MV procedure {proc_name} failed: {error_message}")

        end_time = datetime.now()
        final_error_msg = "\n".join(error_messages) if error_messages else None

        # Update batch file with procedures
        try:
            update_batch_file_with_procs(file_path, proc_names)
        except Exception as e:
            error_msg = f"Error updating batch file: {e}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")

        # Handle success/failure
        if all_success:
            try:
                insert_job_execution_log(
                    conn,
                    job_name,
                    client_id,
                    "SUCCESS",
                    start_time,
                    end_time,
                    None,
                    file_name,
                    batch_id,
                )
                move_file(file_path, client_schema, "refreshed")
                logger.info("MV refresh completed successfully")
                print("[OK] MV refresh completed successfully")
                conn.close()
                return True
            except Exception as e:
                logger.error(f"Error in success handling: {e}")
                print(f"[FAIL] Error in success handling: {e}")
                traceback.print_exc()
                try:
                    conn.close()
                except Exception:
                    pass
                return False
        else:
            try:
                insert_job_execution_log(
                    conn,
                    job_name,
                    client_id,
                    "FAILED",
                    start_time,
                    end_time,
                    final_error_msg,
                    file_name,
                    batch_id,
                )
                move_file(file_path, client_schema, "failed")
                logger.error(f"MV refresh failed: {final_error_msg}")
                print(f"[FAIL] MV refresh failed: {final_error_msg}")
                conn.close()
                return False
            except Exception as e:
                logger.error(f"Error in failure handling: {e}")
                print(f"[FAIL] Error in failure handling: {e}")
                traceback.print_exc()
                try:
                    conn.close()
                except Exception:
                    pass
                return False

    except Exception as e:
        end_time = datetime.now()
        error_msg = f"Unexpected error in MV refresh: {e}"
        logger.error(error_msg)
        print(f"[FAIL] {error_msg}")
        traceback.print_exc()

        try:
            if "conn" in locals():
                insert_job_execution_log(
                    conn,
                    job_name,
                    client_id if client_id else None,
                    "FAILED",
                    start_time,
                    end_time,
                    str(e),
                    file_name,
                    batch_id,
                )
                if file_path:
                    move_file(file_path, client_schema, "failed")
                conn.close()
        except Exception:
            pass
        return False


# -----------------------------
# CLI compatibility
# -----------------------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python refresh_mv_flow.py <client_schema>")
        sys.exit(2)

    client_schema = sys.argv[1]

    import asyncio

    result = asyncio.run(refresh_mv_flow(client_schema))

    if result:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
