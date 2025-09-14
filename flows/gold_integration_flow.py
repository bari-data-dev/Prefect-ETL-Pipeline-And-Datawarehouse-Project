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


def load_single_batch_file_from_success(client_schema):
    folder_path = os.path.join("batch_info", client_schema, "success")
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
        SELECT client_id FROM tools.client_reference WHERE client_schema = %s
    """,
        (client_schema,),
    )
    row = cur.fetchone()
    if not row:
        raise Exception(
            f"client_schema '{client_schema}' tidak ditemukan di client_reference"
        )
    return row[0]


def get_active_integrations(client_id, conn):
    """
    Ambil daftar prosedur dari tools.integration_config:
    - hanya is_active = true
    - filter client_id
    - ambil proc_name, table_type (dimension/fact), run_order
    - hasil diurutkan: dimension dulu (by run_order), kemudian fact (by run_order)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT proc_name, table_type, COALESCE(run_order, 0) AS run_order
            FROM tools.integration_config
            WHERE client_id = %s
              AND is_active = true
            ORDER BY
              CASE WHEN table_type = 'dimension' THEN 1 ELSE 2 END,
              COALESCE(run_order, 0),
              proc_name
        """,
            (client_id,),
        )
        rows = cur.fetchall()
        if not rows:
            raise ValueError(
                f"Tidak ditemukan integrasi aktif untuk client_id {client_id}"
            )
        return [
            {"proc_name": r[0], "table_type": r[1], "run_order": r[2]} for r in rows
        ]


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
    """
    Kontrak prosedur:
      CALL schema.proc_name(p_client_schema, p_batch_id, OUT is_success, OUT error_message);
    Pola fetchone() dipertahankan agar kompatibel dengan skrip existing.
    """
    proc_conn = None
    try:
        proc_conn = get_connection()
        proc_conn.autocommit = True
        with proc_conn.cursor() as cur:
            print(
                f"Menjalankan: CALL {proc_name}('{client_schema}', '{batch_id}', {proc_name}, NULL, NULL)"
            )
            cur.execute(
                f"CALL {proc_name}(%s, %s, %s, %s, %s);",
                (client_schema, batch_id, proc_name, None, None),
            )
            is_success, error_message = True, None
            try:
                result = cur.fetchone()
                if result is not None:
                    # Expect tuple (is_success, error_message)
                    if isinstance(result, (list, tuple)) and len(result) >= 2:
                        is_success = bool(result[0]) if result[0] is not None else True
                        error_message = result[1]
            except psycopg2.ProgrammingError:
                # Tidak ada resultset untuk di-fetch; anggap sukses (logging detail ada di DB)
                pass
            print(f"  => is_success={is_success}, error_message={error_message}")
            return is_success, error_message
    except Exception as e:
        return False, str(e)
    finally:
        if proc_conn:
            proc_conn.close()


def move_file_to(target_dir_name, src_path, client_schema):
    """
    target_dir_name ∈ {'archive','failed'}
    Source file saat ini ada di: batch_info/<client_schema>/success
    """
    base_folder = os.path.join("batch_info", client_schema)
    target_folder = os.path.join(base_folder, target_dir_name)
    os.makedirs(target_folder, exist_ok=True)
    file_name = os.path.basename(src_path)
    dest_path = os.path.join(target_folder, file_name)
    shutil.move(src_path, dest_path)
    print(f"File {file_name} dipindah ke {target_dir_name}")


def update_batch_file_with_procedures(dest_path, procedures):
    """
    Update file batch JSON di lokasi tujuan (archive/failed):
    - Jika 'integration_procedure' belum ada -> set langsung.
    - Jika sudah ada -> buat 'integration_procedure_reprocessN' (otomatis increment).
    """
    with open(dest_path, "r") as f:
        data = json.load(f)

    key_name = "integration_procedure"
    if key_name not in data:
        data[key_name] = procedures
    else:
        idx = 1
        while f"{key_name}_reprocess{idx}" in data:
            idx += 1
        data[f"{key_name}_reprocess{idx}"] = procedures

    with open(dest_path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Updated batch file {dest_path} with procedures info")


# =========================
# Dependency Handling
# =========================
def get_fact_dependencies(client_id, fact_proc_name, conn):
    """
    Ambil daftar dimensi yang menjadi dependency dari sebuah fact untuk client_id tertentu.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT dim_proc_name
            FROM tools.integration_dependencies
            WHERE client_id = %s
              AND fact_proc_name = %s
              AND is_active = true
        """,
            (client_id, fact_proc_name),
        )
        return [r[0] for r in cur.fetchall()]


def check_dependencies(client_id, batch_id, dim_proc_names, conn):
    """
    Cek seluruh dependency:
    - Return (True, []) jika semua dependency punya status 'SUCCESS' pada batch yang sama.
    - Return (False, [(proc, status_or_'MISSING'), ...]) jika ada yang gagal/absen.
    """
    if not dim_proc_names:
        return True, []

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT proc_name, status
            FROM tools.integration_log
            WHERE client_id = %s
              AND batch_id = %s
              AND proc_name = ANY(%s)
        """,
            (client_id, batch_id, dim_proc_names),
        )
        rows = cur.fetchall()
        status_map = {r[0]: r[1] for r in rows}

    failed = []
    for dep in dim_proc_names:
        st = status_map.get(dep)
        if st != "SUCCESS":  # None (missing) or not SUCCESS
            failed.append((dep, st if st is not None else "MISSING"))

    return (len(failed) == 0), failed


def insert_integration_log_skip(conn, client_id, proc_name, batch_id, failed_detail):
    """
    Insert baris SKIPPED untuk fact yang tidak dieksekusi karena dependency gagal.
    failed_detail: list of tuples (dim_proc_name, status_or_'MISSING')
    """
    if failed_detail:
        parts = [f"{d}[{s}]" for d, s in failed_detail]
        msg = "Skipped due to failed dependency: " + ", ".join(parts)
    else:
        msg = "Skipped due to failed dependency"

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tools.integration_log (
                client_id, status, record_count, proc_name, table_type, batch_id, message, start_time, end_time
            ) VALUES (%s, %s, NULL, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
            (client_id, "SKIPPED", proc_name, "fact", batch_id, msg),
        )
    conn.commit()


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
@flow(name="Gold Integration Flow")
async def gold_integration_flow(client_schema: str):
    """Execute gold integration procedures for client schema"""
    logger = get_run_logger()
    start_time = datetime.now()
    job_name = "gold_integration.py"
    client_id = None
    batch_id = None
    file_name = None
    file_path = None
    dim_procs = []
    fact_procs = []

    try:
        logger.info(f"Starting gold integration for {client_schema}")
        print(f"Starting gold integration for {client_schema}")

        # Load batch file from success folder
        try:
            batch_info, file_name, file_path = load_single_batch_file_from_success(
                client_schema
            )
        except Exception as e:
            error_msg = f"Error membaca batch file dari success: {e}"
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
        custom_name = f"{client_schema}-{batch_id}-gold-integration"
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

        logger.info(
            f"client_schema={client_schema}, client_id={client_id}, batch_id={batch_id}"
        )
        print(
            f"[INFO] client_schema={client_schema}, client_id={client_id}, batch_id={batch_id}"
        )

        # Get active integrations
        try:
            integrations = get_active_integrations(client_id, conn)
            dim_procs = [
                i["proc_name"] for i in integrations if i["table_type"] == "dimension"
            ]
            fact_procs = [i["proc_name"] for i in integrations if i["table_type"] == "fact"]

            logger.info(f"DIM to run (ordered): {dim_procs}")
            logger.info(f"FACT to run (ordered): {fact_procs}")
            print(f"[INFO] DIM to run (ordered): {dim_procs}")
            print(f"[INFO] FACT to run (ordered): {fact_procs}")
        except Exception as e:
            error_msg = f"Error getting active integrations: {e}"
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
                move_file_to("failed", file_path, client_schema)
                conn.close()
            except Exception:
                pass
            return False

        all_success = True
        error_messages = []

        # 1) Run Dimensions
        for proc_name in dim_procs:
            logger.info(f"Executing dimension procedure: {proc_name}")
            ok, err = run_procedure(proc_name, client_schema, batch_id)
            if not ok:
                all_success = False
                error_messages.append(f"{proc_name} gagal: {err}")
                logger.error(f"Dimension procedure {proc_name} failed: {err}")

        # 2) Run Facts (dependency-aware)
        for proc_name in fact_procs:
            logger.info(f"Checking dependencies for fact procedure: {proc_name}")
            deps = get_fact_dependencies(client_id, proc_name, conn)
            ok_deps, failed_detail = check_dependencies(client_id, batch_id, deps, conn)

            if not ok_deps:
                skip_msg = (
                    f"SKIP {proc_name} karena dependency tidak SUCCESS pada batch {batch_id}. "
                    f"Deps: {deps} | Failed: {failed_detail}"
                )
                logger.info(skip_msg)
                print(f"[INFO] {skip_msg}")
                insert_integration_log_skip(
                    conn, client_id, proc_name, batch_id, failed_detail
                )
                continue

            logger.info(f"Executing fact procedure: {proc_name}")
            ok, err = run_procedure(proc_name, client_schema, batch_id)
            if not ok:
                all_success = False
                error_messages.append(f"{proc_name} gagal: {err}")
                logger.error(f"Fact procedure {proc_name} failed: {err}")

        end_time = datetime.now()
        final_error_msg = "\n".join(error_messages) if error_messages else None

        procedures_run = dim_procs + fact_procs

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
                move_file_to("archive", file_path, client_schema)
                try:
                    dest_path = os.path.join(
                        "batch_info", client_schema, "archive", file_name
                    )
                    update_batch_file_with_procedures(dest_path, procedures_run)
                except Exception as e:
                    logger.warning(
                        f"Gagal update batch file dengan integration_procedure: {e}"
                    )
                    print(
                        f"[WARN] Gagal update batch file dengan integration_procedure: {e}"
                    )
                
                logger.info("Gold integration completed successfully")
                print("[OK] Gold integration completed successfully")
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
                move_file_to("failed", file_path, client_schema)
                try:
                    dest_path = os.path.join(
                        "batch_info", client_schema, "failed", file_name
                    )
                    update_batch_file_with_procedures(dest_path, procedures_run)
                except Exception as e:
                    logger.warning(
                        f"Gagal update batch file dengan integration_procedure: {e}"
                    )
                    print(
                        f"[WARN] Gagal update batch file dengan integration_procedure: {e}"
                    )
                
                logger.error(f"Gold integration failed: {final_error_msg}")
                print(f"[FAIL] Gold integration failed: {final_error_msg}")
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
        error_msg = f"Unexpected error in gold integration: {e}"
        logger.error(error_msg)
        print(f"[FAIL] {error_msg}")
        traceback.print_exc()
        
        try:
            if 'conn' in locals():
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
                    move_file_to("failed", file_path, client_schema)
                    try:
                        if client_schema is None or file_name is None:
                            raise ValueError("client_schema atau file_name bernilai None — tidak bisa membentuk path")
                        dest_path = os.path.join("batch_info", client_schema, "failed", file_name)
                        procedures_run = dim_procs + fact_procs
                        update_batch_file_with_procedures(dest_path, procedures_run)
                    except Exception as e2:
                        logger.warning(
                            f"Gagal update batch file (fatal path) dengan integration_procedure: {e2}"
                        )
                        print(
                            f"[WARN] Gagal update batch file (fatal path) dengan integration_procedure: {e2}"
                        )
                conn.close()
        except Exception:
            pass
        return False


# -----------------------------
# CLI compatibility
# -----------------------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python gold_integration_flow.py <client_schema>")
        sys.exit(2)

    client_schema = sys.argv[1]

    import asyncio

    result = asyncio.run(gold_integration_flow(client_schema))

    if result:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()