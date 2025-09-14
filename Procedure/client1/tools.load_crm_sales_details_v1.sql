CREATE OR REPLACE PROCEDURE tools.load_crm_sales_details_v1 (
    IN  p_client_schema VARCHAR,
    IN  p_batch_id      VARCHAR,
    OUT is_success      BOOLEAN,
    OUT error_message   TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_sql       TEXT;
    v_count     INT;
    v_client_id INT;
BEGIN
    -- Ambil client_id
    SELECT client_id
    INTO v_client_id
    FROM tools.client_reference
    WHERE client_schema = p_client_schema;

    IF v_client_id IS NULL THEN
        RAISE EXCEPTION 'Client schema % tidak ditemukan di client_reference', p_client_schema;
    END IF;

    -- Validasi batch_id
    IF p_batch_id IS NULL OR trim(p_batch_id) = '' THEN
        RAISE EXCEPTION 'Batch ID tidak boleh kosong';
    END IF;

    -- 1. Cek data di bronze
    EXECUTE format(
        'SELECT COUNT(*) FROM bronze_client1.crm_sales_details WHERE dwh_batch_id = %L',
        p_batch_id
    )
    INTO v_count;

    IF v_count = 0 THEN
        is_success := true;
        error_message := NULL;
        RETURN;
    END IF;

    -- 2. Cek & tambah kolom dwh_batch_id di silver jika belum ada
    PERFORM 1
    FROM information_schema.columns
    WHERE table_schema = 'silver_client1'
      AND table_name = 'crm_sales_details'
      AND column_name = 'dwh_batch_id';

    IF NOT FOUND THEN
        v_sql := format(
            'ALTER TABLE %I.%I ADD COLUMN dwh_batch_id VARCHAR(30)',
            'silver_client1', 'crm_sales_details'
        );
        EXECUTE v_sql;
    END IF;

    -- 3. Hapus data existing di silver untuk batch ini
    EXECUTE format(
        'DELETE FROM silver_client1.crm_sales_details WHERE dwh_batch_id = %L',
        p_batch_id
    );

    -- 4. Insert hasil transformasi
    EXECUTE format($sql$
        INSERT INTO silver_client1.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price,
            dwh_batch_id
        )
        SELECT
            TRIM(sls_ord_num) AS sls_ord_num,
            TRIM(sls_prd_key) AS sls_prd_key,
            sls_cust_id,
            CASE
                WHEN sls_order_dt = 0
                  OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE
                WHEN sls_ship_dt = 0
                  OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE
                WHEN sls_due_dt = 0
                  OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE
                WHEN sls_sales IS NULL
                  OR sls_sales <= 0
                  OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL
                  OR sls_price <= 0
                    THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price,
            %L
        FROM bronze_client1.crm_sales_details
        WHERE dwh_batch_id = %L;
    $sql$, p_batch_id, p_batch_id);

    -- 5. Hitung jumlah record
    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- 6. Insert log sukses
    INSERT INTO tools.transformation_log (
        client_id,
        source_table,
        target_table,
        record_count,
        status,
        message,
        batch_id
    ) VALUES (
        v_client_id,
        'bronze_client1.crm_sales_details',
        'silver_client1.crm_sales_details',
        v_count,
        'SUCCESS',
        'Transformation completed successfully',
        p_batch_id
    );

    is_success := true;
    error_message := NULL;

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO tools.transformation_log (
            client_id,
            source_table,
            target_table,
            record_count,
            status,
            message,
            batch_id
        ) VALUES (
            v_client_id,
            'bronze_client1.crm_sales_details',
            'silver_client1.crm_sales_details',
            0,
            'FAILED',
            SQLERRM,
            p_batch_id
        );
        is_success := false;
        error_message := SQLERRM;
END;
$$;

ALTER PROCEDURE tools.load_crm_sales_details_v1 (VARCHAR, VARCHAR)
    OWNER TO postgres;
