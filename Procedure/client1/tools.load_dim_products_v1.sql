CREATE OR REPLACE PROCEDURE tools.load_dim_products_v1 (
    IN p_client_schema varchar,
    IN p_batch_id varchar,
    IN p_proc_name varchar,
    OUT is_success boolean,
    OUT error_message text
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_sql text;
    v_count int;
    v_client_id int;
BEGIN
    -- Resolve client_id
    SELECT client_id
    INTO v_client_id
    FROM tools.client_reference
    WHERE client_schema = p_client_schema;

    IF v_client_id IS NULL THEN
        RAISE EXCEPTION 'Client schema % tidak ditemukan di client_reference', p_client_schema;
    END IF;

    -- Validate batch_id
    IF p_batch_id IS NULL OR trim(p_batch_id) = '' THEN
        RAISE EXCEPTION 'Batch ID tidak boleh kosong';
    END IF;

    -- Check source data
    EXECUTE format(
        'SELECT COUNT(*) FROM silver_client1.crm_prd_info WHERE dwh_batch_id = %L',
        p_batch_id
    )
    INTO v_count;

    IF v_count = 0 THEN
        is_success := TRUE;
        error_message := NULL;
        RETURN;
    END IF;

    -- Clear existing batch in gold
    EXECUTE format(
        'DELETE FROM gold_client1.dim_products WHERE dwh_batch_id = %L',
        p_batch_id
    );

    -- Insert into gold
    EXECUTE format($sql$
        INSERT INTO gold_client1.dim_products (
            product_id,
            product_number,
            product_name,
            product_line,
            category_id,
            category,
            sub_category,
            maintenance,
            product_cost,
            start_date,
            dwh_batch_id
        )
        SELECT
            po.prd_id,
            po.prd_key,
            po.prd_nm,
            po.prd_line,
            po.cat_id,
            pc.cat,
            pc.subcat,
            pc.maintenance,
            po.prd_cost,
            po.prd_start_dt,
            %L
        FROM silver_client1.crm_prd_info po
        LEFT JOIN silver_client1.erp_px_cat_g1v2 pc
               ON po.cat_id = pc.id
               AND po.dwh_batch_id = pc.dwh_batch_id
        WHERE po.prd_end_dt IS NULL
          AND po.dwh_batch_id = %L;
    $sql$, p_batch_id, p_batch_id);

    -- Get inserted row count
    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- Log success
    INSERT INTO tools.integration_log (
        client_id,
        status,
        record_count,
        proc_name,
        table_type,
        batch_id,
        message,
        end_time
    )
    VALUES (
        v_client_id,
        'SUCCESS',
        v_count,
        p_proc_name,
        'dimension',
        p_batch_id,
        'Integration completed',
        NOW()
    );

    is_success := TRUE;
    error_message := NULL;

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO tools.integration_log (
            client_id,
            status,
            record_count,
            proc_name,
            table_type,
            batch_id,
            message,
            end_time
        )
        VALUES (
            v_client_id,
            'FAILED',
            0,
            p_proc_name,
            'dimension',
            p_batch_id,
            SQLERRM,
            NOW()
        );

        is_success := FALSE;
        error_message := SQLERRM;
END;
$$;
