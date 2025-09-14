CREATE OR REPLACE PROCEDURE tools.load_fact_sales_v1 (
    IN p_client_schema varchar,
    IN p_batch_id varchar,
    IN p_proc_name varchar, -- tambahan parameter untuk log
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
        'SELECT COUNT(*) FROM silver_client1.crm_sales_details WHERE dwh_batch_id = %L',
        p_batch_id
    )
    INTO v_count;

    IF v_count = 0 THEN
        is_success := TRUE;
        error_message := NULL;
        RETURN;
    END IF;

    -- Clear existing batch
    EXECUTE format(
        'DELETE FROM gold_client1.fact_sales WHERE dwh_batch_id = %L',
        p_batch_id
    );

    -- Insert into fact with surrogate key lookup + fallback -1
    EXECUTE format($sql$
        INSERT INTO gold_client1.fact_sales (
            order_number,
            customer_key,
            product_key,
            order_date,
            shipping_date,
            due_date,
            sales,
            quantity,
            price,
            dwh_batch_id
        )
        SELECT
            sd.sls_ord_num,
            COALESCE(cs.customer_key, -1) AS customer_key, -- fallback unknown member
            COALESCE(pr.product_key, -1) AS product_key,   -- fallback unknown member
            sd.sls_order_dt,
            sd.sls_ship_dt,
            sd.sls_due_dt,
            sd.sls_sales,
            sd.sls_quantity,
            sd.sls_price,
            %L
        FROM silver_client1.crm_sales_details sd
        LEFT JOIN gold_client1.dim_products pr
               ON sd.sls_prd_key = pr.product_number
               AND sd.dwh_batch_id = pr.dwh_batch_id
        LEFT JOIN gold_client1.dim_customers cs
               ON sd.sls_cust_id = cs.customer_id
               AND sd.dwh_batch_id = cs.dwh_batch_id
        WHERE sd.dwh_batch_id = %L;
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
        'fact',
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
            'fact',
            p_batch_id,
            SQLERRM,
            NOW()
        );

        is_success := FALSE;
        error_message := SQLERRM;
END;
$$;
