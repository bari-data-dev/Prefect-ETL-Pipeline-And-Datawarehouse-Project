CREATE OR REPLACE PROCEDURE tools.refresh_<Materialized View Name> (
    IN p_client_schema varchar,
    IN p_batch_id varchar,
    IN p_proc_name varchar,
    OUT is_success boolean,
    OUT error_message text
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_client_id int;
    v_start_time timestamp := clock_timestamp();
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

    -- Refresh MV
    EXECUTE 'REFRESH MATERIALIZED VIEW gold_client1.<Materialized View Name>';

    -- Log success
    INSERT INTO tools.mv_refresh_log (
        client_id,
        status,
        proc_mv_name,
        batch_id,
        message,
        start_time,
        end_time
    )
    VALUES (
        v_client_id,
        'SUCCESS',
        p_proc_name,
        p_batch_id,
        'Materialized view refreshed successfully',
        v_start_time,
        clock_timestamp()
    );

    is_success := TRUE;
    error_message := NULL;

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO tools.mv_refresh_log (
            client_id,
            status,
            proc_mv_name,
            batch_id,
            message,
            start_time,
            end_time
        )
        VALUES (
            COALESCE(v_client_id, -1),
            'FAILED',
            p_proc_name,
            p_batch_id,
            SQLERRM,
            v_start_time,
            clock_timestamp()
        );

        is_success := FALSE;
        error_message := SQLERRM;
END;
$$;
