CREATE OR REPLACE PROCEDURE tools.load_erp_px_cat_g1v2_v1 (
    IN p_client_schema character varying,
    IN p_batch_id character varying,
    OUT is_success boolean,
    OUT error_message text
)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_sql text;
    v_count int;
    v_client_id int;
BEGIN
    -- Ambil client_id dari client_reference
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
        'SELECT COUNT(*) FROM bronze_client1.erp_px_cat_g1v2 WHERE dwh_batch_id = %L',
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
      AND table_name = 'erp_px_cat_g1v2'
      AND column_name = 'dwh_batch_id';

    IF NOT FOUND THEN
        v_sql := format(
            'ALTER TABLE %I.%I ADD COLUMN dwh_batch_id varchar(30)',
            'silver_client1', 'erp_px_cat_g1v2'
        );
        EXECUTE v_sql;
    END IF;

    -- 3. Hapus data existing di silver untuk batch ini
    EXECUTE format(
        'DELETE FROM silver_client1.erp_px_cat_g1v2 WHERE dwh_batch_id = %L',
        p_batch_id
    );

    -- 4. Insert hasil transformasi
    EXECUTE format($sql$
        INSERT INTO silver_client1.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance,
            dwh_batch_id
        )
        SELECT
            id,
            TRIM(cat) AS cat,
            TRIM(subcat) AS subcat,
            TRIM(maintenance) AS maintenance,
            %L
        FROM bronze_client1.erp_px_cat_g1v2
        WHERE dwh_batch_id = %L;
    $sql$, p_batch_id, p_batch_id);

    -- 5. Hitung jumlah record insert
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
    )
    VALUES (
        v_client_id,
        'bronze_client1.erp_px_cat_g1v2',
        'silver_client1.erp_px_cat_g1v2',
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
        )
        VALUES (
            v_client_id,
            'bronze_client1.erp_px_cat_g1v2',
            'silver_client1.erp_px_cat_g1v2',
            0,
            'FAILED',
            SQLERRM,
            p_batch_id
        );
        is_success := false;
        error_message := SQLERRM;
END;
$BODY$;

ALTER PROCEDURE tools.load_erp_px_cat_g1v2_v1 (character varying, character varying)
    OWNER TO postgres;
