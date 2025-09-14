CREATE OR REPLACE PROCEDURE tools.load_crm_prd_info_v1(
  IN p_client_schema VARCHAR,
  IN p_batch_id VARCHAR,
  OUT is_success BOOLEAN,
  OUT error_message TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_sql TEXT;
  v_count INT;
  v_client_id INT;
BEGIN
  -- 1. Ambil client_id
  SELECT client_id
  INTO v_client_id
  FROM tools.client_reference
  WHERE client_schema = p_client_schema;

  IF v_client_id IS NULL THEN
    RAISE EXCEPTION 'Client schema % tidak ditemukan di client_reference', p_client_schema;
  END IF;

  -- 2. Validasi batch_id
  IF p_batch_id IS NULL OR trim(p_batch_id) = '' THEN
    RAISE EXCEPTION 'Batch ID tidak boleh kosong';
  END IF;

  -- 3. Cek data di bronze
  EXECUTE format(
    'SELECT COUNT(*) FROM bronze_client1.crm_prd_info WHERE dwh_batch_id = %L',
    p_batch_id
  )
  INTO v_count;

  IF v_count = 0 THEN
    is_success := true;
    error_message := NULL;
    RETURN;
  END IF;

  -- 4. Cek & tambah kolom dwh_batch_id di silver jika belum ada
  PERFORM 1
  FROM information_schema.columns
  WHERE table_schema = 'silver_client1'
    AND table_name = 'crm_prd_info'
    AND column_name = 'dwh_batch_id';

  IF NOT FOUND THEN
    v_sql := format(
      'ALTER TABLE %I.%I ADD COLUMN dwh_batch_id VARCHAR(30)',
      'silver_client1',
      'crm_prd_info'
    );
    EXECUTE v_sql;
  END IF;

  -- 5. Hapus data existing di silver untuk batch ini
  EXECUTE format(
    'DELETE FROM silver_client1.crm_prd_info WHERE dwh_batch_id = %L',
    p_batch_id
  );

  -- 6. Insert hasil transformasi
  EXECUTE format($sql$
    INSERT INTO silver_client1.crm_prd_info (
      prd_id,
      cat_id,
      prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt,
      dwh_batch_id
    )
    SELECT
      prd_id,
      TRIM(REPLACE(substring(prd_key, 1, 5), '-', '_')) AS cat_id,
      TRIM(substring(prd_key, 7, LENGTH(prd_key))) AS prd_key,
      TRIM(prd_nm) AS prd_nm,
      COALESCE(prd_cost, 0) AS prd_cost,
      CASE
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Sport'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'Unknown'
      END AS prd_line,
      CAST(prd_start_dt AS DATE) AS prd_start_dt,
      CAST(
        LEAD(prd_start_dt) OVER (
          PARTITION BY prd_key
          ORDER BY prd_start_dt
        ) - INTERVAL '1 day' AS DATE
      ) AS prd_end_dt,
      %L
    FROM bronze_client1.crm_prd_info
    WHERE dwh_batch_id = %L;
  $sql$, p_batch_id, p_batch_id);

  -- 7. Hitung jumlah record insert
  GET DIAGNOSTICS v_count = ROW_COUNT;

  -- 8. Insert log sukses
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
    'bronze_client1.crm_prd_info',
    'silver_client1.crm_prd_info',
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
      'bronze_client1.crm_prd_info',
      'silver_client1.crm_prd_info',
      0,
      'FAILED',
      SQLERRM,
      p_batch_id
    );

    is_success := false;
    error_message := SQLERRM;
END;
$$;
