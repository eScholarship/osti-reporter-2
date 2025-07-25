SELECT
    id,
    osti_id,
    doi,
    elements_id,
    eschol_ark,
    substring(eschol_ark,12) as 'eschol_id',
    md5,
    eschol_pr_modified_when,
    prf_filename,
    prf_size,
    media_response_code,
    media_id,
    media_file_id,
    media_id_deleted
From table_replace;