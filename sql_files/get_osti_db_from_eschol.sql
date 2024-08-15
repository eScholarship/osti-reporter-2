SELECT
    id,
    osti_id,
    doi,
    elements_id,
    eschol_ark,
    substring(eschol_ark,12) as 'eschol_id',
    md5,
    media_response_code,
    eschol_pr_modified_when,
    prf_filename,
    prf_size
From table_replace;