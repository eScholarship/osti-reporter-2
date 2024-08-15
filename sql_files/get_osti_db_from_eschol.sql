SELECT
    id,
    doi,
    eschol_ark,
    md5,
    substring(eschol_ark,12) as 'eschol_id',
    media_response_code,
    eschol_pr_modified_when,
    prf_filename,
    prf_size
From table_replace;