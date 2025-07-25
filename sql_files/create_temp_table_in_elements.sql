CREATE TABLE #osti_submitted (
    osti_id INT,
    elements_id INT,
    doi VARCHAR(80),
    eschol_id VARCHAR(80),
    eschol_pr_modified_when DATETIME,
    prf_filename VARCHAR(200),
    prf_size BIGINT,
    media_response_code INT,
    media_id INT,
    media_file_id INT,
    media_id_deleted BIT
);