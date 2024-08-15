SELECT DISTINCT
    os.[elements_id] AS [OSTI elements_id],
    os.[eschol_id] AS [OSTI eschol_id],
    CASE
        WHEN pr.[Modified When] > os.[eschol_modified_when]
        THEN 1 ELSE NULL
        END AS [metadata_updated],
   CASE
        WHEN (os.[prf_filename] != prf.[Filename]
            OR os.[prf_size] != prf.[Size])
        THEN 1 ELSE NULL
        END AS [pdf_updated],
FROM
    #osti_submitted os
        join [Publication Record] pr
            on os.eschol_id = pr.[Data Source Proprietary ID]
        join [Publication Record File] prf
            on prf.[Publication Record ID] = pr.[ID]
    WHERE
        pr.[Modified When] > os.[eschol_modified_when]
        OR (os.[prf_filename] != prf.[Filename]
            OR os.[prf_size] != prf.[Size]);