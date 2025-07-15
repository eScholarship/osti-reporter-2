SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
BEGIN TRANSACTION;

-- Fiscal year calculation
DECLARE @fiscal_year_cutoff date =
	CASE WHEN (MONTH(GETDATE()) >= 10)
        THEN CONVERT(VARCHAR, YEAR(GETDATE()) - 3) + '-10-01'
        ELSE CONVERT(VARCHAR, YEAR(GETDATE()) - 2) + '-10-01'
	END;

-- Elements and eScholarship URLs, values replaced in elements_db_functions.get_new_osti_pubs.py
DECLARE @elements_pub_url VARCHAR(120) = 'ELEMENTS_PUB_URL_REPLACE';
DECLARE @eschol_files_url VARCHAR(120) = 'ESCHOL_FILES_URL_REPLACE';

-- Main Query
SELECT DISTINCT
    os.[osti_id],
    os.[eschol_id] AS [OSTI eschol_id],
    os.[media_id],
    os.[media_file_id],
    os.[prf_filename] as [osti_prf_filename],
    os.[prf_size] as [osti_prf_size],
    p.id,
    p.title,
    pr.[ID] AS [Pub Record ID],
    pr.[Data Source Proprietary ID] AS [eSchol ID],
    prf.[Filename],
	prf.[File Extension],
	prf.[Size] AS [File Size],

	CONCAT(@eschol_files_url, pr.[Data Source Proprietary ID],
		'/', pr.[Data Source Proprietary ID], '.pdf') AS [File URL]

FROM
    [Publication] p

	-- Has an eScholarship pub record...
    -- (select only the most recent if there's multiple)
    JOIN (SELECT
        inner_pr.id,
        inner_pr.[Data Source Proprietary ID],
        inner_pr.[Publication ID],
        inner_pr.[doi],
        inner_pr.[volume],
        inner_pr.[issue],
        inner_pr.[abstract],
        inner_pr.[publication-date],
        inner_pr.[online-publication-date],
        inner_pr.[public-url],
        inner_pr.[Modified When],
        ROW_NUMBER() OVER (
            partition BY inner_pr.[Publication ID]
            ORDER BY inner_pr.id desc) as pr_id_rank
        FROM [Publication Record] inner_pr
        WHERE inner_pr.[Data Source] = 'escholarship'
        	AND inner_pr.[Created When] >= @fiscal_year_cutoff
    ) pr
    on p.id = pr.[Publication ID]
    and pr.pr_id_rank = 1

    -- ...with a file attached.
	-- Note: Cut for "supp" ensures only one prf per publication record.
	-- Supplemental files are aggregated into JSON during SELECT.
	JOIN [Publication Record File] prf
		ON pr.ID = prf.[Publication Record ID]
		AND prf.Filename IS NOT NULL
		AND prf.[File URL] IS NOT NULL
		AND prf.[Proprietary ID] NOT LIKE ('%/supp/%')

    -- Has already been sent to OSTI
    -- And is after the E-Link 2 switchover.
    JOIN #osti_submitted os
		ON (    os.[doi] = pr.[doi]
		    OR os.[eschol_id] = pr.[Data Source Proprietary ID]
		    OR os.[elements_id] = p.[ID])
		AND os.osti_id >= 2568336

WHERE
    -- No media submission or the prf.[index]=0 file has changed
    os.[media_response_code] is NULL
	OR os.[media_response_code] > 300
	OR os.[prf_filename] != prf.[Filename]
	OR os.[prf_size] != prf.[Size]

	-- INDIVIDUAL UPDATES PUB ID LIST REPLACE

	;

COMMIT TRANSACTION;