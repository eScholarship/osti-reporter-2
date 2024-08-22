SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
BEGIN TRANSACTION;

-- Fiscal year calculation
DECLARE @fiscal_year_cutoff date =
	CASE WHEN (MONTH(GETDATE()) >= 10)
        THEN CONVERT(VARCHAR, YEAR(GETDATE()) - 3) + '-10-01'
        ELSE CONVERT(VARCHAR, YEAR(GETDATE()) - 2) + '-10-01'
	END;

-- Changeover to E-Link v2, date cutoff for metadata updates
-- DECLARE @elink_2_start datetime = '2024-08-01 00:00:00'

-- Elements and eScholarship URLs, values replaced in elements_db_functions.get_new_osti_pubs.py
DECLARE @elements_pub_url VARCHAR(120) = 'ELEMENTS_PUB_URL_REPLACE';
DECLARE @eschol_files_url VARCHAR(120) = 'ESCHOL_FILES_URL_REPLACE';

-- Main query
SELECT DISTINCT
    os.[osti_id],
    os.[eschol_id] AS [OSTI eschol_id],
    p.id,
 	CONCAT(@elements_pub_url, p.id) as [Elements URL],
	p.title,
	p.[Type],
	p.[publication-status],
	pr.doi,
	p.[name-of-conference],
	p.[parent-title],
	FORMAT(p.[Reporting Date 1], 'MM/dd/yyyy') AS [Reporting Date 1],
	FORMAT(pr.[publication-date], 'MM/dd/yyyy') AS [eschol Pub Date],
	FORMAT(pr.[online-publication-date], 'MM/dd/yyyy') AS [eschol Online Pub Date],
	pr.[ID] AS [Pub Record ID],
	pr.[abstract],
 	pr.[Data Source Proprietary ID] AS [eSchol ID],
 	pr.[Modified When] as [eschol_pr_modified_when],
 	pr.[public-url] AS [eSchol URL],
 	CONCAT('ark:/13030/', pr.[Data Source Proprietary ID]) AS [ark],
	prf.[Filename],
	prf.[File Extension],
	prf.[Size] AS [File Size],

	-- Build the file URL.
	-- Note: These PDFs are created during deposit and are live after a few seconds after deposit.
	-- DOCX files are converted to PDFs, and the eScholarship title page is created and prepended.
	-- There CAN be errors during the creation process, but it's rare in practice.
	CONCAT(@eschol_files_url, pr.[Data Source Proprietary ID],
		'/', pr.[Data Source Proprietary ID], '.pdf') AS [File URL],

	-- Use a fallback for journals without Canonical Titles (eg. bioarxive)
    CASE WHEN p.[Canonical Journal Title] IS NULL
	    THEN p.[journal]
	    ELSE p.[Canonical Journal Title]
	END AS [Journal Name],

	-- If pub. volume/issue are null, use eScholarship volume/issue
    CASE WHEN p.[volume] IS NULL
	    THEN pr.[volume]
	    ELSE p.[volume]
	END AS [volume],

    CASE WHEN p.[issue] IS NULL
	    THEN pr.[issue]
	    ELSE p.[issue]
	END AS [issue],

	-- Find LBL report numbers
	CASE WHEN (UPPER(p.number) LIKE '%LBL%' OR UPPER(p.number) LIKE '%LBNL%')
		THEN p.number
		ELSE NULL
	END AS [LBL Report Number],

	-- JSON fields, general note:
	-- We're using these JSON fields to aggregate multiple rows from other tables
	-- (e.g. authors and grants), so it makes sense to use subqueries here.
	-- FOR JSON (PATH|AUTO) can be used with tables in the FROM clause, but the
	-- selected fields then need to be listed in GROUP BY, thereby negating the aggregation.

	-- Authors JSON
	(SELECT
		prp.[Last Name] AS "last_name",
		prp.[First Name] AS "first_name",
		prp.[Middle Names] AS "middle_name",
		prp.[Email] AS "email",
		prp.[Phone Number] AS "phone",

		CASE
			WHEN prp.[property] = 'authors'
				THEN 'AUTHOR'
			WHEN prp.[property] = 'associated-authors'
				THEN 'AUTHOR'
			WHEN prp.[property] = 'editors'
				THEN 'CONTRIBUTING'
			ELSE NULL
		END AS "type",

		CASE WHEN prp.[property] = 'editors'
			THEN 'Editor'
			ELSE NULL
		END AS "contributor_type"

	FROM
		[Publication Record Person] prp

	-- Note: The 500-author limit is a carryover from the OSTI v1 ruby,
    -- it limits submission data size for large-authored publications.
	WHERE
		prp.[Publication Record ID] = pr.id
		and prp.[Index] < 500

	FOR JSON AUTO
	) AS [authors],

	-- Grants JSON
	(SELECT
		'SPONSOR' AS "type",
		g.[funder-name] AS "name"
	FROM
		[Grant] g
			JOIN [Grant Publication Relationship] gpr
				ON g.id = gpr.[Grant ID]
	WHERE
		p.id = gpr.[Publication ID]
		AND g.[funder-name] LIKE '%USDOE%'

	FOR JSON AUTO
	) AS [grants],

	-- Supplemental Files JSON
	(SELECT
		CONCAT(@eschol_files_url, pr.[Data Source Proprietary ID],
		    '/supp/', supp_files.[Filename]) AS "url",
		supp_files.[File Extension] AS "file_extension"
	FROM
		[publication record file] supp_files
	WHERE
		supp_files.[Publication Record ID] = pr.[ID]
		AND supp_files.[Proprietary ID] LIKE '%/supp/%'

	FOR JSON AUTO
	) AS [Supplemental Files]

FROM
	Publication p

	-- Pubs w/ DOE grants only
	-- Note: This cut ensures only pubs w/ DOE grants are selected.
	-- The grants themselves are aggregated in the JSON subquery above.
	JOIN [Grant Publication Relationship] gpr
		ON p.ID = gpr.[Publication ID]
	JOIN [Grant] g
		ON g.ID = gpr.[Grant ID]
		AND g.[funder-name] LIKE '%USDOE%'

	-- Pubs w/ LBL authors only
	JOIN [Publication User Relationship] pur
		ON p.ID = pur.[Publication ID]
	JOIN [User] u
		ON u.ID = pur.[User ID]
 		AND u.[Primary Group Descriptor] LIKE '%lbl%'

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

	-- LEFT JOIN to the temp table, thus including #osti_submitted NULLs
	JOIN #osti_submitted os
		ON os.[doi] = pr.[doi]
		OR os.[eschol_id] = pr.[Data Source Proprietary ID]
		OR os.[elements_id] = p.[ID]

WHERE

	-- is already sent to OSTI and the pub record has been modified
    (os.[eschol_pr_modified_when] IS NOT NULL
    AND pr.[Modified When] > os.[eschol_pr_modified_when])
	
    -- Only certain publication types
	AND p.[Type] IN (
	    'Book',
        'Chapter',
        'Conference papers',
        'Internet publication',
        'Journal article',
        'Poster',
        'Report'
    )

	-- Is within one fiscal year
	AND (
	    p.[Reporting Date 1] IS NOT NULL
	    and p.[Reporting Date 1] >= @fiscal_year_cutoff
	)

	-- Is not embargoed
	AND (
	    prf.[Embargo Release Date] IS NULL
	    or prf.[Embargo Release Date] < GETDATE()
    )

GROUP BY
	p.id,
	p.title,
	p.[Type],
	p.[publication-status],
	pr.doi,
	p.[Canonical Journal Title],
	p.[journal],
	p.volume,
	p.issue,
    pr.volume,
	pr.issue,
	p.[name-of-conference],
	p.[parent-title],
	p.[Reporting Date 1],
	pr.[publication-date],
	pr.[online-publication-date],
	pr.[Modified When],
 	p.[number],
	pr.[ID],
	pr.[Data Source Proprietary ID],
	pr.[abstract],
	pr.[public-url],
	prf.[Filename],
	prf.[File URL],
	prf.[File Extension],
	prf.[Size],
	os.[doi],
	os.[osti_id],
	os.[eschol_id],
	os.[eschol_pr_modified_when]

ORDER BY
	p.id;

COMMIT TRANSACTION;