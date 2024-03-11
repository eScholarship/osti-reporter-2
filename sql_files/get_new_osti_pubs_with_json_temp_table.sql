DECLARE @fiscal_year_cutoff date =
	CASE
		WHEN (MONTH(GETDATE()) >= 10) THEN
			CONVERT(VARCHAR, YEAR(GETDATE()) - 3) + '-10-01'
		ELSE
			CONVERT(VARCHAR, YEAR(GETDATE()) - 2) + '-10-01'
	END;

SELECT DISTINCT
	os.[doi] AS [OSTI doi],
	os.[eschol_id] AS [OSTI eschol_id],
    p.id,
 	CONCAT('https://oapolicy.universityofcalifornia.edu/viewobject.html?cid=1&id=',
 		p.id) as [Elements URL],
	p.title,
	p.[Type],
	p.[publication-status],
	pr.doi,
	p.volume,
	p.issue,
	p.[name-of-conference],
	p.[parent-title],
	FORMAT(p.[Reporting Date 1], 'MM/dd/yyyy') AS [Reporting Date 1],
	FORMAT(pr.[publication-date], 'MM/dd/yyyy') AS [eschol Pub Date],
	FORMAT(pr.[online-publication-date], 'MM/dd/yyyy') AS [eschol Online Pub Date],
	pr.[ID] AS [Pub Record ID],
	pr.[abstract],
 	max(pr.[Data Source Proprietary ID]) AS [eSchol ID],
 	pr.[public-url] AS [eSchol URL],
 	CONCAT('ark:/13030/', max(pr.[Data Source Proprietary ID])) AS [ark],
	prf.[Filename],
	prf.[File Extension],
	prf.[Size] AS [File Size],

	-- Build the file URL.
	-- Note: These PDFs are created during deposit and are live after a few seconds after deposit.
	-- DocX files are converted, and the eScholarship title page is created and added.
	-- There CAN be errors during the creation process, but it's rare in practice.
	CONCAT('https://escholarship.org/content/',
		max(pr.[Data Source Proprietary ID]),
		'/', max(pr.[Data Source Proprietary ID]), '.pdf') AS [File URL],

	-- Use a fallback for journals without Canonical titles (eg. bioarxive)
    CASE WHEN p.[Canonical Journal Title] IS NULL
	    THEN p.[journal]
	    ELSE p.[Canonical Journal Title]
	END AS [Journal Name],

	-- Find LBL report numbers
	CASE WHEN (
		p.number LIKE '%lbl%'
		OR p.number LIKE '%LBL%'
		OR p.number LIKE '%lbnl%'
		OR p.number LIKE '%LBNL%'
		) THEN p.number
		ELSE NULL
	END AS [LBL Report Number],

	-- Authors JSON
	-- Note: The 500-author limit is a carryover from the OSTI v1 ruby,
    -- it limits submission data size for thousand-author publications.
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
		END AS [type],
		CASE
			WHEN prp.[property] = 'editors'
				THEN 'Editor'
			ELSE NULL
		END AS [contributor_type]
	FROM
		[Publication Record Person] prp
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
		CONCAT('https://escholarship.org/content/',
		    pr.[Data Source Proprietary ID], '/supp/',
			supp_files.[Filename]) AS [url],
		supp_files.[File Extension] as [file_extension]
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
	JOIN [Publication Record] pr
		ON p.ID = pr.[Publication ID]
		AND pr.[Data Source] = 'escholarship'

	-- ...with a file attached.
	-- Cut for "supp" ensures only one prf per publication record.
	-- Supplemental files are aggregated into JSON during SELECT.
	JOIN [Publication Record File] prf
		ON pr.ID = prf.[Publication Record ID]
		AND prf.Filename IS NOT NULL
		AND prf.[File URL] IS NOT NULL
		AND prf.[Proprietary ID] NOT LIKE ('%/supp/%')

	-- LEFT JOIN to the temp table, thus including #osti_submitted NULLs
	LEFT JOIN #osti_submitted os
		ON os.[doi] = pr.[doi]
		OR os.[eschol_id] = pr.[Data Source Proprietary ID]

WHERE
	-- Exclude certain pub types
	p.[Type] NOT IN ('Other', 'Preprint', 'Presentation')

	-- Not already sent to OSTI
    AND os.[doi] IS NULL
	AND os.[eschol_id] IS NULL

	-- Is within one fiscal year
	AND p.[Reporting Date 1] >= @fiscal_year_cutoff

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
	p.[name-of-conference],
	p.[parent-title],
	p.[Reporting Date 1],
	pr.[publication-date],
	pr.[online-publication-date],
 	p.[number],
	g.[funder-name],
	pr.[ID],
	pr.[Data Source Proprietary ID],
	pr.[abstract],
	pr.[public-url],
	prf.[Filename],
	prf.[File URL],
	prf.[File Extension],
	prf.[Size],
	prf.[File URL],
	os.[doi],
	os.[eschol_id]

ORDER BY
	p.id;