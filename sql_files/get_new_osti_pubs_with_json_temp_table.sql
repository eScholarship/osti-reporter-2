DECLARE @fiscal_year_cutoff date =
	CASE
		WHEN (MONTH(GETDATE()) >= 10) THEN
			CONVERT(VARCHAR, YEAR(GETDATE()) - 3) + '-10-01'
		ELSE
			CONVERT(VARCHAR, YEAR(GETDATE()) - 2) + '-10-01'
	END;

SELECT DISTINCT
    p.id,
 	CONCAT('https://oapolicy.universityofcalifornia.edu/viewobject.html?cid=1&id=',
 		p.id) as [Elements URL],
	p.title,
	p.[Type],
	p.[publication-status],
	pr.doi,
	p.[Canonical Journal Title] as [journal],
	p.volume,
	p.issue,
	p.[name-of-conference],
	p.[parent-title],
	FORMAT(p.[Reporting Date 1], 'MM/dd/yyyy') as [Reporting Date 1],
	FORMAT(pr.[publication-date], 'MM/dd/yyyy') as [eschol Pub Date],
	FORMAT(pr.[online-publication-date], 'MM/dd/yyyy') as [eschol Online Pub Date],
	pr.[ID] as [Pub Record ID],
	pr.[abstract],
 	max(pr.[Data Source Proprietary ID]) as [eSchol ID],
 	pr.[public-url] as [eSchol URL],
 	CONCAT('ark:/13030/', max(pr.[Data Source Proprietary ID])) as [ark],
	prf.[Filename],
	prf.[File Extension],
	prf.[Size] as [File Size],

	-- Build the file URL.
	-- Note: These PDFs are created during deposit and are live after a few seconds after deposit.
	-- DocX files are converted, and the eScholarship title page is created and added.
	-- There CAN be errors during the creation process, but it's rare in practice.
	CONCAT('https://escholarship.org/content/',
		max(pr.[Data Source Proprietary ID]),
		'/', max(pr.[Data Source Proprietary ID]), '.pdf') as [File URL],

	-- Find LBL report numbers
	case when (
		p.number like '%lbl%'
		or p.number like '%LBL%'
		or p.number like '%lbnl%'
		or p.number like '%LBNL%'
		) then p.number
		else null

	end as [LBL Report Number],

	-- Authors JSON
	(SELECT
		prp.[Last Name] as "last_name",
		prp.[First Name] as "first_name",
		prp.[Middle Names] as "middle_name",
		prp.[Email] as "email",
		prp.[Phone Number] as "phone",
		CASE
			WHEN prp.[property] = 'authors'
				THEN 'AUTHOR'
			WHEN prp.[property] = 'associated-authors'
				THEN 'AUTHOR'
			WHEN prp.[property] = 'editors'
				THEN 'CONTRIBUTING'
			ELSE NULL
		END AS "type",
		CASE
			WHEN prp.[property] = 'editors'
				THEN 'Editor'
			ELSE NULL
		END AS "contributor_type"
	FROM
		[Publication Record Person] prp
	WHERE
		prp.[Publication Record ID] = pr.id
		and prp.[Index] < 500
	FOR JSON AUTO
	) AS [authors],

	-- Grants JSON
	(SELECT
		'SPONSOR' as "type",
		g.[funder-name] as "name"
	FROM
		[Grant] g
			join [Grant Publication Relationship] gpr
				on g.id = gpr.[Grant ID]
	WHERE
		p.id = gpr.[Publication ID]
		AND
			(g.[funder-name] like '%USDOE%'
			or g.[title] like '%USDOE%'
			or g.[Computed Title] like '%USDOE%')
	FOR JSON AUTO
	) AS [grants]

FROM
	Publication p

	-- DOE grants only
	join [Grant Publication Relationship] gpr
		on p.ID = gpr.[Publication ID]
	join [Grant] g
		on g.ID = gpr.[Grant ID]
		and
			(g.[funder-name] like '%USDOE%'
			or g.[title] like '%USDOE%'
			or g.[Computed Title] like '%USDOE%')

	-- LBL users only
	join [Publication User Relationship] pur
		on p.ID = pur.[Publication ID]
	join [User] u
		on u.ID = pur.[User ID]
 		and u.[Primary Group Descriptor] like '%lbl%'

	-- Has an eScholarship pub record...
	join [Publication Record] pr
		on p.ID = pr.[Publication ID]
		and pr.[Data Source] = 'escholarship'

	-- ...with a file attached.
	join [Publication Record File] prf
		on pr.ID = prf.[Publication Record ID]
		and prf.Filename IS NOT NULL
		and prf.[File URL] IS NOT NULL

-- Not already sent to OSTI and within -1 fiscal year
WHERE
    -- The temp table #osti_eschol_db is created in elements_db_functions.py,
    -- using data from the mysql [osti_eschol_db]
	p.doi NOT IN (SELECT [doi] FROM #osti_submitted)
	and pr.[Data Source Proprietary ID] NOT IN (SELECT [eschol_id] FROM #osti_submitted)

	-- No embargoed pubs
	and (
	    prf.[Embargo Release Date] IS NULL
	    or prf.[Embargo Release Date] < GETDATE()
    )

	-- See DECLARE above for date calculation
	and (
	    p.[Reporting Date 1] >= @fiscal_year_cutoff
	    or pr.[publication-date] >= @fiscal_year_cutoff
	    -- or p.[online-publication-date] >= @fiscal_year_cutoff
	    )

GROUP BY
	p.id,
	p.title,
	p.[Type],
	p.[publication-status],
	pr.doi,
	p.[Canonical Journal Title],
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
	pr.[abstract],
	pr.[public-url],
	prf.[Filename],
	prf.[File Extension],
	prf.[Size],
	prf.[File URL]

ORDER BY
	p.id;