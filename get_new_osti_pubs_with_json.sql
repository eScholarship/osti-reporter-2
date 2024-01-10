SELECT TOP 400
	p.id,
	p.title,
	p.[Type],
--	p.[publication-status],
	p.doi,
	p.journal,
	p.volume,
	p.issue,
	p.[name-of-conference],
	p.[parent-title],
	FORMAT(p.[Reporting Date 1], 'MM/dd/yyyy') as 'Reporting Date 1',
	pr.[ID] as 'Pub Record ID',
	pr.[abstract],
 	max(pr.[Data Source Proprietary ID]) as 'eSchol ID',
 	concat('ark:/13030/', max(pr.[Data Source Proprietary ID])) as 'ark',
	pr.[public-url] as 'eSchol URL',
	prf.[File URL],
	prf.[Filename],
	prf.[File Extension],
	prf.[Size] as 'File Size',

	-- Find LBL report numbers
	case when (
		p.number like '%lbl%'
		or p.number like '%LBL%'
		or p.number like '%lbnl%'
		or p.number like '%LBNL%'
		) then p.number
		else null
	end as 'LBL Report Number',

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
	) AS 'authors',
	
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
	) AS 'grants'

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
		
-- And not already sent to OSTI.
WHERE
	p.doi NOT IN (
		SELECT TOP 500 oe.doi -- TK TK "top ##" for testing
		FROM UCOPReports.osti_eschol oe)

GROUP BY
	p.id,
	p.title,
	p.[Type],
--	p.[publication-status],
	p.doi,
	p.journal,
	p.volume,
	p.issue,
	p.[name-of-conference],
	p.[parent-title],
	p.[Reporting Date 1],
 	p.[number],
	g.[funder-name],
	pr.[ID],
	pr.[abstract],
	pr.[public-url],
	prf.[File URL],
	prf.[Filename],
	prf.[File Extension],
	prf.[Size]

ORDER BY
	p.id;