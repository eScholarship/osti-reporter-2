select
	osti_id,
	elements_id,
	eschol_id
from
	table_replace
where
	doi is null
	and osti_doi is null
	and date_stamp >= (SELECT
        -- Fiscal year calculation
        CASE WHEN MONTH(CURDATE()) >= 10
            THEN CONCAT(YEAR(CURDATE()), '-10-01')
            ELSE CONCAT(YEAR(CURDATE()) - 1, '-10-01')
            END
    );