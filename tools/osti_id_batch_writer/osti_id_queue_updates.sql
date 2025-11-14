INSERT INTO eschol_api_queue
SELECT
	id,
	osti_id,
	elements_id,
    eschol_id,
	0 as `updated`
from
	osti_submissions_live
where
	media_response_code between 200 and 299
	and osti_id not in (
		select eaq.osti_id
		from eschol_api_queue eaq);