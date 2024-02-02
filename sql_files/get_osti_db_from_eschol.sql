SELECT
    id, doi, eschol_ark, md5, substring(eschol_ark,12) as 'eschol_id'
from osti_eschol;