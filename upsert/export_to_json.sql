set search_path = public, acl, core, client, criminal, jt;

COPY (select string_agg(row_to_json(t), ',')
from (
  select id_classe_judicial from tb_classe_judicial
) t ) TO  'C:\\Users\\Alexandre\\workspace\\github\\python\\pycharm\\migracao_py-upsert\\tb_classe_judicial_ids.json' with (DELIMITER ',');

select string_agg(row_to_json(t), ',')
from (
  select id_classe_judicial from tb_classe_judicial
) t 