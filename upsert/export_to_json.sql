set search_path = public, acl, core, client, criminal, jt;

COPY (select row_to_json(t)
from (
  select id_classe_judicial from tb_classe_judicial
) t ) TO  'C:\\Users\\t0085324\\workspace\\github\\python\\pycharm\\migracao_py-upsert\\tb_classe_judicial_ids.json' with (DELIMITER ',');

COPY (select row_to_json(tb_classe_judicial) from tb_classe_judicial) TO  'C:\\Users\\t0085324\\workspace\\github\\python\\pycharm\\migracao_py-upsert\\tb_classe_judicial.json' with (DELIMITER ',');