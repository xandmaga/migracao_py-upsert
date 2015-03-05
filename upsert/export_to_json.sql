set search_path = public, acl, core, client, criminal, jt;

/*
	exporta tabela para json
*/
CREATE OR REPLACE FUNCTION db_to_json(path TEXT) RETURNS void AS $$
declare
  columns_id TEXT;
  tables RECORD;
  statement TEXT;
begin
  FOR tables IN 
    SELECT (table_name) AS schema_table
    FROM information_schema.tables t INNER JOIN information_schema.schemata s 
    ON s.schema_name = t.table_schema 
    WHERE t.table_name IN ('tb_classe_judicial', 'tb_assunto_trf', 'tb_competencia', 'tb_orgao_julgador', 'tb_dimensao_alcada', 'tb_aplicacao_classe', 'tb_jurisdicao', 'tb_localizacao', 'tb_endereco', 'tb_estado', 'tb_cep', 'tb_fluxo', 'tb_tipo_audiencia', 'tb_tipo_parte', 'tb_tipo_parte_trf', 'tb_usuario', 'tb_municipio', 'tb_usuario_login')
    ORDER BY schema_table
  LOOP
  	columns_id := (SELECT string_agg(a.attname, ',') FROM   pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = tables.schema_table::regclass AND i.indisprimary);  
    statement := 'COPY ' || ' (select row_to_json(' || tables.schema_table || ') from ' || tables.schema_table || ' order by ' || columns_id || ')' || ' TO ''' || path || '\\' || tables.schema_table || '.json''';
    EXECUTE statement;
  END LOOP;
  return;  
end;
$$ LANGUAGE plpgsql;
SELECT db_to_json('C:\\Users\\t0085324\\workspace\\bitbucket\\emacs\\org\\tarefas\\pje\\redmine\\14344\\pjetstlocal');


/*
	exporta coluna da tabela para json
*/
CREATE OR REPLACE FUNCTION db_to_json(path TEXT) RETURNS void AS $$
declare
  columns_id TEXT;
  tables RECORD;
  statement TEXT;
begin
  FOR tables IN 
    SELECT (table_name) AS schema_table
    FROM information_schema.tables t INNER JOIN information_schema.schemata s 
    ON s.schema_name = t.table_schema 
    WHERE t.table_name IN ('tb_classe_judicial', 'tb_assunto_trf', 'tb_competencia', 'tb_orgao_julgador', 'tb_dimensao_alcada', 'tb_aplicacao_classe', 'tb_jurisdicao', 'tb_localizacao', 'tb_endereco', 'tb_estado', 'tb_cep', 'tb_fluxo', 'tb_tipo_audiencia', 'tb_tipo_parte', 'tb_tipo_parte_trf', 'tb_usuario', 'tb_municipio', 'tb_usuario_login')
    ORDER BY schema_table
  LOOP
  	columns_id := (SELECT string_agg(a.attname, ',') FROM   pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = tables.schema_table::regclass AND i.indisprimary);
    statement := 'COPY ' || ' (select row_to_json(t) from (  select ' || columns_id || ' from ' || tables.schema_table || ' order by ' || columns_id || ' ) t )' || ' TO ''' || path || '\\' || tables.schema_table || '_ids' || '.json''';
    EXECUTE statement;
  END LOOP;
  return;  
end;
$$ LANGUAGE plpgsql;
SELECT db_to_json('C:\\Users\\t0085324\\workspace\\bitbucket\\emacs\\org\\tarefas\\pje\\redmine\\14344\\pjetstlocal');
