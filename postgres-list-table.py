import psycopg2
import collections


def consulta(query):
	cursor.execute(query)
	return cursor.fetchall()

def constroi_consulta_lista(lista_tabelas):		
	tabelas = ""
	for tabela in lista_tabelas:
		tabelas = tabelas + "'" + tabela + "'," 

	query = "SELECT distinct cl2.relname AS ref_table FROM pg_constraint as co JOIN pg_class AS cl1 ON co.conrelid=cl1.oid JOIN pg_class AS cl2 ON co.confrelid=cl2.oid WHERE co.contype='f' AND cl1.relname in (" + tabelas + ") AND cl2.relname <> cl1.relname ORDER BY cl2.relname"	
	return query.replace(",)",")")

def constroi_consulta(tabela):		

	tabela =  "'" + tabela + "'"

	query = "SELECT distinct cl2.relname AS ref_table FROM pg_constraint as co JOIN pg_class AS cl1 ON co.conrelid=cl1.oid JOIN pg_class AS cl2 ON co.confrelid=cl2.oid WHERE co.contype='f' AND cl1.relname = " + tabela + " AND cl2.relname <> cl1.relname ORDER BY cl2.relname"	
	return query

def export_csv(lista_tabelas, path):
	query_parte1 = """
CREATE OR REPLACE FUNCTION db_to_csv(path TEXT) RETURNS void AS $$
declare
   tables RECORD;
   statement TEXT;
begin
  FOR tables IN """

  	tabelas = ""
	for tabela in lista_tabelas:
		tabelas = tabelas + "'" + tabela + "'," 

	query_parte2 = ("(" + tabelas + ")").replace(",)", ")")

  	query_parte3 = """
  LOOP
    statement := 'COPY ' || tables.schema_table || ' TO ''' || path || '\' || tables.schema_table || '.csv' ||''' DELIMITER '';'' CSV HEADER';
    EXECUTE statement;
  END LOOP;
  return;  
end;
$$ LANGUAGE plpgsql;"""
	query = "SELECT db_to_csv(" + path + ");"

	

def convert_tupla_lista(lista_tupla):
	
	lista_temp = []
	for tupla in lista_tupla:
		lista_temp = lista_temp + [tupla[0]]

	return lista_temp

def consulta_tabelas_dependentes_lista(lista_tabelas, lista_tabelas_resultado):
		lista_tabelas_temp = []

		if (lista_tabelas is not None) and (lista_tabelas):
			lista_tabelas_temp = convert_tupla_lista(consulta(constroi_consulta_lista(lista_tabelas=lista_tabelas)), )
			[lista_tabelas_resultado.append(item) if item not in lista_tabelas_resultado else None for item in lista_tabelas_temp]			
			return consulta_tabelas_dependentes_lista(lista_tabelas=lista_tabelas_temp, lista_tabelas_resultado=lista_tabelas_resultado)
		else:
			return lista_tabelas_resultado


def consulta_tabelas_dependentes(tabela, lista_tabelas_resultado):
		lista_tabelas_temp = []

		if tabela is not None:
			lista_tabelas_temp = convert_tupla_lista(consulta(constroi_consulta(tabela=tabela)))
			for tabela in lista_tabelas_temp:
				if tabela not in lista_tabelas_resultado:
					lista_tabelas_resultado.append(tabela)
					consulta_tabelas_dependentes(tabela=tabela, lista_tabelas_resultado=lista_tabelas_resultado)
		else:
			return lista_tabelas_resultado			

class OrderedSet(collections.MutableSet):

    def __init__(self, iterable=None):
        self.end = end = [] 
        end += [None, end, end]         # sentinel node for doubly linked list
        self.map = {}                   # key --> [key, prev, next]
        if iterable is not None:
            self |= iterable

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        return key in self.map

    def add(self, key):
        if key not in self.map:
            end = self.end
            curr = end[1]
            curr[2] = end[1] = self.map[key] = [key, curr, end]

    def discard(self, key):
        if key in self.map:        
            key, prev, next = self.map.pop(key)
            prev[2] = next
            next[1] = prev

    def __iter__(self):
        end = self.end
        curr = end[2]
        while curr is not end:
            yield curr[0]
            curr = curr[2]

    def __reversed__(self):
        end = self.end
        curr = end[1]
        while curr is not end:
            yield curr[0]
            curr = curr[1]

    def pop(self, last=True):
        if not self:
            raise KeyError('set is empty')
        key = self.end[1][0] if last else self.end[2][0]
        self.discard(key)
        return key

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, OrderedSet):
            return len(self) == len(other) and list(self) == list(other)
        return set(self) == set(other)
'''
lista_tabelas_resultado = []
print(consulta_tabelas_dependentes_lista(lista_tabelas=["tb_classe_judicial"], lista_tabelas_resultado=lista_tabelas_resultado))
lista_tabelas_resultado = []
print(consulta_tabelas_dependentes_lista(lista_tabelas=["tb_assunto_trf"], lista_tabelas_resultado=lista_tabelas_resultado))
lista_tabelas_resultado = []
print(consulta_tabelas_dependentes_lista(lista_tabelas=["tb_competencia"], lista_tabelas_resultado=lista_tabelas_resultado))
lista_tabelas_resultado = []
print(consulta_tabelas_dependentes_lista(lista_tabelas=["tb_orgao_julgador"], lista_tabelas_resultado=lista_tabelas_resultado))
lista_tabelas_resultado = []
print(consulta_tabelas_dependentes_lista(lista_tabelas=["tb_jurisdicao"], lista_tabelas_resultado=lista_tabelas_resultado))
'''

from upsert import Upsert
from csv2json import carrega_arquivo

def migra_tabela(tabela):
	json_id_tabela = carrega_arquivo(tabela + "_ids.json")
	json_tabela = carrega_arquivo(tabela + ".json")

	linhas = len(json_id_tabela)	
	upsert = Upsert(cursor, tabela)
	i=0

	while i < linhas:	
		upsert.row(json_id_tabela[i] , json_tabela[i])
		i = i + 1

def migra_linha():
	upsert = Upsert(cursor, "tb_endereco" )
	upsert.row({'id_endereco': 100054} , {'nm_logradouro': "RUA HERACLITO", 'id_cep': 365878})





''' Conexao pjesup
pjesupconn = psycopg2.connect("dbname=pje user=pjeadmin password=pj3adm1n-TJMG host=linbdpje-5 port=5432")
pjesupcursor = pjesupconn.cursor()
'''
pje_tstlocal_conn = psycopg2.connect("dbname=pjetst user=postgres password=Postgres1234 host=localhost port=5432")
pje_tstlocal_cursor = pje_tstlocal_conn.cursor()

cursor = pje_tstlocal_cursor

cursor.execute("set search_path = public, acl, core, client, criminal, jt;")
	 
migra_tabela("tb_classe_judicial")
