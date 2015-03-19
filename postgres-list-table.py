import psycopg2
import collections
import json
import sys

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


def le_arquivo_json(filename):
    print(filename)
    f = open(filename, 'r')
    lista = []
    for row in f:
        lista.append(json.loads(row.replace('\\\\','\\')))
    return lista


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
import traceback

def migra_tabela(tabela):

    try:

        json_id_tabela = le_arquivo_json(tabela + "_ids.json")
        json_tabela = le_arquivo_json(tabela + ".json")

        linhas = len(json_id_tabela)

        upsert = Upsert(cursor, tabela)
        i=0

        while i < linhas:
            upsert.row(json_id_tabela[i] , json_tabela[i])
            i = i + 1

        return True
    except:
        traceback.print_exc(file=sys.stdout)
        return False

def desabilita_triggers(tabela):
    cursor.execute("ALTER TABLE " + tabela + " DISABLE TRIGGER ALL;")    

def habilita_triggers(tabela):
    cursor.execute("ALTER TABLE " + tabela + " ENABLE TRIGGER ALL;")

def migra_linha():
	upsert = Upsert(cursor, "tb_endereco" )
	upsert.row({'id_endereco': 100054} , {'nm_logradouro': "RUA HERACLITO", 'id_cep': 365878})


def migra_tabelas(lista_tabelas):
    return [migra_tabela(tabela) for tabela in lista_tabelas]



''' Conexao pjesup
pjesupconn = psycopg2.connect("dbname=pje user=pjeadmin password=pj3adm1n-TJMG host=linbdpje-5 port=5432")
pjesupcursor = pjesupconn.cursor()
cursor = pjesupcursor
'''

#Conexao pjetst
pjetstconn = psycopg2.connect("dbname=pje user=pjeadmin password=pj3adm1n-TJMG host=linbdpje-10 port=5432")
pjetstcursor = pjetstconn.cursor()
cursor = pjetstcursor



''' conexao pjetstcasa
pje_local_conn = psycopg2.connect("dbname=pje user=postgres password=123456 host=localhost port=5432")
pje_local_cursor = pje_local_conn.cursor()
cursor = pje_local_cursor
'''

''' # conexao pjetstlocal
pje_tstlocal_conn = psycopg2.connect("dbname=pjetst user=postgres password=Postgres1234 host=localhost port=5432")
pje_tstlocal_cursor = pje_tstlocal_conn.cursor()
cursor = pje_tstlocal_cursor
'''

cursor.execute("set search_path = public, acl, core, client, criminal, jt; SET CONSTRAINTS ALL DEFERRED;")

lista_tabelas = ['tb_classe_judicial','tb_assunto_trf','tb_competencia','tb_orgao_julgador']
lista_tabelas.reverse()
migra_tabelas(lista_tabelas)

#migra_tabela("tb_classe_judicial")
