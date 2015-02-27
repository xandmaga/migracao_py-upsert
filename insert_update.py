import json

def carrega_arquivo(arquivo):
	with open(arquivo) as json_file:
	    return json.load(json_file)
	    

