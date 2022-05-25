import gzip
import json
from chargement import Chargement

#Dossier où se trouve le fichier :
folder = r"C:\\Users\\mathi\\Documents\\Ensai\\Projet Traitement de Données\\PTD\\"
filename = "2013-01.json.gz"
with gzip.open(folder + filename, mode = 'rt') as gzfile :
    data = json.load(gzfile)

with gzip.open(folder + filename, 'rb') as f:
    line = f.readline()
    one_line = json.loads(line)
    print(one_line)

class Chargement_json(Chargement):
    pass