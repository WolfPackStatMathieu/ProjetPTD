import gzip
import json

# Dossier où se trouve le fichier :
folder = ""
filename = "2013-01.json.gz"
with gzip.open(folder + filename, mode ='rt') as gzfile :
    data = json.load(gzfile)
