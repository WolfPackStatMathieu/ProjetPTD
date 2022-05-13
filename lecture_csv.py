from fileinput import filename
import gzip
import csv
import numpy as np
#Dossier où se trouve le fichier :
folder = ''
filename = 'synop.201301.csv.gz'
data = []
with gzip.open(folder + filename, mode='rt') as gzfile :
    #.readlines()[1:3] pour ne lire que les 3 premières lignes
    synopreader = csv.reader(gzfile.readlines()[0:3], delimiter = ';')
    for row in synopreader :
        data.append(row)
print(data)
data_np = np.array(data)
print(data_np)
