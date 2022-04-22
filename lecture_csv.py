from fileinput import filename
import gzip
import csv

#Dossier où se trouve le fichier :
folder = ''
filename = 'synop.201301.csv.gz'
data = []
with gzip.open(folder + filename, mode='rt') as gzfile :
    synopreader = csv.reader(gzfile, delimiter = ';')
    for row in synopreader :
        data.append(row)

print(data[-1])