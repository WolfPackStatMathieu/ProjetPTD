''''''
from re import I
import numpy as np

class Donnees :
        def __init__(self, variables , data):
            self.variables = variables
            self.data = data
            self.var_types = []
            for v in self.variables:
                self.var_types.append(self.var_type(v))

        def get_var(self, nom_variable):
            for i in range(len(self.variables)) :
                if self.variables[i] == nom_variable :
                    return i
            raise Exception("variable inconnue au bataillon")
        
        def var_type(self, nom_variable):
            i = self.get_var(nom_variable)
            for k in range(self.data.shape[0]):
                if self.data[k,i] != np.nan:
                    return type(self.data[k,i])
            return np.nan
            
        def list_var(self) :
            return self.variables

        def __str__(self) :
            return np.array2string(self.data)
    
        def add_var(self, variable_sups, donnees_sups):
            if self.data.shape[0] == variable_sups.shape[0] :
                self.variables += variable_sups
                self.data = np.concatenate((self.data, donnees_sups), axis = 1)
                for v in donnees_sups:
                    self.var_types.append(self.var_type(v))
            else:
                raise Exception("dimension non compatible")
        
        def delete(self, variables_to_del):
            for v in variables_to_del :
                i = self.get_var(v)
                self.variables.pop(i)
                self.var_types.pop(i)
                self.data = np.delete(self.data,i ,1)
    
        def concat(self, autres_donnees):
            assert len(self.variables) >= len(autres_donnees.variables)
            permutation = np.full(autres_donnees.data.shape, np.nan)
            for v in self.variables :
                if v in autres_donnees.variables :
                    permutation[:,self.get_var(v)] = autres_donnees.data[:,]
            self.data = np.concatenate((self.data,permutation))
        
        def filtre(self,parametres, test_filtre, keep_na = False):
            # on utilise un compteur pour éviter les problèmes d'index après suppresion lors du parcours
            i=0
            while i <self.data.shape[0]:
                x = []
                for p in parametres :
                    x.append(self.data[i,self.get_var(p)])
                if (np.nan in x and not keep_na) or not test_filtre(tuple(x)):
                    np.delete(self.data,i ,0)
                else :
                    i+=1

        def transform(self,nom, parametres, transformation, keep_na = False):
            assert not nom in self.variables
            ajout = np.full((self.data.shape[0],1),np.nan)
            for i in range(self.data.shape[0]):
                x = []
                for p in parametres :
                    x.append(self.data[i,self.get_var(p)])
                if not np.nan in x :
                    ajout[i] = transformation(tuple(x))
            self.add_var([nom],ajout)