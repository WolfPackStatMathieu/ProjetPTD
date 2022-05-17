'''module Donnees pour représenter un jeu de données
'''
import numpy as np

class Donnees :
    '''représente un jeu de données

    Parameters
    ----------
    variables : list[str]
        Liste des noms de variables
    data : np.array
        donnees du jeu de donnees

    Attributes
    ----------
    variables : list[str]
        Liste des noms de variables
    data : np.array
        donnees du jeu de donnees
    var_types : list[type]
        liste des types des variables

    Examples
    --------
    >>> import numpy as np
    >>> test = Donnees(['nom', 'valeur'],np.array([['a',1], ['b', 5, ], ['c',9]]))
    '''
    # def __init__(self, nom , variables, data):
    def __init__(self, variables , data):
        self.variables = variables
        self.data = data
        self.var_types = []
        for v in self.variables:
            self.var_types.append(self.var_type(v))
        #self.nom = nom

    def get_var(self, nom_variable):
        '''Renvoie l'indice de la variable en entrée
        
        Parameters
        ----------
        nom_variable : str
            nom de la variable dont on veut l'indice

        Returns
        -------
        int
            indice de la variable dans la liste variables

        Examples
        --------
        >>> import numpy as np
        >>> test = Donnees(['nom', 'valeur'],np.array([['a',1], ['b', 5, ], ['c',9]]))
        >>> test.get_var('valeur')
        1
        '''
        for i in range(len(self.variables)) :
            if self.variables[i] == nom_variable :
                return i
        raise Exception("variable inconnue au bataillon")
        
    def var_type(self, nom_variable):
        '''Renvoie le type de la variable en entrée
        
        Parameters
        ----------
        nom_variable : str
            nom de la variable dont on veut le type

        Returns
        -------
        int
            type de la variable dans le tableau numpy

        Examples
        --------
        >>> import numpy as np
        >>> test = Donnees(['nom', 'valeur'],np.array([['a',1], ['b', 5, ], ['c',9]]))
        >>> test.var_type('valeur')
        int
        '''        
        i = self.get_var(nom_variable)
        for k in range(self.data.shape[0]):
            if self.data[k,i] != np.nan:
                return type(self.data[k,i].item())
        return np.nan
            
    def list_var(self) :
        '''renvoie la liste des variables
        '''
        return self.variables

    def __str__(self) :
        '''renvoie le tableau numpy du jeu de données sous format str'''
        return np.array2string(self.data)
    
    def add_var(self, variable_sups, donnees_sups):
        '''ajoute une liste de variable a un jeu de données
        
        Parameters
        ----------
        variables_sups : list[str]
            liste des noms des variables
        donnees_sups : numpy.array
            données des variables à ajouter

        Examples
        --------
        >>> import numpy as np
        >>> test = Donnees(['nom', 'valeur'],np.array([['a',1], ['b', 5, ], ['c',9]]))
        >>> test.add_var(['numero'],np.array([[1],[2],[3]]))
        >>> print(test)
        [['a' '1' '1']
         ['b' '5' '2']
         ['c' '9' '3']]
        '''

        if self.data.shape[0] == variable_sups.shape[0] :
            self.variables += variable_sups
            self.data = np.concatenate((self.data, donnees_sups), axis = 1)
            for v in donnees_sups:
                self.var_types.append(self.var_type(v))
        else:
            raise Exception("dimension non compatible")
        
    def del_var(self, variables_to_del):
        '''enleve une liste de variable a un jeu de données
        
        Parameters
        ----------
        variables_sups : list[str]
            liste des noms des variables

        Examples
        --------
        >>> import numpy as np
        >>> test = Donnees(['nom', 'valeur'],np.array([['a',1], ['b', 5, ], ['c',9]]))
        >>> test.del_var(['nom'])
        >>> print(test)
        [[1]
         [5]
         [9]]
        '''
        for v in variables_to_del :
            i = self.get_var(v)
            self.variables.pop(i)
            self.var_types.pop(i)
            self.data = np.delete(self.data,i ,1)
    
    def var_num(self):
        for v in self.variables:
            if self.var_type(v) != int and self.var_type(v) != float :
                self.del_var([v])
    
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




if __name__ == '__main__':
    #Test des exemples de la documentation
    import doctest
    doctest.testmod(verbose=True)
