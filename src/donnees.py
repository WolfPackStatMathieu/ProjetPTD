'''module Donnees pour représenter un jeu de données
'''
import numpy as np

class Donnees :
    '''représente un jeu de données

    Parameters
    ----------
    variables : list[str]
        Liste des noms de variables
    data : list[list]
        donnees du jeu de donnees sous format matrice
    nom : str
        nom du jeu de donnéees

    Attributes
    ----------
    nom: str
        nom du jeu de données
    variables : list[str]
        Liste des noms de variables
    data : np.array
        donnees du jeu de donnees converties en array
    var_types : list[type]
        liste des types des variables

    Examples
    --------
    >>> import numpy as np
    >>> mes_donnees = Donnees('mon_nom_jeu_de_donnees',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])
    >>> isinstance(mes_donnees, Donnees)
    True
    >>> mes_donnees.nom
    'mon_nom_jeu_de_donnees'
    >>> mes_donnees.variables
    ['nom', 'valeur']
    >>> mes_donnees.var_types
    [<class 'str'>, <class 'int'>]
    >>> type(mes_donnees.data)
    <class 'numpy.ndarray'>
    >>> print(mes_donnees.data)
    [['a' 1]
     ['b' 5]
     ['c' 9]]


    '''
    def __init__(self, nom , variables, data):
        """_summary_

        Parameters
        ----------
        nom: str
            nom du jeu de données
        variables : list[str]
            Liste des noms de variables
        data : np.array
            donnees du jeu de donnees converties en array
        """
        self.variables = variables
        self.data = np.array(data,dtype=object)
        self.var_types = []
        for v in self.variables:
            self.var_types.append(self.var_type(v))
        self.nom = nom

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
        >>> test = Donnees('nom',['nom', 'valeur'],[['a',1], ['b', 5], ['c',9]])
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
        str
            type de la variable dans le tableau numpy

        Examples
        --------
        >>> import numpy as np
        >>> test = Donnees('nom',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])
        >>> test.var_type('valeur')
        <class 'int'>
        '''
        i = self.get_var(nom_variable)
        for k in range(self.data.shape[0]):
            if self.data[k,i] != np.nan:
                return type(self.data[k,i])
        return np.nan

    def list_var(self) :
        '''renvoie la liste des variables
        '''
        print(self.variables)
        return self.variables

    def __str__(self) :
        '''renvoie le tableau numpy du jeu de données sous format str
        '''
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
        >>> test = Donnees('nom',['nom', 'valeur'],[['a',1], ['b', 5, ], ['c',9]])
        >>> test.add_var(['numero'],np.array([[1],[2],[3]], dtype=object))
        >>> print(test)
        [['a' 1 1]
         ['b' 5 2]
         ['c' 9 3]]
        '''

        if self.data.shape[0] == donnees_sups.shape[0] :
            self.variables += variable_sups
            self.data = np.concatenate((self.data, donnees_sups), axis = 1)
            for v in variable_sups:
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
        >>> test = Donnees('nom',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])
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

    def var_num(self, exceptions=[]):
        '''enleve les variable non numériques du jeu de données sauf les exceptions

        Examples
        --------
        >>> import numpy as np
        >>> test = Donnees('nom',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])
        >>> test.var_num()
        >>> print(test)
        [[1]
         [5]
         [9]]
        '''
        for v in self.variables:
            if self.var_type(v) != int and self.var_type(v) != float :
                if not v in exceptions:
                    self.del_var([v])

    def concat(self, autres_donnees):
        '''concatène 2 jeu de données selon les variables de l'objet de la méthode

        Parameters
        ----------
        autres_donnees : Donnees
            donnees à concaténer

        Examples
        --------
        >>> import numpy as np
        >>> test = Donnees('nom',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9]])
        >>> test2 =  Donnees('nom2',['nom', 'valeur'],[['d',10], ['e', 50 ], ['f',90], ['z',100]])
        >>> test.concat(test2)
        >>> print(test)
        [['a' 1]
         ['b' 5]
         ['c' 9]
         ['d' 10]
         ['e' 50]
         ['f' 90]
         ['z' 100]]
        '''
        assert len(self.variables) >= len(autres_donnees.variables)
        permutation = np.full((autres_donnees.data.shape[0], self.data.shape[1]), np.nan, dtype= object)
        for v in self.variables :
            if v in autres_donnees.variables :
                permutation[:,self.get_var(v)] = autres_donnees.data[:,autres_donnees.get_var(v)]
        self.data = np.concatenate((self.data,permutation), axis = 0)

    def filtre(self,parametres, test_filtre, keep_na = False):
        '''filtre les lignes du jeu de donnees selon une fonction booleenne a partir d'un jeu de parametres
        pris parmis les variables, keep_na determine si on garde la ligne lorsque qu'un des parametres manque

        Parameters
        ----------
        parametres : list[str]
            liste de parametres pris parmis les variables du jeu de donnees
        test_filtre : function
            fonction qui renvoit un booleen, utiliser de préférence une lambda function
        keep_na : bool
            décide si on garde les lignes où un des paramètres manque

        Examples
        --------
        >>> import numpy as np
        >>> test = Donnees('nom',['nom', 'valeur'],[['a',1], ['b', 5 ], ['c',9],['d',10], ['e', 50 ], ['f',90], ['z',100]])
        >>> test.filtre(['valeur'],lambda x : x[0] >= 50 )
        >>> print(test)
        [['e' 50]
         ['f' 90]
         ['z' 100]]
        '''
        # on utilise un compteur pour éviter les problèmes d'index après suppresion de ligne
        # lors du parcours
        i=0
        while i <self.data.shape[0]:
            t = []
            for p in parametres :
                t.append(self.data[i,self.get_var(p)])
            if (np.nan in t and not keep_na) or not test_filtre(tuple(t)):
                self.data = np.delete(self.data,i , axis = 0)
            else :
                i+=1

    def transform(self,nom, parametres, transformation):
        '''crée une nouvelle variable et ajoute une colonne au jeu de données composée des valeur qu'on a calculé

        Parameters
        ----------
        parametres : list[str]
            liste de parametres pris parmis les variables du jeu de donnees
        transformation : function
            fonction qui renvoit la valeur de la nouvelle variable pour cette ligne
        nom : str
            nom de la nouvelle variable

        Examples
        --------
        >>> import numpy as np
        >>> test = Donnees('nom',['nom', 'valeur'],[['e', 50 ], ['f',90], ['z',100]])
        >>> test.transform('nouveau',['valeur'], lambda x : x[0] + 2)
        >>> print(test)
        [['e' 50 52]
         ['f' 90 92]
         ['z' 100 102]]
        '''

        assert not nom in self.variables
        ajout = np.full((self.data.shape[0],1),np.nan, dtype= object )
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
    doctest.testmod(verbose=False)