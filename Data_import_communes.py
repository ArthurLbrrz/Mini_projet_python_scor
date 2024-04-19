#La structure de mes codes se décompose en 2 parties, une première partie ou je définis l'ensemble des 
#fonctions qui vont me servir, et une deuxième partie où j'applique ces fonctions 

#Partie 3 : Importer les données du fichier donnees_communes.csv

#On importe les modules requis, on peut réutiliser la fonction local du fichier Data_import_local.py

import pandas as pd
import polars as pl
from Data_import_local import local_pandas
from Data_import_local import local_polars



if __name__ == '__main__' :
    file = 'donnees_communes.csv'

    #Avec Pandas : 

    communes_df = local_pandas(file,';')    
    #On peut maintenant réutiliser la fonction local ici, 
    #même si au vu de la simplicité de la fonction local ce n'est pas forcément nécessaire ici. 
    #Après ouverture du fichier on voit que le séparateur est un point virgule donc on prend soin de le changer dans les paramètres 
    
    print(communes_df.head(20))    #On affiche les 20 premières lignes du tableau à l'aide de la fonction head()
    print(f'La taille du dataframe pandas est de : {communes_df.shape}')

    #print(communes_df.dtypes)
    # On définit un dictionnaire type pour préciser à polars quels types attribuer pour les colonnes 
    #ayant des valeurs numériques et litérales, je masque la ligne après l'avoir fait pour ne plus l'afficher dans la console.

    types = {'Région' : pl.Utf8, 'DEP' : pl.Utf8, 'CODCAN' : pl.Utf8, 'COM' : pl.Utf8} 

    
    #Avec Polars

    communes_df = local_polars(file,';', types)
    print(communes_df.head(20))
    print(f'La taille du dataframe polars est de : {communes_df.shape}')

#On obtient bien deux dataframe de même dimensions, pour pandas la taille affichée est (34970,10)
# auquel on ajoute la colonne d'index soit 11 colonnes, et pour polars (34970,10)