#La structure de mes codes se décompose en 2 parties, une première partie ou je définis l'ensemble des 
#fonctions qui vont me servir, et une deuxième partie où j'applique ces fonctions 

#Partie 2 : Importing data by using an URL, Importer des données en utilisant une URL

#On commence par importer le module utile
import pandas as pd
import polars as pl 

def url_pandas(lien,separateur) :
    emissions_df =  pd.read_csv(lien, sep = separateur, index_col=0)
    #La fonction read_csv de pandas est aussi capable de prendre des urls
    return emissions_df

def url_polars(lien,separateur,types) :
    emissions_df = pl.read_csv(lien, separator= separateur, dtypes= types)  
    #De la même manière que dans la version où on stocke localement le fichier on précise le type de la colonne INSEE commune
    return emissions_df 


if __name__ == "__main__" :
    emissions_df = url_pandas('https://koumoul.com/s/data-fair/api/v1/datasets/igt-pouvoir-de-rechauffement-global/convert', ',')
    print(emissions_df.head(20))
    print(emissions_df.shape)  #On compare les tailles des deux dataframes obtenus avec polars et pandas pour comparer

    emissions_df2 = url_polars('https://koumoul.com/s/data-fair/api/v1/datasets/igt-pouvoir-de-rechauffement-global/convert', ',', {'INSEE commune' : pl.Utf8})
    print(emissions_df2.head(20))
    print(emissions_df2.shape)

    #On obtient bien les mêmes dimensions pour les deux tableaux à savoir (35798,12), et cela coïncide avec l'autre méthode de création de dataframe.
