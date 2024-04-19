#La structure de mes codes se décompose en 2 parties, une première partie ou je définis l'ensemble des 
#fonctions qui vont me servir, et une deuxième partie où j'applique ces fonctions 


#Partie 1 : Importer des données en les stockant localement

#On commence par importer les modules nécessaires 
import pandas as pd
import polars as pl

def local_pandas(file, separateur) :   
    #On précise quand même un paramètre séparateur qui permet de distinguer les fichiers .csv où les valeurs 
    #sont séparées par des virgules de ceux où les valeurs sont séparées par des points-virgules, 
    #le séparateur peut être déterminé en regardant le fichier fourni
    emissions_df = pd.read_csv(file, sep = separateur, index_col=0)
    #Il est important de spécifier l'index lors de l'ouverture du fichier car sinon on risque de se retrouver avec une colonne Unnamed
    return emissions_df


#N'étant pas familier avec polars, toutes les fonctions établies avec polars se font à l'aide de la documentation
#concernant polars, que j'ai trouvé en ligne principalement sur le site : 
#https://docs.pola.rs/py-polars/html/reference/api/. Pour l'importation des données cela me semble très similaire à pandas. 


def local_polars(file, separateur,types) :
    emissions_df = pl.read_csv(file, separator=separateur, dtypes= types) 
    return emissions_df

    

if __name__ == "__main__" :   
    #cette commande me permet d'éxécuter les commandes suivantes seulement si il est éxécuter depuis le même fichier, 
    #ainsi en appelant ce fichier dans d'autres fichiers les commandes suivantes seront ignorées 
    #et on peut extraire les fonctions en tant que module ainsi que les variables globales, en l'occurence les dataframes
    #On n'aura donc plus besoin d'importer nos fichiers .csv.

    emissions_df = local_pandas('IGT - Pouvoir de réchauffement global.csv', ',')      
    #Il est important de spécifier l'index lors de l'ouverture du fichier car sinon on risque 
    #le dataframe emissions_df aura une colonne index crée alors que on peut utiliser la colonne
    #INSEE commune
    print(emissions_df.head(20)) #La fonction head(x) permet de donner un apercu des x premières colonnes 
    print(emissions_df.shape)       #Pour comparer la taille des deux tableaux entre polars et pandas

    emissions_df = local_polars('IGT - Pouvoir de réchauffement global.csv', ',', {'INSEE commune' : pl.Utf8})
    print(emissions_df.head(20))    
    print(emissions_df.shape)    

    #polars dispose des fonctions similaires à pandas ce qui m'a facilité la compréhension 

    
    #je me suis rendu compte qu'il fallait préciser le type de la colonne INSEE commune en str sinon on obtenait une
    #erreur lors de la création du dataframe, sur les entrées comme 2A001 par exemple. 
    #Si l'on ignorait ces erreurs à l'aide du paramètre 'ignore_errors' cela donnait
    #bien le même dataframe qu'avec pandas (vérifier avec la fonction shape présente dans les deux bibliothèques) mais je pense qu'il est mieux de fixer le type 
    #de INSEE commune. J'ai donc converti le type de la colonne INSEE commune en chaîne de caractère 
    #à l'aide du paramètre dtypes qui permet d'affecter le type souhaité dans le dictionnaire aux colonnes présentes dans le dictionnaire. 
    #Pour déterminer l'exact syntaxe du type str avec polars j'ai demandé à chat gpt qui m'a donné la réponse
    #pl.Utf8. Cela n'était pas nécessaire avec pandas car après vérification il attribue directement le type
    #Object à la colonne INSEE commune prise comme index.

