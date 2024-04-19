#La structure de mes codes se décompose en 2 parties, une première partie ou je définis l'ensemble des 
#fonctions qui vont me servir, et une deuxième partie où j'applique ces fonctions 

import polars as pl
from Data_import_local import local_polars

#Source de documentation : https://docs.pola.rs/

#Partie 1 :

#La fonction rename est commune à pandas et polars, la syntaxe est la même selon la documentation de polars.

def analyse_colonnes(dataframe) :
    colonnes = dataframe.columns
    for c in colonnes :
        print(c)

def renaming(dataframe, new_noms) :
    dataframe = dataframe.rename(new_noms)
    return dataframe

#Partie 2

"""La fonction gestion_NaN va permettre de traiter les valeurs manquantes du dataframe passé en paramètre.
On spécifie également quelles colonnes vont être traitées avec la méthode interpolate dans une liste colonnes_interpolate
et les colonnes qu'on va traiter avec la moyenne dans une liste colonnes_mean, on renvoie ensuite le dataframe actualisé"""
#La fonction with_columns, trouvée sur internet sur le forum StackOverflow, permet de modifier les colonnes, le reste de 
#la syntaxe a été trouvé sur le même forum en adaptant un exemple illustratif. Les fonctions interpolate et fill_null 
#ont été trouvées en ligne via la documentation de polars. J'ai aussi utilisé chatGPT croisé avec le forum StackOverflow
#Pour comprendre et corriger mes erreurs.

def gestion_NaN(dataframe, col_interpolate, col_mean) :
    for c in col_interpolate :
        dataframe = dataframe.with_columns(pl.col(c).interpolate().alias(f'{c}_filled'))
    for c in col_mean :
        mean = dataframe.select(pl.col(c)).mean()
        dataframe = dataframe.with_columns(pl.col(c).fill_null(mean).alias(f'{c}_filled'))
    return dataframe

"""La fonction statistiques affiche les statistiques relatives aux colonnes passées en paramètre dans la liste colonnes"""


def statistics(dataframe, colonnes) :
    for c in colonnes :
        print(f'Les statistiques de la colonne {c} sont :')
        print('')
        print(dataframe.select(pl.col(c)).describe())
        print('')

#Partie 3 :

"""La fonction ajout_data permet de merge les colonnes d'un dataframe sur un autre en se basant ur une colonne présente dans les deux 
dataframe (on l'appelle cle_commune et ce sera le code insee)"""

#Pour polars la fonction merge de pandas est nommée join mais la syntaxe reste la même au vu de la documentation de polars

def merge(dataframe1, dataframe2, colonnes, cle_commune) :
    dataframe1 = dataframe1.join(dataframe2, on = cle_commune)
    dataframe1 = dataframe1.select([pl.col(c) for c in colonnes])
    return dataframe1

if __name__ == '__main__' : 

    print('Partie 1 :')
    print('')
    emissions_df_polars = local_polars('IGT - Pouvoir de réchauffement global.csv', ',', {'INSEE commune' : pl.Utf8})
    emissions_df = emissions_df_polars.clone()
    analyse_colonnes(emissions_df)
    print('')

    colonnes = {'INSEE commune' : 'commune_code_insee', 'Commune' : 'commune' , 'Agriculture' : 'agriculture', 'Autres transports' : 'transports',
                'Autres transports international' : 'transports_international', 'CO2 biomasse hors-total' : 'biomasse_hors-total_co2',
                'Déchets' : 'dechets' , 'Energie' : 'energie', 'Industrie hors-énergie' : 'industrie_hors-energie', 'Résidentiel' : 'residentiel',
                'Routier' : 'routier' , 'Tertiaire' : 'tertiaire'}
    
    emissions_df = renaming(emissions_df,colonnes)
    analyse_colonnes(emissions_df)
    print(' ')
    print(emissions_df.head())
    
    print('')
    print('')
    print('Partie 2 :')
    print('')
    #Pour la gestion des valeurs manquantes je vais procéder de la même façon que dans la partie pandas
    
    colonnes_interpolate = ['transports', 'transports_international']
    colonnes_mean = ['agriculture', 'dechets', 'energie', 'industrie_hors-energie', 'residentiel', 'routier']
    emissions_df = gestion_NaN(emissions_df,colonnes_interpolate,colonnes_mean)
    columns_describe = ['agriculture_filled','transports_filled', 'transports_international_filled', 'biomasse_hors-total_co2', 'dechets_filled', 'energie_filled', 'industrie_hors-energie_filled', 'residentiel_filled', 'routier_filled', 'tertiaire']
    

    #A nouveau on voit, à partir des statistiques qui affichent le nombre de valeurs manquantes que la fonction 
    #interpolate() n'a pas pu remplacer toutes les valeurs manquantes. Il en manque 7
    #dans transports et 24 dans transports_international. A nouveau je vais simplement les remplacer par la nouvelle valeur moyenne

    emissions_df = emissions_df.with_columns(pl.col("transports_filled").fill_null(emissions_df.select(pl.col('transports_filled')).mean()))
    emissions_df = emissions_df.with_columns(pl.col("transports_international_filled").fill_null(emissions_df.select(pl.col('transports_international_filled')).mean()))

    statistics(emissions_df, columns_describe)

    print('')
    print('')
    print('Partie 3 :')
    print('')

    types = {'Région' : pl.Utf8, 'DEP' : pl.Utf8, 'CODCAN' : pl.Utf8, 'COM' : pl.Utf8} 
    file = 'donnees_communes.csv'

    communes_df = local_polars(file,';', types)
    print(communes_df.head(20))
    print(f'La taille du dataframe communes est de : {communes_df.shape}')  #34 970 lignes
    print(f'La taille du dataframe emissions est de : {emissions_df.shape}')    #35 798 lignes

    #Comme avec la version pandas on a le même problème de taille des dataframe. Ici aussi j'ai donc fait le choix de 
    #faire un merge INNER, ce qui implique donc l'abandon de certaines données
    

    colums_to_merge = ['region_code', 'region_name', 'departement_code', 'commune_code_insee', 'commune', 
                       'population_totale', 'agriculture_filled', 'transports_filled', 
                       'transports_international_filled', 'biomasse_hors-total_co2','dechets_filled', 
                       'energie_filled', 'industrie_hors-energie_filled', 'residentiel_filled', 
                       'routier_filled', 'tertiaire']
    emissions_communes_df = communes_df.clone()
    emissions_communes_df = emissions_communes_df.select(pl.col(['REG', 'Région', 'DEP', 'COM', 'Commune', 'PTOT']))
    
    noms = {'REG' : 'region_code' , 'Région' : 'region_name' , 'DEP' : 'departement_code' , 
            'COM' : 'commune_code_insee' , 'Commune' : 'commune', 'PTOT' : 'population_totale'}
    emissions_communes_df = renaming(emissions_communes_df, noms)
    print(emissions_communes_df.head()) 

    emissions_communes_df = merge(emissions_communes_df,emissions_df,colums_to_merge,'commune_code_insee')
    print(emissions_communes_df.head())
    statistics(emissions_communes_df, columns_describe)

    print(emissions_communes_df.shape)  #34857 lignes

    #En effet on a bien perdu des données mais il n'y a pas de différence avec pandas ici.

    #emissions_communes_df.write_csv('C:/Users/arthu/Documents/Centrale/important/stage 2A/entretien/Scor/Test/emissions_communes_polars.csv')
    #Enfin on enregistre notre dataframe dans un csv pour pouvoir l'utiliser dans la partie 3. 
    #Je masque cette ligne lors de l'envoi du projet pour ne pas créer d'erreurs. 