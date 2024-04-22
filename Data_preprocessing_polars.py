#The structure of the file python are divided in two parts. In the first part I defined the auxiliar function
#I will be using to treat the topic. In the second part I apply these function with detailed explanation on the choices
#I've made.

import polars as pl
from Data_import_local import local_polars

#Online documentation : https://docs.pola.rs/

#Part 1 :

#The rename function have the same syntaxe between pandas and polars.

def analyse_colonnes(dataframe) :
    colonnes = dataframe.columns
    for c in colonnes :
        print(c)

def renaming(dataframe, new_noms) :
    dataframe = dataframe.rename(new_noms)
    return dataframe

#Partie 2

"""Paremeters : (dataframe : pandas.Dataframe, colonnes_interpolate : list, colonnes_mean : list
    But : Return a dataframe with added columns with no missing values to the dataframe. The missing values in these added columns will be treated
    differently depending on the list the columns is parted of : interpolate for the columns in colonnes_interpolate
    mean if this is the other one.)
"""
#The function 'with_columns' found on the StackOverflow forum, can modify or create column based on a given expression.
#The syntaxe has been found on the same forum with a detailed example. The function interpolate and fill_null have been 
#found with the online documentation of polars. I used chatGPT and StackOverflow to understand and correct my syntaxe's mistakes.


def gestion_NaN(dataframe, col_interpolate, col_mean) :
    for c in col_interpolate :
        dataframe = dataframe.with_columns(pl.col(c).interpolate().alias(f'{c}_filled'))
    for c in col_mean :
        mean = dataframe.select(pl.col(c)).mean()
        dataframe = dataframe.with_columns(pl.col(c).fill_null(mean).alias(f'{c}_filled'))
    return dataframe

"""PArameters : (dataframe : pandas.Dataframe, colonnes : list
    But : statistics print the statistics of the columns in the list colonnes, which are in the dataframe"""

def statistics(dataframe, colonnes) :
    for c in colonnes :
        print(f'Les statistiques de la colonne {c} sont :')
        print('')
        print(dataframe.select(pl.col(c)).describe())
        print('')

#Partie 3 :

"""
Parameters : (dataframe1 : pandas.Dataframe, dataframe2 : pandas.Dataframe, colonnes : list, cle_commune : str)
But : Return one dataframe with all columns from colonnes merged on dataframe1 from dataframe2 based on the cle_commune  
"""

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
    
    #For the missing values part I proceed just like the pandas part.
    
    colonnes_interpolate = ['transports', 'transports_international']
    colonnes_mean = ['agriculture', 'dechets', 'energie', 'industrie_hors-energie', 'residentiel', 'routier']
    emissions_df = gestion_NaN(emissions_df,colonnes_interpolate,colonnes_mean)
    columns_describe = ['agriculture_filled','transports_filled', 'transports_international_filled', 'biomasse_hors-total_co2', 'dechets_filled', 'energie_filled', 'industrie_hors-energie_filled', 'residentiel_filled', 'routier_filled', 'tertiaire']
    
    #Again the interpolate function forget about 7 and 24 values in the transports related columns. So again I replace 
    #these values directly here without an auxiliar function.
    
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

    #Just like with the pandas Dataframe, there is the same problem of shape between the two dataframe. I treated 
    #the problem the same way I did with pandas i.e with a INNER merge.
    

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

    #Again values are missing but there is no differences with the pandas dataframe (on the shape aspect)

    #emissions_communes_df.write_csv('C:/Users/arthu/Documents/Centrale/important/stage 2A/entretien/Scor/Test/emissions_communes_polars.csv')
    
    #Finally I save the dataframe in a .csv file to reopen it easily in 3rd part.