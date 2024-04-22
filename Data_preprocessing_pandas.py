#The structure of the file python are divided in two parts. In the first part I defined the auxiliar function
#I will be using to treat the topic. In the second part I apply these function with detailed explanation on the choices
#I've made.

import pandas as pd
from Data_import_local import local_pandas

#Partie 1

def analyse_colonnes(dataframe, new_colonnes, index_name) :
    colonnes = dataframe.columns            
    print('Les colonnes du dataframe emissions_df sont :')     
    for c in colonnes :
        print(c)
    print(' ')
    dataframe.columns = new_colonnes
    dataframe.index.names = [index_name]
    return dataframe

#Partie 2

"""Paremeters : (dataframe : pandas.Dataframe, colonnes_interpolate : list, colonnes_mean : list
    But : Return a dataframe with added columns with no missing values to the dataframe. The missing values in these added columns will be treated
    differently depending on the list the columns is parted of : interpolate for the columns in colonnes_interpolate
    mean if this is the other one.)
"""

def gestion_NaN(dataframe,colonnes_interpolate, colonnes_mean) :
    for c in colonnes_interpolate :
        dataframe[f'{c}_filled'] = dataframe[f'{c}'].copy()     #A deep copy of the column to modify is first add to the dataframe
        dataframe[f'{c}_filled'] = dataframe[f'{c}_filled'].interpolate('linear')   #Then this copy is modified with the adequate method
    for c in colonnes_mean : 
        dataframe[f'{c}_filled'] = dataframe[f'{c}'].copy()
        mean_c = dataframe[f'{c}'].mean()
        dataframe[f'{c}_filled'] = dataframe[f'{c}_filled'].fillna(mean_c)
    return dataframe

"""PArameters : (dataframe : pandas.Dataframe, colonnes : list
    But : statistics print the statistics of the columns in the list colonnes, which are in the dataframe"""

def statistics(dataframe, colonnes) :
    for c in colonnes :
        print(f'Les statistiques de la colonne {c} sont :')
        print(' ')
        print(dataframe[c].describe())
        print(' ')

#Partie 3 :

"""
Parameters : (dataframe1 : pandas.Dataframe, dataframe2 : pandas.Dataframe, colonnes : list, cle_commune : str)
But : Return one dataframe with all columns from colonnes merged on dataframe1 from dataframe2 based on the cle_commune  
"""

def ajout_data(dataframe1, dataframe2, colonnes, cle_commune) :
    for c in colonnes :
        dataframe1 = dataframe1.merge(dataframe2[c], on = cle_commune)
    return dataframe1
        

if __name__ == '__main__' :
    file = 'IGT - Pouvoir de réchauffement global.csv'

    #I use the function local presented in tthe Data_import part to create my dataframe
    
    emissions_df = local_pandas(file, ',')
    print(emissions_df.shape)

    print(' ')
    print('Partie 1 :')
    print('')

    #I rename each columns to have names in lowercase and I add '_' to replace each space,
    #This is done in one step with the function analyse_colonnes.

    #On renomme toute les colonnes en minuscule et on ajoute des '_' pour chaque espace, 
    #je le fais en une seule étape avec la fonction analyse_colonnes:

    new_colums = ['commune','agriculture','transports', 'transports_international', 'biomasse_hors-total_co2', 'dechets', 'energie', 'industrie_hors-energie', 'residentiel', 'routier', 'tertiaire']
    nom = 'commune_code_insee'
    analyse_colonnes(emissions_df, new_colums, nom)

    print(emissions_df.head(10))     #We check the dataframe if the changes implemented are correct
    
    print(' ')
    print(' ')
    #Partie 2 :
    print('Partie 2 :')

    #The describe function can be use to have a look on the statistics of the dataframe

    print(emissions_df.describe())

    #We know there is missing values in the dataframe because when we look the 10 first lines of the dataframe
    #missing values can be see on the transports related columns.

    print((emissions_df.isna().sum()/emissions_df.shape[0])*100)

    #Here I print the ratio of missing values for each columns 
    
    #The following columns contain missing values :
    #agriculture (0.17%); transports (72%); transports_international (92%); déchets (0.016%) ; energie (3.65%) ; 
    #industrie_hors-energie (3.65%) ; residentiel (0.016%) ; routier (0.056%) 

    #It is mostly the transports columns that have the most missing values. Since there is a large amount of missing
    # (92% and 72%) for these two columns, my first idea is to interpolate these missing values. For the others columns
    #since there is less missing values, it is possible to replace the missing values with the mean of the columns,
    #since it is calculated with a sufficient number of values, when it is impossible for the transports related columns
    #since the mean is not correct because there is not enough values. By looking online I found on the pandas
    #documentation the funtion interpolate() that can interpolate values. First I wanted to do a polynomial interpolation
    #but this created errors due to the nature of the index and the values in the dataframe. therefore I did a linear 
    #interpolation 

    #I add the suffix '_filled' to the completed columns added to the dataframe thanks to the function gestion_NaN:

    col_interpolate = ['transports', 'transports_international']    #The columns where missing values will be interpolated
    col_mean = ['agriculture', 'dechets', 'energie', 'industrie_hors-energie', 'residentiel', 'routier']    #Columns where missing values will be replaced by the mean    
    emissions_df = gestion_NaN(emissions_df,col_interpolate,col_mean)
    print(emissions_df.isna().sum())    #We check that the missing values have been replaced

    #A remark about the work, since there is a large number of missing values in the columns linked to the transports branch
    #I chose to interpolate these missing values. When I check my code I believe this was not the correct choice for
    #all the town. Indeed the meaning of the international_transports columns is linked to way of transports like 
    #plane, train, boat ect... However not all cities are concerned by such way of transports since not every city 
    #have an aeroport for example. So if there is no data about international transports for one city it is maybe because
    #there is not any. Therefore I should have replaced the missing values with a zero. 

    #I find here that the interpolate function did not work for all missing values. To find why, I asked chatGPT 
    #and the reason can be the large amount of missing values in the transports related columns. Therefore, since 
    #there is not much msisng values (2 and 14 over 35 798 values) I will replace these missing values with the mean
    #of the columns. I implement the correction directly here without an auxiliar function since it happens for only
    #2 columns.

    emissions_df['transports_filled'] = emissions_df['transports_filled'].fillna(emissions_df['transports_filled'].mean())
    emissions_df['transports_international_filled'] = emissions_df['transports_international_filled'].fillna(emissions_df['transports_international_filled'].mean())

    
    #Then we can print the statistics related to each columns :
    colonnes_décrites = ['agriculture_filled', 'transports_filled', 'transports_international_filled', 'biomasse_hors-total_co2', 'dechets_filled', 'energie_filled', 'industrie_hors-energie_filled', 'residentiel_filled', 'routier_filled', 'tertiaire']
    statistics(emissions_df,colonnes_décrites)

    #Since there is many columns I use an auxiliar function to print each statistics with a loop to have a better lisibility
    #The column 'commune' is not included since it does not contain numeric values.

    print(' ')
    print(' ')
    print('Partie 3 :')
    print(' ')

    communes_df = local_pandas('donnees_communes.csv', ';')
    print(communes_df.shape)    #34970 lines
    print(emissions_df.shape)   #35798 lines

    #I first checked if the 2 dataframes have the same amount of data. This is not the case, communes_df have less values
    #than emissions_df, so when I will merge the 2 dataframe together it will cause the loss of some data. Even the use of 
    #the key word OUTER when merging the two dataframe will create missing values on the cities related. If I choose
    #this method I would need to add the missing inforlations about each insee codes added to the merged dataframe.
    #I would need to find the name of the city, its region, its department and all the code related. I don't see a way
    #to do this automaticly so I chose the key word INNER when merging the two dataframe knowing that I lose data doing so. 

    emissions_communes_df = communes_df.copy() 

    #I chose to create the emissions_communes_df by deep copying the communes_df, since they have many columns in common
    #Then I drop the useless columns from emissions_communes_df, and I rename the remaining ones according to the 
    #guide. I also need to change the index, initially it is the region's code and I need it to be the insee code (with the same name as in emissions_df) to
    #make the merge.   


    emissions_communes_df = emissions_communes_df.reset_index()
    emissions_communes_df = emissions_communes_df.set_index('COM')
    emissions_communes_df.index.names = ['commune_code_insee']
    emissions_communes_df = emissions_communes_df.drop(columns=['CODARR', 'CODCAN', 'CODCOM', 'PMUN', 'PCAP'])
    colonnes = ['region_code','region_name','departement_code', 'commune' ,'population_totale']
    emissions_communes_df.columns = colonnes
    print(emissions_communes_df.head())
    print(emissions_df.head())
    
    #Now I add the data from emissions_df 

    colums_to_merge = ['agriculture_filled', 'transports_filled', 'transports_international_filled', 'biomasse_hors-total_co2','dechets_filled', 'energie_filled', 'industrie_hors-energie_filled', 'residentiel_filled', 'routier_filled', 'tertiaire']
    emissions_communes_df = ajout_data(emissions_communes_df, emissions_df, colums_to_merge, 'commune_code_insee')
    print(emissions_communes_df.head())
    print(emissions_communes_df.shape)  #34857 lines.

    #The database have 34970 lines, so we lost data again with the merge. It is due to the use of the key word INNER
    #that merges data when both insee code are present in both dataframe. Since both dataframe don't have all insee code
    #in common, the individual insee code are lost during the merging step. Adding missing values on emissions linked to 
    #cities present only in communes_df could have been possible with a second treatment of missing values but I chose 
    #to focus on 3rd part first.
    
    #emissions_communes_df.to_csv('C:/Users/arthu/Documents/Centrale/important/stage 2A/entretien/Scor/Test/emissions_communes_pandas.csv', index = True)

    #Finally I save the dataframe in a .csv file to reopen it easily in 3rd part.