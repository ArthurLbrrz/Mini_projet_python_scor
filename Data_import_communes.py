#The structure of the file python are divided in two parts. In the first part I defined the auxiliar function
#I will be using to treat the topic. In the second part I apply these function with detailed explanation on the choices
#I've made.

#Partie 3 : 

#I first import the needed modules. I also import the function local, coded in the local part to use it in here

import pandas as pd
import polars as pl
from Data_import_local import local_pandas
from Data_import_local import local_polars



if __name__ == '__main__' :
    file = 'donnees_communes.csv'

    #Avec Pandas : 

    communes_df = local_pandas(file,';')    

    #I reuse the local function here. I could also have open the file with the read_csv function since local is a very 
    #simple function. 
    #After opening the file I find that the separator is different than the previous one.
       
    print(communes_df.head(20))    
    print(f'La taille du dataframe pandas est de : {communes_df.shape}')

    #print(communes_df.dtypes)
    #For polars we specify a dictionary for the types to use. 
    types = {'Région' : pl.Utf8, 'DEP' : pl.Utf8, 'CODCAN' : pl.Utf8, 'COM' : pl.Utf8} 

    
    #With Polars

    communes_df = local_polars(file,';', types)
    print(communes_df.head(20))
    print(f'La taille du dataframe polars est de : {communes_df.shape}')

#We had two dataframe with the same dimensions : (34970,10)
#and we add the index column index to the polars version : (34970,11)