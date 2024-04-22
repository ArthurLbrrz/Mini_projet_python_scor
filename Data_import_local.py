#The structure of the file python are divided in two parts. In the first part I defined the auxiliar function
#I will be using to treat the topic. In the second part I apply these function with detailed explanation on the choices
#I've made.


#Partie 1 : 

#First I import the needed modules
import pandas as pd
import polars as pl

def local_pandas(file, separateur) :   
    #Since the separators can changes between files, I specify the parameter 'sep'. The separator can be found 
    #quickly just by opening the .csv file in VScode or in excel or in bloc-note. 
    emissions_df = pd.read_csv(file, sep = separateur, index_col=0)
    return emissions_df


#Since I do not know polars, the vast majority of informations find about usual functions have been found on the site :
#https://docs.pola.rs/py-polars/html/reference/api/. 
#With the help of chatGPT to correct errors when necessary.


def local_polars(file, separateur,types) :
    emissions_df = pl.read_csv(file, separator=separateur, dtypes= types) 
    return emissions_df

    

if __name__ == "__main__" :   

    emissions_df = local_pandas('IGT - Pouvoir de réchauffement global.csv', ',')    
    #If the index coloumns is not precised, pandas can create by himself a column to use as its index when here it
    #is preferable to use the INSEE code as index.  

    print(emissions_df.head(20))    #head(x) give a look of the first x lines of the dataframe 
    print(emissions_df.shape)       #I check that the two methods give the same result

    emissions_df = local_polars('IGT - Pouvoir de réchauffement global.csv', ',', {'INSEE commune' : pl.Utf8})
    print(emissions_df.head(20))    
    print(emissions_df.shape)    

    #polars have functions in common with pandas, so it made its comprehension easier for me 

    #I found that it was necessary to precise the type of the INSEE commune column. If not precised an error raises
    #when creating the dataframe on the entry like 2A001 for example. 
    #It is possible to ignore the errors when creating the dataframe, this return the same dataframe creating with pandas.
    #But, in my opinion it is better to fix the types of the columns when there is mixed values (for example str and int)
    #The parameter dtype can be use to do that. I had to find how polars coded the type 'str' and chatGPT gave me pl.Utf8.
    #This step was not necessary with pandas because pandas attributes directly the type Object to the index.
