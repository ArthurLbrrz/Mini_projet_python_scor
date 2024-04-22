#The structure of the file python are divided in two parts. In the first part I defined the auxiliar function
#I will be using to treat the topic. In the second part I apply these function with detailed explanation on the choices
#I've made.

#Partie 2 : Importing data by using an URL

#First I import the needed modules
import pandas as pd
import polars as pl 

def url_pandas(lien,separateur) :
    emissions_df =  pd.read_csv(lien, sep = separateur, index_col=0)
    #The function read_csv from pandas is also capable of reading URLs
    return emissions_df

def url_polars(lien,separateur,types) :
    emissions_df = pl.read_csv(lien, separator= separateur, dtypes= types)  
    #Just like pandas, polars.read_csv can also read URLs. It is still needed to specify the dtypes wished for ambiguous columns.
    return emissions_df 


if __name__ == "__main__" :
    emissions_df = url_pandas('https://koumoul.com/s/data-fair/api/v1/datasets/igt-pouvoir-de-rechauffement-global/convert', ',')
    print(emissions_df.head(20))
    print(emissions_df.shape)  #I check that both methods are equivalent

    emissions_df2 = url_polars('https://koumoul.com/s/data-fair/api/v1/datasets/igt-pouvoir-de-rechauffement-global/convert', ',', {'INSEE commune' : pl.Utf8})
    print(emissions_df2.head(20))
    print(emissions_df2.shape)

    #The two method are equivalent and moreover this is the same results as in the first method when the file was stored locally.
