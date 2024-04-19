#La structure de mes codes se décompose en 2 parties, une première partie ou je définis l'ensemble des 
#fonctions qui vont me servir, et une deuxième partie où j'applique ces fonctions 

import pandas as pd
from Data_import_local import local_pandas

#Partie 1

def analyse_colonnes(dataframe, new_colonnes, index_name) :
    colonnes = dataframe.columns             #Je stocke les colonnes pour un éventuel usage futur
    print('Les colonnes du dataframe emissions_df sont :')     #On affiche les colonnes avec une boucle pour davantage de lisibilité
    for c in colonnes :
        print(c)
    print(' ')
    dataframe.columns = new_colonnes
    dataframe.index.names = [index_name]
    return dataframe

#Partie 2

"""La fonction gestion_NaN va permettre de traiter les valeurs manquantes du dataframe passé en paramètre.
On spécifie également quelles colonnes vont être traitées avec la méthode interpolate dans une liste colonnes_interpolate
et les colonnes qu'on va traiter avec la moyenne dans une liste colonnes_mean, on renvoie ensuite le dataframe actualisé"""

def gestion_NaN(dataframe,colonnes_interpolate, colonnes_mean) :
    for c in colonnes_interpolate :
        dataframe[f'{c}_filled'] = dataframe[f'{c}'].copy()     #Il faut commencer par ajouter au dataframe une deep copy de chaque colonne à corriger
        dataframe[f'{c}_filled'] = dataframe[f'{c}_filled'].interpolate('linear')   #On modifie ensuite la nouvelle colonne crée
    for c in colonnes_mean : 
        dataframe[f'{c}_filled'] = dataframe[f'{c}'].copy()
        mean_c = dataframe[f'{c}'].mean()
        dataframe[f'{c}_filled'] = dataframe[f'{c}_filled'].fillna(mean_c)
    return dataframe

"""La fonction statistiques affiche les statistiques relatives aux colonnes passées en paramètre dans la liste colonnes"""

def statistics(dataframe, colonnes) :
    for c in colonnes :
        print(f'Les statistiques de la colonne {c} sont :')
        print(' ')
        print(dataframe[c].describe())
        print(' ')

#Partie 3 :

"""La fonction ajout_data permet de merge les colonnes d'un dataframe sur un autre en se basant ur une colonne présente dans les deux 
dataframe (on l'appelle cle_commune et ce sera le code insee)"""

def ajout_data(dataframe1, dataframe2, colonnes, cle_commune) :
    for c in colonnes :
        dataframe1 = dataframe1.merge(dataframe2[c], on = cle_commune)
    return dataframe1
        

if __name__ == '__main__' :
    file = 'IGT - Pouvoir de réchauffement global.csv'

    #On crée notre dataframe par la méthode local présentée dans la partie Data import
    emissions_df = local_pandas(file, ',')
    print(emissions_df.shape)

    print(' ')
    print('Partie 1 :')
    print('')
    #On renomme toute les colonnes en minuscule et on ajoute des '_' pour chaque espace, 
    #je le fais en une seule étape avec la fonction analyse_colonnes:

    new_colums = ['commune','agriculture','transports', 'transports_international', 'biomasse_hors-total_co2', 'dechets', 'energie', 'industrie_hors-energie', 'residentiel', 'routier', 'tertiaire']
    nom = 'commune_code_insee'
    analyse_colonnes(emissions_df, new_colums, nom)

    print(emissions_df.head(10))     #On vérifie ici que le dataframe est correct
    
    print(' ')
    print(' ')
    #Partie 2 :
    print('Partie 2 :')

    #On utilise la fonction describe pour afficher les statistiques du dataframe

    print(emissions_df.describe())

    #On sait qu'il manque des valeurs dans le dataframe lorsu'on affiche les 10 premières lignes du tableau. 
    #On va donc regarder pour chaque colonne le nombre de valeurs manquantes et créer une nouvelle colonne pour chaque colonne
    #où il existe une valeur manquante. 

    print((emissions_df.isna().sum()/emissions_df.shape[0])*100)

    #Ici j'affiche le pourcentage de valeurs manquantes par rapport aux nombres d'entrées du dataframe
    
    #Les colonnes suivantes présentent des valeurs manquantes :
    #agriculture (0.17%); transports (72%); transports_international (92%); déchets (0.016%) ; energie (3.65%) ; 
    #industrie_hors-energie (3.65%) ; residentiel (0.016%) ; routier (0.056%) 

    #On observe que ce sont surtout les colonnes relatives au trnsport qui présentent le plus de valeurs manquantes
    #Ma première idée était de remplacer ces valeurs manquantes par la moyenne de la colonne cepednant au vu du nombre 
    #de valeurs manquante on ne peut pas considérer la moyenne comme étant un bon moyen de remplacer ces valeurs 
    #puisque elle sera calculer qu'à partir d'un nombre insuffisant de données pour les colonnes transports
    #(18% des données pour la colonne transports et 8% pour la colonne transports_international). 
    #J'ai donc du chercher sur internet s'il existait des fonctions permettant d'approximer les valeurs d'une colonne.
    #La fonction interpolate() de pandas permet exactement cela. On va donc faire une interpolation polynomiale 
    #des valeurs manquantes des colonnes relatives au transports. 
    #Au vu du faible nombre de données manquantes pour les autres colonnes, on peut les remplacer par la 
    #moyenne de la colonne concernée car cette dernière est calculée à partir d'un nombre suffisant de valeurs.

    #Je rajoute le suffixe '_filled' aux colonnes complétées ajoutées au dataframe à l'aide de la fonction gestion_NaN:

    col_interpolate = ['transports', 'transports_international']    #Les colonnes où les valeurs manquantes vont être interpolées
    col_mean = ['agriculture', 'dechets', 'energie', 'industrie_hors-energie', 'residentiel', 'routier']    #Les colonnes où les valeurs manquantes vont être remplacées par la moyenne de la colonne
    
    emissions_df = gestion_NaN(emissions_df,col_interpolate,col_mean)
    print(emissions_df.isna().sum())    #On vérifie si toutes les valeurs manquantes ont été corrigées

    #Une remarque en relisant mon travail : au vu du grands nombres de valeurs manquantes dans les colonnes relatives au transports j'ai choisi
    #d'interpoler toutes les valeurs, ce qui n'était peut être pas correct pour la totalité des communes. En effet
    #si on s'interesse au sens de la colonne transports_international cela se refère certainement à tout les longs
    #trajets de marchandises, que ce soit par train/camion ou avion, ou aussi les voyages à l'étranger. Hors toutes
    #les villes de France ne sont pas nécessairement concernées par ce genre d'activité et il aurait donc peut être été 
    #préférable de combler les valeurs manquantes par un zéro car, si à priori il n'y a pas de données c'est peut être car il n'y en existe pas.
    #Cependant par manque de temps la correction n'a pas pu être implémentée.

    #Je remarque ici que la fonction interpolate n'a pas fonctionné pour toutes les valeurs. Après avoir demandé à
    #ChatGPT la raison, il est possible que ce soit causé par le grand nombre de valeurs manquantes des colonnes relatives au transports
    #Pour corriger cela je vais remplacer ces valeurs par la nouvelle moyenne corrigée, car cela se produit pour un nombre
    #négligeable de valeurs (2 et 14 sur 35 798). Et comme cela se produit que pour 2 colonnes 
    #je le corrige directement ici sans passer par une fonction auxiliaire.

    emissions_df['transports_filled'] = emissions_df['transports_filled'].fillna(emissions_df['transports_filled'].mean())
    emissions_df['transports_international_filled'] = emissions_df['transports_international_filled'].fillna(emissions_df['transports_international_filled'].mean())

    
    #On peut ensuite affficher les statistiques relatives à chaque colonne :
    colonnes_décrites = ['agriculture_filled', 'transports_filled', 'transports_international_filled', 'biomasse_hors-total_co2', 'dechets_filled', 'energie_filled', 'industrie_hors-energie_filled', 'residentiel_filled', 'routier_filled', 'tertiaire']
    statistics(emissions_df,colonnes_décrites)

    #Je passe par une fonction auxiliaire, car on a un grand nombre de colonne ce qui nous empêche de visualiser la 
    #totalité des stistiques dans la console de python, donc j'ai choisi d'afficher à la suite les statistiques de chaque
    #colonne les unes après les autres pour plus de lisibilité. On n'inclue pas la colonne 'commune' car elle ne 
    #présente pas de valeurs numériques.

    print(' ')
    print(' ')
    print('Partie 3 :')
    print(' ')

    communes_df = local_pandas('donnees_communes.csv', ';')
    print(communes_df.shape)    #34970 lignes
    print(emissions_df.shape)   #35798 lignes

    #En voulant vérifier que les dataframes avaient le même nombre d'entrées je me rends compte que la base de données communes
    #contient moins de valeurs que la base de données relatives aux emissions. Cela pose problème dans la création de notre
    #nouveau dataframe car si l'on veut toutes les données d'émission il faudrait faire un merge OUTER ce qui entraînerait 
    #la création de valeurs manquantes. Cela impliquerait donc de devoir rajouter toutes les communes absentes de la base
    #de données communes (région, code insee, departement, nom de la commune) mais présente dans la base de données emissions_df.
    #Ici j'ai fait le choix de ne rien rajouter aux bases de données et de faire un merge INNER, c'est à dire de
    #ne garder les communes présentes dans la base de données communes.Il faut donc garder en tête pour l'analyse des données
    #que on a retirer des valeurs.
    

    emissions_communes_df = communes_df.copy() 


    #Comme la plupart des infos souhaitées dans le dataframe emissions_communes_df se trouvent dans le dataframe 
    #communes_df j'ai choisi de faire une deep copy du dataframe communes_df et de ne faire que les merges à partir
    #de emissions_df. On commence par retirer les colonnes inutiles et on renomme les colonnes comme demandé.
    #On change également l'index, initialement le code de la région, on utilise plutôt le code INSEE comme dans le dataframe
    #emissions_df, et on lui donne le même nom (commune_code_insee) pour réaliser le merge.
    emissions_communes_df = emissions_communes_df.reset_index()
    emissions_communes_df = emissions_communes_df.set_index('COM')
    emissions_communes_df.index.names = ['commune_code_insee']
    emissions_communes_df = emissions_communes_df.drop(columns=['CODARR', 'CODCAN', 'CODCOM', 'PMUN', 'PCAP'])
    colonnes = ['region_code','region_name','departement_code', 'commune' ,'population_totale']
    emissions_communes_df.columns = colonnes
    print(emissions_communes_df.head())
    print(emissions_df.head())
    
    #Maintenant on ajoute les informations du dataframe emissions_df

    colums_to_merge = ['agriculture_filled', 'transports_filled', 'transports_international_filled', 'biomasse_hors-total_co2','dechets_filled', 'energie_filled', 'industrie_hors-energie_filled', 'residentiel_filled', 'routier_filled', 'tertiaire']
    emissions_communes_df = ajout_data(emissions_communes_df, emissions_df, colums_to_merge, 'commune_code_insee')
    print(emissions_communes_df.head())
    print(emissions_communes_df.shape)  #34857 lignes.

    #La base de données communes_df avait 34970 lignes, on a donc encore perdu d'autres données lors du merge. C'est dû 
    #au fait que des codes insee doivent être présent dans un dataframe mais pas dans l'autre. On aurait pu rajouter toutes les 
    #colonnes avec le merge OUTER mais cela aurait demandé un second traitement des valeurs manquantes. 
    # J'ai préféré garder ce dataframe tel quel afin de traiter la partie 3 en priorité. 
    
    emissions_communes_df.to_csv('C:/Users/arthu/Documents/Centrale/important/stage 2A/entretien/Scor/Test/emissions_communes_pandas.csv', index = True)

    #Enfin on enregistre notre dataframe dans un csv pour pouvoir l'utiliser dans la partie 3. 
    #Je masque cette ligne lors de l'envoi du projet pour ne pas créer d'erreurs. 