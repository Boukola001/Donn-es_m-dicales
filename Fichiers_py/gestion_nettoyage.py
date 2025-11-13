#########################################################################
###### Noms Prénoms: BIAYE Bernadette - DIAGNE Ndeye Fatou - GBAYE Boukola - OTCHOFFA Karline
######
######
#########################################################################

import pandas as pd
import numpy as np


### Fonctions utilitaires

def cor_type(df_sante) : 
    """
    Cette fonction charge un DataFrame, corrige le type des variables.

    Paramètres :
    - df_sante (pd.DataFrame): Le dataframe à préparer.

    Renvoie :
    - pd.DataFrame : Le DataFrame avec les bons types de variables.
    """
    
    # Types par défaut
    print("Types des variables par défaut: \n", df_sante.dtypes)
    
    # Conversion des types
    
    # Date of Admission et Discharge Date doivent être de type datetime
    df_sante["Date of Admission"] = pd.to_datetime(df_sante["Date of Admission"])
    df_sante["Discharge Date"] = pd.to_datetime(df_sante["Discharge Date"])
    
    # Billing Amount doit être de type float64
    # On commence par se débarasser des €
    df_sante["Billing Amount"] = df_sante["Billing Amount"].str.replace("€","")
    # On convertit
    df_sante["Billing Amount"] = pd.to_numeric( df_sante["Billing Amount"])
    
    # Le numéro de dhambre n'est pas censé être agrégé (à voir)
    #df_sante["Room Number"] = df_sante["Room Number"].astype("object")
    
    print("Types des variables après traitement : \n", df_sante.dtypes)
    
    return df_sante


def gestion_nan(df_sante) : 
    """
    Cette fonction charge un DataFrame à préparer, retrouve les valeurs manquantes
    et leur impute une valeur.

    Paramètres :
    - df_sante (pd.DataFrame): Le dataframe à préparer.

    Renvoie :
    - pd.DataFrame : Le DataFrame sans valeurs manquantes.
    """
    # Afficher le nombre de valeurs manquantes par colonnes
    print("Nombre de valeurs manquantes par colonne : \n", df_sante.isna().sum())
    # Les colonnes problématiques sont Medication et Insurance Provider

    # Traitement de Insurance Provider :  On cherche d'abord l'élément le plus fréquent qu'on va imputer aux NAN
    
    max_IP = df_sante["Insurance Provider"].mode()[0]
    df_sante["Insurance Provider"] = df_sante["Insurance Provider"].fillna(max_IP)
    #df_sante["Insurance Provider"].value_counts()
    
    
    # Traitement de Medication :  On cherche d'abord l'élément le moins fréquent qu'on va imputer aux NAN
    df_sante["Medication"].value_counts()
    # Dictionnaire des Medications avec leurs nombres d'occurences
    dico_med = df_sante["Medication"].value_counts().to_dict()
    print(dico_med)
    # Récupération du médicament le moins fréquent 
    min_med = min(dico_med, key = lambda x : dico_med[x])
    print(min_med)
    df_sante["Medication"] = df_sante["Medication"].fillna(min_med)
    #df_sante["Medication"].value_counts()
    
    print("Nombre de valeurs manquantes par colonne après traitement : \n", df_sante.isna().sum())
    
    return df_sante


def gestion_duplicatas(df_sante) :
    
    """
    Cette fonction charge un DataFrame à préparer, recherche les duplications 
    et les corrige.

    Paramètres :
    - df_sante (pd.DataFrame): Le dataframe à préparer.

    Renvoie :
    - pd.DataFrame : Le DataFrame sans duplications de lignes.
    """
    
    # Trouver les duplications
    nb_dup = df_sante.duplicated().sum()
    print(f"""Il y a en tout {nb_dup} lignes dupliquées.
          Le taux de duplication est de {nb_dup*100/len(df_sante)}""")
    #df_sante[df_sante.duplicated()]

    # Suppression des doublons en gardant la première occurence
    df_sante.drop_duplicates(inplace=True)
    
    print(f"Après traitement, il y a en tout {df_sante.duplicated().sum()} lignes dupliquées.")
    
    return df_sante

#Fonction pour nettoyer les préfixes et suffixes
def clean_pref_suf(val, liste_pref, liste_suf):
    """
    Cette fonction permet de retirer les préfixes et suffixes 
    d'une chaine de caractères.

    Paramètres :
    - val (str): la chaine de caractères à nettoyer.
    - liste_pref (list[str]) : les préfixes possibles à retirer.
    - liste_suf (list[str]) : les suffixes possibles à retirer.

    Renvoie :
    - str : la cahine sans préfixes et suffixes gênants.
    """
    for elt in liste_pref:
        if val.startswith(elt):
            val = val.removeprefix(elt)
    for elt in liste_suf : 
        if val.endswith(elt):
            val = val.removesuffix(elt)
    return val

    


def gestion_inco_typos(df_sante) :
    
    """
    Cette fonction charge un DataFrame à préparer 
    et corrige les incohérences dans les colonnes textuelles.

    Paramètres :
    - df_sante (pd.DataFrame): Le dataframe à préparer.

    Renvoie :
    - pd.DataFrame : Le DataFrame avec des colonnes textuelles propres
    et cohérentes.
    """
    
    # Gestion de Medical Condition
    # Colonne médical Condition
    # Suppression des espaces inutiles
    df_sante["Medical Condition"] = df_sante["Medical Condition"].str.strip()
    # Uniformisation
    df_sante["Medical Condition"] = df_sante["Medical Condition"].str.capitalize()
    df_sante["Medical Condition"].value_counts()
    # Gestion des incohérences
    diabetes = ["Diabète", "Diabete", "Diabetes"]
    cancer = ["Canser", "Cancer"]
    df_sante["Medical Condition"] = np.where(df_sante["Medical Condition"].isin(diabetes), "Diabetes", df_sante["Medical Condition"])
    df_sante["Medical Condition"] = np.where(df_sante["Medical Condition"].isin(cancer), "Cancer", df_sante["Medical Condition"])

    
    # Gestion de la colonne Hospital
    df_sante["Hospital"] = df_sante["Hospital"].str.strip()
    
    # Préfixes observés
    liste_pref_hosp = [",", "and, ", ", and ", "and "]
    # Suffixes observés
    liste_suf_hosp = [",", " and,", ", and"]
    
    # Suppression des préfixes et suffixes gênants
    df_sante["Hospital"] = df_sante["Hospital"].apply(lambda x : clean_pref_suf(x, liste_pref_hosp, liste_suf_hosp))
    # S'assurer qu'il n'y a plus d'espace en début ou fin de chaînes
    df_sante["Hospital"] = df_sante["Hospital"].str.strip()
    
    
    # Gestion des colonnes Doctor ou Name : title() pour l'uniformisation
    df_sante["Name"] = df_sante["Name"].str.title()
    df_sante["Doctor"] = df_sante["Doctor"].str.title()
    
    # Se débarasser des potentiels espaces indésirables
    df_sante["Name"] = df_sante["Name"].str.strip()
    df_sante["Doctor"] = df_sante["Doctor"].str.strip()
    
    # Préfixes observés
    liste_pref_pers = ["Mr.", "Mrs.", "Miss", "Ms.", "Mx.", "Dr.", "Prof.", "Phd", "Md", "Do", "Dds", "Dvm", "Dd"]
    # Suffixes observés
    liste_suf_pers = ["Phd", "Md", "Do", "Dds", "Dvm", "Edd", "Psyd", "Dphil", "Scd", "Dpharm", "Dpm", "Jr.", 
                      "I","Ii","Iii", "Iv", "V", "Lld", "Thd", "Dth", "Dba", "Engd", "Dpa"]
    # Suppression des préfixes et suffixes gênants
    df_sante["Doctor"] = df_sante["Doctor"].apply(lambda x : clean_pref_suf(x, liste_pref_pers, liste_suf_pers))
    df_sante["Name"] = df_sante["Name"].apply(lambda x : clean_pref_suf(x, liste_pref_pers, liste_suf_pers))
    # Effacer les . ou espaces résiduels
    df_sante["Name"] = df_sante["Name"].str.strip('. ')
    df_sante["Doctor"] = df_sante["Doctor"].str.strip('. ')
    
    return df_sante


def calculeur_interv(df_sante, col) :
    """
    Cette fonction charge un DataFrame à préparer et le nom d'une colonne
    numérique. Elle trouve les valeurs atypiques de col et les supprime.

    Paramètres :
    - df_sante (pd.DataFrame): Le dataframe à préparer.

    Renvoie :
    - pd.DataFrame : Le DataFrame sans valeurs aberrantes dans la colonne col.
    """
    #Calculons les quartiles de col
    Q1 = df_sante[col].quantile(0.25)
    Q3 = df_sante[col].quantile(0.75)
    IQR = Q3 - Q1
    low = Q1 - 1.5*IQR
    up = Q3 + 1.5*IQR
    
    mask = (df_sante[col] < low) | (df_sante[col] > up)
    nb_ab = df_sante[col][mask].sum()
    if nb_ab != 0 : 
        print(f"Il y a en tout, {nb_ab} valeurs aberrantes dans la colonne {col}. Il faut donc s'en séparer")
        #Gérer le filtrage
    else :
        print(f"Il n'y a pas de valeurs aberrantes dans la colonne {col}.")
    return df_sante

def gestion_val_ab(df_sante) :
    
    """
    Cette fonction charge un DataFrame à préparer, 
    nettoie les colonnes numériques.

    Paramètres :
    - df_sante (pd.DataFrame): Le dataframe à préparer.

    Renvoie :
    - pd.DataFrame : Le DataFrame sans valeurs aberrantes.
    """
    print("Types de variables : \n", df_sante.dtypes)
    print("Les valeurs aberrantes sont déterminées pour les colonnes numériques. Il s'agira ici de Age et Billing Amount")
    
    #Âge :
    df_sante = calculeur_interv(df_sante, "Age")
    #Billing Amount
    # Remplacer les Billing Amount <0 par leur valeur absolue
    df_sante["Billing Amount"] = np.where(df_sante["Billing Amount"]<0,-df_sante["Billing Amount"],df_sante["Billing Amount"])
    df_sante = calculeur_interv(df_sante, "Billing Amount")
    return df_sante
    

def prep_mcd(df_sante) : 
    """
    Cette fonction charge un DataFrame à préparer, 
    divise les colonnes de patients et docteurs de sorte à avoir
    nom et prénom séparés dans différentes colonnes.

    Paramètres :
    - df_sante (pd.DataFrame): Le dataframe à préparer.

    Renvoie :
    - pd.DataFrame : Le DataFrame avec les nouvelles colonnes.
    """
    df_sante[["Patient_surname", "Patient_name"]] = df_sante["Name"].str.split(" ", expand=True)
    df_sante[["Doctor_surname", "Doctor_name"]] = df_sante["Doctor"].str.split(" ", expand=True)
    return df_sante


def nettoyer_preparer_donnees(nom_fichier):
    """
    Cette fonction charge un fichier CSV, effectue un nettoyage de données complet,
    et prépare les données pour une analyse ultérieure.

    Étapes de traitement :
    1. Conversion des types de données où nécessaire
    2. Gestion des données manquantes
    3. Détection et suppression des duplications dans le DataFrame.
    4. Normalisation des données pour corriger les incohérences et les fautes de frappe
    5. Vérification et traitement des valeurs aberrantes.
    6. Préparation des données pour la conception de Modèle Conceptuel de Données (MCD) et Modèle Logique de Données (MLD), y compris la gestion des cardinalités entre produits, marques et catégories.

    Paramètres :
    - nom_fichier (str): Le chemin vers le fichier CSV à charger.

    Renvoie :
    - pd.DataFrame : Le DataFrame nettoyé et préparé.
    """
    df_sante = None
    try:
        df_sante = pd.read_csv(nom_fichier)
            ###########################
        #étape 1: Type de donnees #
        ###########################
        # à compléter
        df_sante = cor_type(df_sante)
        print("Les types ont été bien corrigés.")

        ##############################
        #étape 2: Données manquantes #
        ##############################
        # à compléter
        df_sante = gestion_nan(df_sante)
        print("Les données manquantes ont été bien remplacées.")

        ##############################
        #étape 3: Trouver les duplications
        ###############################
        # à compléter
        #df_sante = gestion_duplicatas(df_sante)
        #print("Les duplicatas ont été bien supprimés.")

        ##############################
        #étape 4: Incohérences et Typos
        ###############################
        # à compléter
        df_sante = gestion_inco_typos(df_sante)
        print("Les incohérences de typos ont été bien corrigées.")

        ##############################
        #étape 5: les valeurs aberrantes
        ###############################
        # à compléter
        df_sante = gestion_val_ab(df_sante)
        print("Les valeus aberrantes ont été bien écartées.")
        
        # S'assurer qu'il n'y a pas de duplications
        df_sante = gestion_duplicatas(df_sante)
        
        # Préparation mcd
        df_sante = prep_mcd(df_sante)
        print("Les données sont prêtes pour le mcd")
    except FileNotFoundError as ffe:
        print(f"Erreur!!Fichier non existant ({ffe})")
    except Exception as e:
        print("Erreur inconnue :", e)

    return df_sante

if __name__ == "__main__":
    nom_fichier = "data/jeu_donnees_sante.csv"
    df = nettoyer_preparer_donnees(nom_fichier)
    print(df.columns)
    print(len(df))
    print(df["Medical Condition"].unique().tolist())
