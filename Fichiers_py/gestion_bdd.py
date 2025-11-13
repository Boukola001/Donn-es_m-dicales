#########################################################################
###### Noms Prénoms: BIAYE Bernadette - DIAGNE Ndeye Fatou - GBAYE Boukola - OTCHOFFA Karline
######
#########################################################################
#########################################################################
###### MODULE de gestion de bdd: créer/fermer connexion,
###### Créer la bdd et les tables, insérer les données, explorer les données
#########################################################################
import mysql.connector as mysqlcon
import Fichiers_py.gestion_nettoyage as gn

def creer_connexion(nom_host, nom_user, mdp_user, nom_bdd=""):
    """
    Crée une connexion au serveur MySQL.

    Args:
        nom_host (str): Le nom de l'hôte où le serveur MySQL est situé.
        nom_user (str): Le nom de l'utilisateur pour se connecter au serveur.
        mdp_user (str): Le mot de passe de l'utilisateur pour se connecter au serveur.
        nom_bdd (str): Le nom de la base de données à utiliser (optionnel).

    Returns:
        MySQLConnection: Une instance de connexion au serveur MySQL.
    Raises:
        mysql.connector.Error: Une erreur survient de la part de MySQL pour
                              des raisons qui peuvent inclure des problèmes
                              de connexion réseau ou des pbs liés à la BDD.

    """
    connexion = None
    try:
        connexion = mysqlcon.connect(
            host=nom_host,
            user=nom_user,
            password=mdp_user,
            database=nom_bdd
        )
        print("Connexion réussie !")
        
    except mysqlcon.Error as err:
        print(f"Erreur de connexion : {err}")
    return connexion

def fermer_connexion(connexion):
    """
    Ferme une connexion MySQL si elle est active.

    Args :
    connexion (mysql.connector.connection.MySQLConnection or None): Connexion
    active à une base de données MySQL. Si elle est déjà fermée ou None,
    la fonction ne fait rien.

    """
    if connexion :
        connexion.close()
        print("Connexion fermée")
    

def selectionner_bdd(connexion, nom_bdd):
    """
    Sélectionne la base de données spécifiée pour la connexion donnée.

    Args:
        connexion (MySQLConnection or None): Connexion
        nom_bdd (str): Nom de la base de données à sélectionner.
    """
    curseur = None
    req = f"""
    use {nom_bdd};
    
    """
    req1 = """
    select database();
    """
    
    try:
        curseur = connexion.cursor()
        curseur.execute(req)
        print("Base de données sélectionnée")
        curseur = connexion.cursor()
        curseur.execute(req1)
        print(f"Base de données sélectionnée: {curseur.fetchone()}")
        
    except mysqlcon.Error as e:
        print("Erreur lors de la sélection de BDD:", e)
    finally:
        
        if curseur:
            curseur.close()
            
def creer_bdd(connexion, nom_bdd):
    """
    Crée une nouvelle base de données MySQL si elle n'existe pas déjà
    et la sélectionne pour être utilisée.

    Args:
        connexion (MySQLConnection): La connexion au serveur MySQL où la BDD sera créée.
        nom_bdd (str): Le nom de la BDD à créer et sélectionner.

    Raises:
        mysql.connector.Error: Une erreur survient liée à MySQL pour des raisons
        qui peuvent inclure des problèmes de permissions, erreurs de syntaxe
        dans le nom de BDD, ou problèmes de connexion réseau.
    """
    curseur = None
    req = f"""
    create database if not exists {nom_bdd};
    """
    
    try:
        curseur = connexion.cursor()
        curseur.execute(req)
        print("Base de données créée")
        
    except mysqlcon.Error as e:
        print("Erreur lors de la création de la BDD:", e)
    finally:
        if curseur:
            curseur.close()

def tables_deja_creees(connexion, liste_tables):
    """
    FONCTION COMPLETE
    Vérifie si toutes les tables sont déjà présentes dans la base de données.
    Vous pouvez l'utiliser pour éviter de recréer les tables si elles sont déjà
    créées.

    Args:
        connexion (MySQLConnection): Connexion active à un serveur MySQL.
        liste_tables (list[str]): Liste des noms de tables attendues.

    Returns:
        bool: True si toutes les tables sont présentes, False sinon.
        En cas d’erreur, un message est affiché mais l'exception n’est pas propagée.
    """
    curseur = None
    try:
        curseur = connexion.cursor()
        curseur.execute("SHOW TABLES")
        tables_existantes = {ligne[0] for ligne in curseur.fetchall()}
        return all(table in tables_existantes for table in liste_tables)
    except Exception as e:
        print(f"Erreur lors de la vérification des tables : {e}")
        return False
    finally:
        if curseur:
            curseur.close()

def bdd_existe(connexion, nom_bdd):
    """
    FONCTION COMPLETE
    Vérifie si une base de données existe sur le serveur MySQL.
    Utile lors de la création de la base de données : cela permet d’éviter
    de la recréer si elle existe déjà, grâce à la vérification effectuée par
    bdd_existe.

    Args :
    connexion (MySQLConnection): Connexion active à un serveur MySQL.
    nom_bdd (str): Nom de la base de données dont on veut vérifier l'existence.

    Renvoie :
    bool: True si la base de données existe, False sinon ou en cas d'erreur.
    En cas d’erreur, un message est affiché mais l'exception n’est pas propagée.
    """

    curseur = None
    try:
        curseur = connexion.cursor()
        curseur.execute("SHOW DATABASES LIKE %s", (nom_bdd,))
        for database in curseur:
            if database:
                return True
        return False
    except mysqlcon.Error as e:
        print(f"Erreur lors de la vérification de l'existence de la base de données : {e}")
        return False
    finally:
        if curseur:
            curseur.close()

def creer_tables(connexion):
    """
    Crée les tables nécessaires pour la base de données si elles n'existent
    pas déjà.

    Args:
        connexion (MySQLConnection): La connexion au serveur MySQL utilisée
        pour exécuter les requêtes.
    Raises:
        En cas d’erreur, un message est affiché mais l'exception n’est pas propagée.
    """
    curseur = None
    
    req_doc = """
        create table if not exists Docteurs(
        id_doc int primary key auto_increment,
        nom_doc varchar(50) not null,
        prenom_doc varchar(50) not null
        );
    """
    req_hosp = """
        create table if not exists Hopitaux(
        id_hosp int primary key auto_increment,
        nom_hosp varchar(100) not null unique
        );
    """
    req_doc_hosp = """
        create table if not exists Doct_Hosp(
        id_doc int,
        id_hosp int,
        primary key (id_doc,id_hosp),
        foreign key (id_hosp) references Hopitaux(id_hosp)
        on delete cascade on update cascade,
        foreign key (id_doc) references Docteurs(id_doc)
        on delete cascade on update cascade
        );
    """
    req_cham = """
        create table if not exists Chambres(
        id_chambre int primary key auto_increment,
        num_chambre int,
        id_hosp int,
        foreign key (id_hosp) references Hopitaux(id_hosp)
        on delete cascade on update cascade
        );
    """
    req_medi = """
        CREATE TABLE IF NOT EXISTS Medicaments(
        id_med INT PRIMARY KEY AUTO_INCREMENT,
        libelle VARCHAR(13) not null unique
        );
    """
    req_pat = """
       create table if not exists Patients(
       id_pat int primary key auto_increment,
       nom_pat varchar(50),
       prenom_pat varchar(50),
       genre enum ("Female", "Male"),
       groupe_sanguin  enum ("B-","B+","A+","A-","O+","O-","AB+","AB-")
       );
    """
    req_condit = """
       create table Condits(
	   id_cond int primary key auto_increment,
       nom_cond varchar(100) unique
       ); 
    """
    res_assu = """
       create table if not exists Assurances (
       id_assur int primary key auto_increment,
       nom_assur varchar(50) not null unique
       ); 
    """
    req_assu_pat = """
       create table if not exists Assu_Pat(
       id_assur int,
       id_pat int,
       primary key (id_assur, id_pat),
       foreign key (id_assur) references Assurances (id_assur)
       on delete cascade
       on update cascade,
       foreign key (id_pat) references Patients (id_pat)
       on delete cascade
       on update cascade
       ); 
    """
    req_tests = """
       create table if not exists Tests (
       id_test int primary key auto_increment,
       resultat varchar(50) unique
       ); 
    """
    req_hospi = """
        CREATE TABLE IF NOT EXISTS Hospitalisations(
        id INT PRIMARY KEY AUTO_INCREMENT,
        date_admission DATETIME,
        date_sortie DATETIME,
        type_admission ENUM("Emergency","Urgent","Elective"),
        montant DECIMAL(7, 2) CHECK(montant>0),
        id_doc INT,
        id_med INT,
        id_pat INT,
        id_test INT,
        id_cond INT,
        age INT check(age >0),
        FOREIGN KEY(id_med) REFERENCES Medicaments(id_med)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
        FOREIGN KEY(id_doc) REFERENCES Docteurs(id_doc)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
        FOREIGN KEY(id_pat) REFERENCES Patients(id_pat)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
        FOREIGN KEY(id_test) REFERENCES Tests(id_test)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
        FOREIGN KEY(id_cond) REFERENCES Condits(id_cond)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        );
    """
    
    dico = {req_doc:"Docteurs",req_hosp:"Hopitaux",
            req_doc_hosp:"Doct_Hosp",req_cham:"Chambres",req_medi:
                "Medicaments",req_pat:"Patients",req_condit:"Condits",
                res_assu:"Assurances",req_assu_pat:"Assu_Pat",req_tests:
                    "Tests",req_hospi:"Hospitalisations"}
    try:
        curseur = connexion.cursor()
        for req, table in dico.items():
            if tables_deja_creees(connexion, [table]) == False:
                try:
                    curseur.execute(req)
                    print(f"{table} créée")
                except mysqlcon.Error as e:
                    print(f"Erreur lors de la création de la table {table}: {e}")
                    raise e
            else:
                print(f"La table {table} existe déjà")
        print("Tables créées")
    except mysqlcon.Error as e:
        print(f"Erreur lors de la création des tables :{e}")
    
    finally:
        if curseur:
            curseur.close()
        
def inserer_nouveau_enreg(connexion, nom_table, noms_cols, valeurs):
    """
    Insère un nouvel enregistrement dans une table donnée en ignorant les doublons.

    Args:
        connexion (MySQLConnection): Connexion active à un serveur MySQL.
        nom_table (str): Nom de la table où on insère
        noms_cols (list[str]): Noms des colonnes concernées.
        valeurs (tuple): Valeurs à insérer.

    Returns:
        int ou None: ID de l’enregistrement inséré ou celui existant si contrainte unique, 
        ou None en cas d’échec.
        En cas d’erreur, un message est affiché mais l'exception n’est pas propagée.

    """

    curseur = None
    last_id = None
    try :
        curseur = connexion.cursor(dictionary=True)
        chaine_cols = "("+ ",".join(noms_cols)+")"  
        
        placeholder = "(%s" + ",%s"*(len(noms_cols)-1)+")"
        
        
        req1 = f"Describe {nom_table};"
        curseur.execute(req1)
        id_name = [elt["Field"] for elt in curseur.fetchall() if "PRI" in elt["Key"]][0]
        
        req = f"""
        INSERT IGNORE INTO {nom_table} {chaine_cols}
        VALUES {placeholder};
        """
        curseur.execute(req, valeurs)
        if curseur.rowcount == 1 : #Pour voir si une ligne a été réellement générée
            last_id = curseur.lastrowid
        else : #Si une ligne n'a pa sété générée, on essaie de retrouver son id
            where = " and ".join(f"{col} = %s" for col in noms_cols)
            req2 = f"""
            SELECT {id_name} FROM {nom_table} WHERE {where};
            """
            curseur.execute(req2, valeurs)
            result = curseur.fetchone()
            last_id = result[id_name] if result else None
    except mysqlcon.Error as e:
        print(f"Erreur lors de l'insertion : {e}")
    
    finally:
        if curseur:
            curseur.close()
    return last_id


def inserer_donnees(connexion, df) :
    """
    Cette fonction charge un DataFrame propre. 
    Elle insère les données ligne par ligne.

    Paramètres :
    connexion (MySQLConnection): Connexion active à un serveur MySQL.
    df_sante (pd.DataFrame): Le dataframe pour insertion.

    """
    curseur = None
    try : 
        curseur = connexion.cursor(dictionary=True)
        for indice, enreg in df.iterrows():
        
            # Insertion dans les tables indépendantes
                
            # Medical condition -  Condits
            cols_condits = ["nom_cond"]
            valeurs_cond = (enreg["Medical Condition"],)
            id_cond = inserer_nouveau_enreg(connexion, "Condits", cols_condits, valeurs_cond)
                    
            # Hospital -  Hopitaux
            cols_hosp = ["nom_hosp"]
            valeurs_hosp = (enreg["Hospital"],)
            id_hosp = inserer_nouveau_enreg(connexion, "Hopitaux", cols_hosp, valeurs_hosp)
                    
            # Medication - Medicaments
            cols_med = ["libelle"]
            valeurs_med = (enreg["Medication"],)
            id_med = inserer_nouveau_enreg(connexion, "Medicaments", cols_med, valeurs_med)
                    
            # Insurance Provider - Assurances
            cols_assur = ["nom_assur"]
            valeurs_assur = (enreg["Insurance Provider"],)
            id_assur = inserer_nouveau_enreg(connexion, "Assurances", cols_assur, valeurs_assur)
                    
            # Test Results - Tests
            cols_test = ["resultat"]
            valeurs_test = (enreg["Test Results"],)
            id_test = inserer_nouveau_enreg(connexion, "Tests", cols_test, valeurs_test)
                    
            # Docteurs    
            cols_doc = ["nom_doc", "prenom_doc"]
            valeurs_doc = (enreg["Doctor_name"], enreg["Doctor_surname"])
            id_doc = inserer_nouveau_enreg(connexion, "Docteurs", cols_doc, valeurs_doc)
                    
            # Patients    
            cols_pat = ["nom_pat", "prenom_pat", "genre", "groupe_sanguin"]
            valeurs_pat = (enreg["Patient_name"], enreg["Patient_surname"], enreg["Gender"], enreg["Blood Type"])
            id_pat = inserer_nouveau_enreg(connexion, "Patients", cols_pat, valeurs_pat)
                    
            # Insertion dans les tables enfants
                
            #Enfant docteur, hopital
            cols_doct_hosp = ["id_doc", "id_hosp"]
            valeurs_dh = (id_doc, id_hosp)
            id_dh = inserer_nouveau_enreg(connexion, "Doct_Hosp", cols_doct_hosp, valeurs_dh)

                    
            #Chambres
            cols_room = ["num_chambre", "id_hosp"]
            valeurs_room = (enreg["Room Number"], id_hosp)
            id_room = inserer_nouveau_enreg(connexion, "Chambres", cols_room, valeurs_room)
                    
            #Enfant assurances-patients
            cols_ap = ["id_assur", "id_pat"]
            valeurs_ap = (id_assur, id_pat)
            id_ap = inserer_nouveau_enreg(connexion, "Assu_pat", cols_ap, valeurs_ap)
                    
            # Hospitalisations
            cols_hospi = ["date_admission", "date_sortie", "type_admission", "montant", "age", "id_doc", "id_med",
                        "id_pat", "id_test", "id_cond"]
            valeurs_hospi = (enreg["Date of Admission"], enreg["Discharge Date"], enreg["Admission Type"],
                            enreg["Billing Amount"], enreg["Age"], id_doc, id_med, id_pat, id_test, id_cond)
            id_hospi = inserer_nouveau_enreg(connexion, "Hospitalisations", cols_hospi, valeurs_hospi)
        
            # Visualiser la progression
            if indice % 1000 == 0:
                print(f"{indice} lignes insérées sur {len(df)}")    
        connexion.commit()
        print("Les éléments ont été bien insérés")
    except Exception as e:
        print(f"Erreur lors de l'insertion. {e}")
        connexion.rollback()
    finally:
        if curseur:
            curseur.close()
        
            

#########################################################
####### Fonctions d'exploration de la bdd
#########################################################
def get_nbre_patients_par(connexion, condition):
    """
    Récupère le nombre de patients groupés selon une condition donnée
    (par genre, par groupe sanguin, par condition médicale,
    par type d’admission ou par assurance).

    Args:
        connexion (MySQLConnection): Connexion active à un serveur MySQL.
        condition (str): Critère de regroupement.

    Returns:
        list[dict] | None: Liste des résultats avec les totaux par condition.
        En cas d’erreur, un message est affiché mais l'exception n’est pas propagée.

    """
    curseur = None
    try : 
        curseur = connexion.cursor(dictionary = True)
        if condition == "genre" or condition == "groupe_sanguin" :
            req_cond_1 = f"""
            select {condition}, count(p.id_pat) as nbre_patient from Patients as p
            group by {condition};
            """
            curseur.execute(req_cond_1)
        
        if condition == "nom_cond":
            req_cond_2 = f"""
            select {condition}, count(p.id_pat) as nbre_patient from Patients as p
            join Hospitalisations as h on p.id_pat = h.id_pat join Condits as c on
            h.id_cond = c.id_cond 
            group by {condition};
            """
            curseur.execute(req_cond_2)
             
        if condition == "type_admission":
            req_cond_3 = f"""
            select {condition}, count(p.id_pat) as nbre_patient from Patients as p
            join Hospitalisations as h on p.id_pat = h.id_pat 
            group by {condition};
            """
            curseur.execute(req_cond_3)
               
        if condition == "nom_assur":
            req_cond_4 = f"""
            select {condition}, count(p.id_pat) as nbre_patient from Patients as p
            join Assu_Pat as ap on p.id_pat = ap.id_pat join Assurances as a on
            ap.id_assur = a.id_assur 
            group by {condition};
            """
            curseur.execute(req_cond_4)
                
        res = curseur.fetchall()
        if res:
            return res
        else :
            return None
    except mysqlcon.Error as e:
        print(f"Erreur lors dans la requête : {e}")
    
    finally:
        if curseur:
            curseur.close()


def get_top10_medecins_les_plus_consultes(connexion):
    """
    Récupère les 10 médecins ayant le plus grand nombre de consultations enregistrées.

    Args:
        connexion (MySQLConnection): Connexion active à un serveur MySQL.

    Returns:
        list[dict] | None: Liste de dictionnaires avec nom complet du médecin et nb de consultations.
        En cas d’erreur, un message est affiché mais l'exception n’est pas propagée.

    """
    curseur = None
    try:
        curseur = connexion.cursor(dictionary = True)
        req = """
        select concat(nom_doc, " ", prenom_doc) as Médécin, count(d.id_doc) as docteurs_plus_consultes from Docteurs as d join 
        Hospitalisations as h on d.id_doc = h.id_doc
        group by Médécin order by docteurs_plus_consultes desc
        limit 10;
        """
        curseur.execute(req)
        res = curseur.fetchall()
        return res
    
    except mysqlcon.Error as e:
        print(f"Erreur lors de l'exécution de la requête : {e}")
    
    finally:
        if curseur:
            curseur.close() 


def get_top5_hopitaux_a_plus_gros_budget(connexion):
    """
    Récupère les 5 hôpitaux ayant généré les plus gros budgets totaux via les rapports d’hospitalisation.

    Args:
        connexion (MySQLConnection): Connexion active à un serveur MySQL.

    Returns:
        list[dict] | None: Liste des hôpitaux avec leur budget total.
        En cas d’erreur, un message est affiché mais l'exception n’est pas propagée.

    """
    curseur = None 
    try:
        curseur = connexion.cursor(dictionary=True)
        requete = """
            SELECT h.nom_hosp, SUM(ho.montant) AS montant_total
            FROM hospitalisations ho
            JOIN doct_hosp dh ON dh.id_doc = ho.id_doc
            JOIN hopitaux h ON h.id_hosp = dh.id_hosp
            GROUP BY h.nom_hosp
            ORDER BY montant_total DESC
            LIMIT 5;
        """
        curseur.execute(requete)
        resultats = curseur.fetchall()
        for ligne in resultats:
           ligne['montant_total'] = float(ligne['montant_total'])
        return resultats

    except Exception as e:
        print(f"Erreur lors de la récupération des budgets : {e}")
        return None

    finally:
        if curseur is not None:
            curseur.close()

def get_nbre_sejours_annee(connexion):
    """
    Récupère le nombre total de séjours hospitaliers regroupés par année d’admission.

    Args:
        connexion (MySQLConnection): Connexion active à un serveur MySQL.

    Returns:
        list[dict] | None: Liste des années avec le nombre de séjours associés.
        En cas d’erreur, un message est affiché mais l'exception n’est pas propagée.

    """
    curseur = None
    try:
        curseur = connexion.cursor(dictionary = True)
        req = """
        SELECT YEAR(date_admission) as Annee_admission, COUNT(id) as 
        Nombre_de_séjours_hospitaliers
        FROM hospitalisations
        GROUP BY Annee_admission
        ORDER BY Nombre_de_séjours_hospitaliers desc;
        """
        curseur.execute(req)
        res = curseur.fetchall()
        return res
    except mysqlcon.Error as e:
        print(f"Erreur lors de l'exécution de la requête : {e}")
    
    finally:
        if curseur:
            curseur.close()

def get_nbre_sejours_par_mois(connexion, annee):
    """
    Récupère le nombre de séjours hospitaliers pour chaque mois d’une année donnée.

    Args:
        connexion (MySQLConnection): Connexion active à un serveur MySQL.
        annee (int): Année à filtrer.

    Returns:
        list[dict] | None: Liste des mois (en français) et du nombre de séjours associés.
        En cas d’erreur, un message est affiché mais l'exception n’est pas propagée.
    """
    curseur = None

    req = f"""
       select case 
           when month(date_admission)=01 then "Janvier"
           when month(date_admission)=02 then "Février"
           when month(date_admission)=03 then "Mars"
           when month(date_admission)=04 then "Avril"
           when month(date_admission)=05 then "Mai"
           when month(date_admission)=06 then "Juin"
           when month(date_admission)=07 then "Juillet"
           when month(date_admission)=08 then "Aout"
           when month(date_admission)=09 then "Septembre"
           when month(date_admission)=10 then "Octobre"
           when month(date_admission)=11 then "Novembre"
           when month(date_admission)=12 then "Décembre" 
           end as mois,
        count(id) as nbre_sejours_par_mois from hospitalisations
        where year(date_admission) = {annee}
        group by mois;
   
   """
    try:
        curseur = connexion.cursor(dictionary = True)
        curseur.execute(req)
        resultat = curseur.fetchall()
        return resultat
    
    except mysqlcon.Error as e:
        print(f"Erreur lors de la connexion à la base de données MySQL: {e}")        
    finally:  
        curseur.close()



def fct_principale(nom_bdd):
    """
    Établit une connexion à une base de données MySQL 
    et effectue des opérations.
    Gère les exceptions liées à la connexion et assure la fermeture de la connexion,
    qu'une erreur survienne ou non.
    Utilisée pour réaliser des tests
   
    Exceptions:
        - mysql.connector.Error: Intercepte et affiche les erreurs de connexion à la BDD.
    """
    
    connexion = None
    try:
        #Ici créer une connexion
        connexion = creer_connexion("localhost", "root", 
                                  "toulouse")
        liste_tables = ["Docteurs", "Hopitaux", "Doct_Hosp", "Chambres", "Medicaments", "Patients",
                        "Condits", "Assurances", "Assu_Pat", "Tests", "Hospitalisations"]
        #print("Connexion réussie à la bdd")
        #créer la base de données
        #vérifier si la base de données existe avant de la créer
        if bdd_existe(connexion, nom_bdd) == False : 
            creer_bdd(connexion, nom_bdd)
            
        #àselectionner la base de données
        selectionner_bdd(connexion, nom_bdd)
        
        #Véréfier si la bdd existe déjà
        #print(bdd_existe(connexion, nom_bdd))
        
        #Vérifier l'existence des tables
        if not tables_deja_creees(connexion, liste_tables) : 
            #Créer les tables
            creer_tables(connexion)
        
        # Insérer les données
        nom_fichier = "data/jeu_donnees_sante.csv"
        df = gn.nettoyer_preparer_donnees(nom_fichier)
        inserer_donnees(connexion, df)
    except mysqlcon.Error as e:
        print(f"Erreur lors de la connexion à la base de données MySQL: {e}")        
    finally:  
        #Fermeture de la connexion 
        fermer_connexion(connexion)




if __name__ == "__main__":
    nom_bdd = "bdd_soins_sante"
    #test des fonctions
    fct_principale(nom_bdd)
