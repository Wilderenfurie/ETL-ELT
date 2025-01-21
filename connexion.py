import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from urllib.parse import quote_plus

username = "root"
password = "root"
host = "127.0.0.1"
port = 3306
database = "my_dbt_db"

DATABASE_URI = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'

engine = create_engine(DATABASE_URI)
if not database_exists(engine.url):
    create_database(engine.url)
    print(f"La base de données '{database}' a été créée.")
else:
    print(f"La base de données '{database}' existe déjà.")

liste_tables = ["customers", "items", "orders", "products", "stores", "supplies"]

for table in liste_tables:
    try:
        csv_url = f"https://raw.githubusercontent.com/dsteddy/jaffle_shop_data/main/raw_{table}.csv"
        df = pd.read_csv(csv_url)
        print(f"Chargement du fichier : {csv_url}")

        df.to_sql(f"raw{table}", engine, if_exists="replace", index=False)
        print(f"Table 'raw{table}' chargée avec succès.")
    except Exception as e:
        print(f"Erreur lors du chargement de la table '{table}': {e}")