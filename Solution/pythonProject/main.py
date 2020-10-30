import requests
import pandas as pd
import json
import sqlite3
from sqlite3 import Error

filename = "response.json"
tableQuery = "CREATE TABLE 'user' (  'id' integer NOT NULL PRIMARY KEY AUTOINCREMENT,  'user_id' varchar(36) NOT NULL DEFAULT '',  'created_at' timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',  'first_name' varchar(255) NOT NULL DEFAULT '',  'last_name' varchar(255) NOT NULL DEFAULT '',  'date_of_birth' date DEFAULT NULL,  'gender' varchar(6) NOT NULL DEFAULT '',  'phone' varchar(255) DEFAULT NULL,  'email' varchar(255) DEFAULT NULL,  'state' varchar(2) NOT NULL DEFAULT '',  'zip' integer NOT NULL,  'active' bool NOT NULL );"
dbName = 'codeChallenge.db'

def getFromRequest():

    query = {'api_key': 'ec093dd5-bbe3-4d8e-bdac-314b40afb796', 'created_at': '2010-01-01'}
    response = requests.get("http://de-challenge.ltvco.com/v1/users", params=query)

    strUsers = json.dumps(response.json())
    jsonUsers = json.loads(strUsers)
    jsonUsers = fixEmptyContacts(jsonUsers)
    #        df = pd.DataFrame.from_dict(jsonUsers)
    df_norm = pd.json_normalize(jsonUsers)

    return df_norm


def getFromFile():
    with open(filename) as f:
        d = json.load(f)
        strUsers = json.dumps(d)
        jsonUsers = json.loads(strUsers)
        jsonUsers = fixEmptyContacts(jsonUsers)
#        df = pd.DataFrame.from_dict(jsonUsers)
        df_norm = pd.json_normalize(jsonUsers)

        return df_norm


def fixEmptyContacts(jsonUsers):
    contact = {"email":"", "phone":""}
    for n in range(len(jsonUsers)):
       if not bool(jsonUsers[n]["contact"]):
           jsonUsers[n]["contact"] = contact
    return jsonUsers


def setColumnTypesNames(df):
    df = df.astype({'created_at': 'int32'})
    df = df.astype({'address.zip':'int32'})

    df = df.rename(columns={'address.zip': 'zip'})
    df = df.rename(columns={'contact.email': 'email'})
    df = df.rename(columns={'contact.phone': 'phone'})
    df = df.rename(columns={'address.state': 'state'})
    df = df.rename(columns={'name.first': 'first_name'})
    df = df.rename(columns={'name.last': 'last_name'})
    df = df.rename(columns={'dob': 'date_of_birth'})

    return df


def jsonToDataFrame(jsonStr):
    json_a = json.loads(jsonStr)
    print(json_a)
    print( type(json_a))
    dataframe = pd.DataFrame.from_dict(json_a)
    dataframe.describe()
    print("end process ToDataFram")

def connToDB():
    conn = sqlite3.connect('codeChallenge.db')
    c = conn.cursor()
    c.execute(tableQuery)
    conn.commit()
    return  conn

def create_connection(db_file, df):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        df.to_sql('user', conn, if_exists='replace', index=False)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def printRow(df, pos):
    for col in df.columns:
        print(col, "  ", df.at[pos, col])

if __name__ == '__main__':
#    readJsonAPI()
    df = getFromRequest()
  #  df = getFromFile()
    df = setColumnTypesNames(df)
    create_connection(dbName, df)

