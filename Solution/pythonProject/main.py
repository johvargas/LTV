import datetime
import os

import requests
import pandas as pd
import json
import sqlite3
from sqlite3 import Error
import click
import sys

tableQuery = "CREATE TABLE 'user' (  'id' integer NOT NULL PRIMARY KEY AUTOINCREMENT,  'user_id' varchar(36) NOT NULL DEFAULT '',  'created_at' timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',  'first_name' varchar(255) NOT NULL DEFAULT '',  'last_name' varchar(255) NOT NULL DEFAULT '',  'date_of_birth' date DEFAULT NULL,  'gender' varchar(6) NOT NULL DEFAULT '',  'phone' varchar(255) DEFAULT NULL,  'email' varchar(255) DEFAULT NULL,  'state' varchar(2) NOT NULL DEFAULT '',  'zip' integer NOT NULL,  'active' bool NOT NULL );"
apiPath = "http://de-challenge.ltvco.com/v1/users"
dbName = 'de-challenge.db'

# -----------------------------------------------------------------------------------------
# Main : Process the request, create data base and import to sqlite.
# paramether:
# Output: the table 'user' filled with the data from the API
# main.py --apikey={New Api Key}
# -----------------------------------------------------------------------------------------
@click.command()
@click.option("--apikey", default='ec093dd5-bbe3-4d8e-bdac-314b40afb796', help="Key to Access the API")
@click.option("--created", prompt="Initial Date (YYYY-MM-DD)", help=": Date used to filter the users based on their creation date")
def main(apikey, created):
    """Simple program that greets NAME for a total of COUNT times."""
    try:
        if validaDate(created):
            df = getRequest(apikey,created)
            createDB()
            importDFtoSQLite(df)
    except Error as e:
        print(e)

# -----------------------------------------------------------------------------------------
# createDB : Create the data base using the name specified at the top od the document.
# Paramether:
# output:
# -----------------------------------------------------------------------------------------

def createDB():

        conn = None
        try:
            if not os.path.isfile(dbName):
                conn = sqlite3.connect(dbName)
                click.echo(f"...Database {dbName} created.")
                c = conn.cursor()
                c.execute(tableQuery)
                conn.commit()
                click.echo(f"...table 'user' created.")
            else:
                click.echo(f"...Database already exists.")
        except Error  as e:
            print(e)
        finally:
            if conn:
                conn.close()


# -----------------------------------------------------------------------------------------
# importDFtoSQLlit: Inser into 'user' table the data within the dataframe.
# parameter: Dataframe witn the structure of columns
# output:
# -----------------------------------------------------------------------------------------
def importDFtoSQLite(df):
    conn = None
    try:
        conn = sqlite3.connect(dbName)
        df.to_sql('user', conn, if_exists='replace', index=False)
        click.echo('... User data imported to Database.')
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


# -----------------------------------------------------------------------------------------
# getRequest: execute get request, to the path,
# parameter:    apikey
#                 createDate : create date for users, with format 'YYYY-MM-DD'
# output: Dataframe with all the data from the response,
# -----------------------------------------------------------------------------------------
def getRequest(apiKey, createdDate):
    click.echo(f"... Getting list of users")

    query = {'api_key': apiKey, 'created_at': createdDate}
    response = requests.get(apiPath, params=query)

    strUsers = json.dumps(response.json())
    jsonUsers = json.loads(strUsers)
    jsonUsers = fixEmptyContacts(jsonUsers)
    df_norm = pd.json_normalize(jsonUsers)
    df_norm = setColumnTypesNames(df_norm)

    click.echo(f"... {getRowsCount(df_norm)} records founds.")
    return df_norm


# -----------------------------------------------------------------------------------------
# fixEmptyContacts: Create empty Dict object for None not values
# parameter: List of users with all the information
# outout: enriched list of user without None values in COntact columns
# -----------------------------------------------------------------------------------------
def fixEmptyContacts(jsonUsers):
    contact = {"email":"", "phone":""}
    for n in range(len(jsonUsers)):
       if not bool(jsonUsers[n]["contact"]):
           jsonUsers[n]["contact"] = contact
    return jsonUsers


def getRowsCount(df):
    index = df.index
    return len(index)

# -----------------------------------------------------------------------------------------
# setColumnTypesNames: set the columns name in  order to match with the ones in DB
# -----------------------------------------------------------------------------------------
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

def validaDate(createDate):
    format = "%Y-%m-%d"

    try:
        datetime.datetime.strptime(createDate, format)
        return createDate
    except ValueError:
        print("This is the incorrect date string format. It should be YYYY-MM-DD")

if __name__ == '__main__':
    main()