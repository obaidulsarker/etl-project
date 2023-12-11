import psycopg2
import logging
from psycopg2 import sql
from datetime import datetime
from dotenv import load_dotenv
import os
load_dotenv()
# Settings

loginInfo = {'host': os.getenv('DB_SERVER'), 'user': os.getenv('DB_USER'), 'password': os.getenv('DB_PASSWORD'),
             'port': os.getenv('DB_PORT')}

business_DatabaseInfo = loginInfo.copy()
business_DatabaseInfo['DBName'] = os.getenv('DB_NAME_PENSION')

stage_DatabaseInfo = loginInfo.copy()
stage_DatabaseInfo['DBName'] = os.getenv('DB_NAME_STG')

partner_DatabaseInfo = loginInfo.copy()
partner_DatabaseInfo['DBName'] = os.getenv('DB_NAME_REG')

datamart_DatabaseInfo = loginInfo.copy()
datamart_DatabaseInfo['DBName'] = os.getenv('DB_NAME_KRI')


# Classes
class Scheme:
    def __init__(self):
        self.scheme_id = None
        self.scheme_name = None
        self.scheme_type = None
        self.trustee_id = None
        self.FM_ID = None
        self.FC_ID = None

    def setSchemeID(self, scheme_id):
        self.scheme_id = scheme_id

    def setSchemeName(self, scheme_name):
        self.scheme_name = scheme_name

    def setSchemeType(self, scheme_type):
        self.scheme_type = scheme_type

    def setTrusteeID(self, trustee_id):
        self.trustee_id = trustee_id

    def setFMID(self, FM_ID):
        self.FM_ID = FM_ID

    def setFCID(self, FC_ID):
        self.FC_ID = FC_ID

    def getSchemeID(self):
        return self.scheme_id

    def getSchemeName(self):
        return self.scheme_name

    def getSchemeType(self):
        return self.scheme_type

    def getTrusteeID(self):
        return self.trustee_id

    def getFMID(self):
        return self.FM_ID

    def getFCID(self):
        return self.FC_ID


class Trustee:
    def __init__(self):
        self.trustee_id = None
        self.trustee_name = None

    def setTrusteeID(self, trustee_id):
        self.trustee_id = trustee_id

    def setTrusteeName(self, trustee_name):
        self.trustee_name = trustee_name

    def getTrusteeID(self):
        return self.trustee_id

    def getTrusteeName(self):
        return self.trustee_name


def connect(databaseInfo):
    """
    This function connects to a database and returns a connection object
    :param databaseInfo: dictionary
    :return: connection object
    """
    try:
        connection = psycopg2.connect(
            host=databaseInfo['host'],
            user=databaseInfo['user'],
            password=databaseInfo['password'],
            port=databaseInfo['port'],
            database=databaseInfo['DBName']
        )
        return connection
    except Exception as e:
        print(e)


def retrieveSchemeData(partnerDB):
    partnerConnector = partnerDB.cursor()
    # Get all the schemes from the scheme table
    partnerConnector.execute('''
            SELECT * FROM schemes_scheme;
        ''')

    schemes = partnerConnector.fetchall()
    schemesList = []  # List of all schemes objects
    for scheme in schemes:
        ID = scheme[1]
        name = scheme[2]
        schemeType = scheme[3]
        newScheme = Scheme()
        newScheme.setSchemeID(ID)
        newScheme.setSchemeName(name)
        newScheme.setSchemeType(schemeType)
        schemesList.append(newScheme)

    return schemesList


def retrieveTrusteeData(partnerDB):
    partnerConnector = partnerDB.cursor()
    # Get all the trustees from the trustee table
    partnerConnector.execute('''
            SELECT trustee_id, company_name FROM schemes_corporatetrustee;
        ''')

    trustees = partnerConnector.fetchall()
    trusteesList = []  # List of all trustees objects
    for trustee in trustees:
        ID = trustee[0]
        name = trustee[1]
        newTrustee = Trustee()
        newTrustee.setTrusteeID(ID)
        newTrustee.setTrusteeName(name)
        trusteesList.append(newTrustee)

    return trusteesList


def insert_Scheme(database, scheme):
    datamartConnector = database.cursor()
    datamartConnector.execute('''
        INSERT INTO scheme (scheme_id, scheme_name, scheme_type_code_ID, trustee_id, fm_id, fc_id)
        VALUES (%s, %s, %s, %s, %s, %s);
    ''', (scheme.getSchemeID(), scheme.getSchemeName(), scheme.getSchemeType(), scheme.getTrusteeID(), scheme.getFMID(),
          scheme.getFCID()))
    database.commit()


def insert_Trustee(database, trustee):
    datamartConnector = database.cursor()
    datamartConnector.execute('''
        INSERT INTO trustee (trustee_id, trustee_name)
        VALUES (%s, %s);
    ''', (trustee.getTrusteeID(), trustee.getTrusteeName()))
    database.commit()


def insertData(schemesList, trusteesList, businessDB):
    for scheme in schemesList:
        try:
            insert_Scheme(businessDB, scheme)
        except Exception as e:
            print(e)
    for trustee in trusteesList:
        try:
            insert_Trustee(businessDB, trustee)
        except Exception as e:
            print(e)

def retrieveData(partnerDB):
    schemesList = retrieveSchemeData(partnerDB)
    trusteesList = retrieveTrusteeData(partnerDB)

    return schemesList, trusteesList


def main():
    # Connect to databases
    partnerDB = connect(partner_DatabaseInfo)
    businessDB = connect(business_DatabaseInfo)
    businessDB.autocommit = True
    schemesList, trusteesList = retrieveData(partnerDB)
    insertData(schemesList, trusteesList, businessDB)

    # Close connections
    partnerDB.close()
    businessDB.close()


if __name__ == '__main__':
    main()
