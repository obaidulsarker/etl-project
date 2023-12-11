import psycopg2
# from datetime import datetime
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

# Constants
quarters = {
    'Q1': ['January', 'February', 'March'],
    'Q2': ['April', 'May', 'June'],
    'Q3': ['July', 'August', 'September'],
    'Q4': ['October', 'November', 'December']
}

monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
              'November', 'December']

monthNumbers = {
    'January': 1,
    'February': 2,
    'March': 3,
    'April': 4,
    'May': 5,
    'June': 6,
    'July': 7,
    'August': 8,
    'September': 9,
    'October': 10,
    'November': 11,
    'December': 12
}


# General Functions
def compare_Strings(string1, string2):
    """
    This function compares two strings and returns True if they are equivalent, False otherwise
    :param string1: string
    :param string2: string
    :return: boolean
    """
    string1 = string1.lower().replace(' ', '').replace('-', '').replace('_', '')
    string2 = string2.lower().replace(' ', '').replace('-', '').replace('_', '')

    return string1 == string2


def getPreviousMonth(year, month):
    """
    This function returns the previous month and year
    :param year: integer
    :param month: String
    :return: tuple of integers
    """
    if month == 'January':
        return year - 1, 'December'
    else:
        return year, monthNames[monthNames.index(month) - 1]


def subtractDates(date1, date2):
    """
    This function subtracts two dates and returns the difference in days
    :param date1: postgreSQL date object
    :param date2: postgreSQL date object
    :return: integer
    """
    return (date1 - date2).days


# CSV Logger
def logCSV(SchemeID: str, KRI_ID: int, refPeriod: str, year: int, Description: str, Data: list) -> None:
    """
    :param SchemeID: Scheme ID, String
    :param KRI_ID: int, KRI value
    :param refPeriod: reference period for data
    :param year: int, year of data
    :param Description: String, description of data
    :param Data: list of tuples, or one tuple
    :return: None
    """
    try:
        outputFileName = "/LOGS/output.csv"
        with open(outputFileName, 'a+') as file:
            if type(Data) == tuple:
                file.write(f"{SchemeID},{KRI_ID},{refPeriod},{year},{Description},{Data}\n")
            if type(Data) == int:
                file.write(f"{SchemeID},{KRI_ID},{refPeriod},{year},{Description},{Data}\n")
            else:
                for data in Data:
                    file.write(f"{SchemeID},{KRI_ID},{refPeriod},{year},{Description},{data}\n")

            file.write("\n")
    except Exception as e:
        print(e)
        print(f'Error writing to CSV file for {SchemeID}, {KRI_ID}, {refPeriod}, {year}, {Description}, {Data}')


def printKRI_Error(KRI_ID: int, entityID, Description: Exception) -> None:
    print(f'Error Calculating KRI_{KRI_ID}, for entity: {entityID}, \nError: {Description}\n')


# Classes
class Scheme:
    def __init__(self, ID, scheme_id, scheme_name, scheme_type_id, tier_type=None):
        self.id = ID
        self.scheme_id = scheme_id
        self.scheme_name = scheme_name
        self.scheme_type_id = scheme_type_id
        self.tier_type = tier_type
        self.ManagingTrustees = []

    def setTierType(self, tier_type):
        self.tier_type = tier_type

    def getSchemeID(self):
        return self.scheme_id

    def getSchemeName(self):
        return self.scheme_name

    def addManagingTrustee(self, trustee):
        self.ManagingTrustees.append(trustee)

    def getCustodians(self):
        for trustee in self.ManagingTrustees:
            if trustee.trustee_type == "Custodian":
                return trustee

    def getFundManagers(self):
        for trustee in self.ManagingTrustees:
            if trustee.trustee_type == "Fund Manager":
                return trustee


class Trustee:
    def __init__(self, ID, trustee_id):
        self.ID = ID
        self.trustee_id = trustee_id
        self.trustee_type = None
        self.schemes_managed = []

    def setTrusteeType(self, trusteeType):
        self.trustee_type = trusteeType

    def getTrusteeID(self):
        return self.trustee_id

    def addManagedSchemes(self, partnerDatabaseConnection, schemesList):
        """
        :param partnerDatabaseConnection: connection object
        :param schemesList: list of scheme objects
        :return: None
        This function adds all schemes managed by the trustee to the schemes_managed list
        It is done by finding all fields containing the trustee ID in the trustee_appointment table
        """
        cursor = partnerDatabaseConnection.cursor()
        cursor.execute('''
            SELECT scheme_id
            FROM schemes_corporatetrusteeappointment
            WHERE trustee_id = %s;
        ''', (self.trustee_id,))
        schemes = cursor.fetchall()

        for scheme in schemes:
            schemeObject = findSchemeObjectUsingID(scheme[0], schemesList)
            if schemeObject is not None:
                self.schemes_managed.append(schemeObject)
                schemeObject.addManagingTrustee(self)


# Object Finders
def findSchemeObjectUsingID(ID, schemesList):
    """
    This function finds a scheme in a list of schemes and returns the scheme object if found, None otherwise
    It uses the database ID of the scheme
    :param ID: integer
    :param schemesList: list of scheme objects
    :return: scheme object or None
    """
    for scheme in schemesList:
        if scheme.ID == ID:
            return scheme
    return None


def findSchemeObjectUsingSchemeID(scheme_id, schemesList):
    """
    This function finds a scheme in a list of schemes and returns the scheme object if found, None otherwise
    It uses the scheme ID of the scheme
    :param scheme_id: integer
    :param schemesList: list of scheme objects
    :return: scheme object or None
    """
    for scheme in schemesList:
        if scheme.scheme_id == scheme_id:
            return scheme
    return None


def findTrusteeObject(trustee_id, trusteesList):
    """
    This function finds a trustee in a list of trustees and returns the trustee object if found, None otherwise
    :param trustee_id: integer
    :param trusteesList: list of trustee objects
    :return: trustee object or None
    """
    for trustee in trusteesList:
        if trustee.trustee_id == trustee_id:
            return trustee
    return None


def findTrusteesManagingScheme(scheme_id, trusteesList):
    """
    This function finds all trustees managing a scheme and returns a list of trustee objects
    :param scheme_id: integer
    :param trusteesList: list of trustee objects
    :return: list of trustee objects
    """
    trusteesManagingScheme = []
    for trustee in trusteesList:
        for scheme in trustee.schemes_managed:
            if scheme.scheme_id == scheme_id:
                trusteesManagingScheme.append(trustee)
    return trusteesManagingScheme


# Database functions
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


def insertKRIValueScheme(businessDatabaseConnection, schemeID, Calc_ID, value, referencePeriod, referenceYear,
                         KRI_Number, onsiteValue=None, regulatoryValue=None):
    """
    This function inserts a KRI value into the Scheme_KRI_Raw table
    :param businessDatabaseConnection: connection object
    :param schemeID: integer
    :param Calc_ID: integer
    :param value: integer
    :param referencePeriod: string
    :param referenceYear: integer
    :param KRI_Number: integer
    :param onsiteValue: integer
    :param regulatoryValue: integer
    :return: None
    """
    try:
        kriType = f'KRI_ID_000{KRI_Number}' if KRI_Number < 10 else f'KRI_ID_00{KRI_Number}'
        cursor = businessDatabaseConnection.cursor()
        cursor.execute('''
            INSERT INTO scheme_kri (scheme_id, calculation_id, raw_value, regulatory_mitigation_value, onsite_mitigation_value, reference_period, reference_year, kri_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        ''', (schemeID, Calc_ID, value, regulatoryValue, onsiteValue, referencePeriod, referenceYear, kriType))
        businessDatabaseConnection.commit()
    except Exception as e:
        print(e)


def insertStatValue(businessDatabaseConnection, scheme, Calc_ID, value, referencePeriod, referenceYear, statNo):
    """
    :param businessDatabaseConnection: connection object
    :param scheme: scheme Object
    :param Calc_ID: Calculation ID
    :param value: int
    :param referencePeriod:
    :param referenceYear:
    :param statNo:
    :return:
    """
    entityID = scheme.getSchemeID()
    entityName = scheme.getSchemeName()
    statID = f'STAT_ID_000{statNo}' if statNo < 10 else f'STAT_ID_00{statNo}'
    try:
        cursor = businessDatabaseConnection.cursor()
        cursor.execute(f'''
            INSERT INTO statisticalcalculation (statisticalcalculation_id, statistical_id, calculation_id, entity_id, 
            entityname, referenceperiodyear, referenceperiod, value)
            VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s);
        ''', (statID, Calc_ID, entityID, entityName, referencePeriod, referenceYear, value))
        businessDatabaseConnection.commit()
    except Exception as e:
        print(e)


def insertKRIValueTrustee(businessDatabaseConnection, trustee, calcID, value, referencePeriod, referenceYear,
                          KRI_Number,
                          onsiteValue=None, regulatoryValue=None):
    """
    :param businessDatabaseConnection: connection object
    :param trustee: trustee object
    :param calcID: integer
    :param value: integer
    :param referencePeriod: string
    :param referenceYear: integer
    :param KRI_Number: integer
    :param onsiteValue: integer
    :param regulatoryValue: integer
    :return: None
    """
    trusteeID = trustee.getTrusteeID()
    kriType = f'KRI_ID_000{KRI_Number}' if KRI_Number < 10 else f'KRI_ID_00{KRI_Number}'
    try:
        cursor = businessDatabaseConnection.cursor()
        cursor.execute('''
            INSERT INTO trustee_kri (trustee_id, calculation_id, raw_value, regulatory_mitigation_value, onsite_mitigation_value, 
            reference_period, reference_year, kri_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        ''', (trusteeID, calcID, value, regulatoryValue, onsiteValue, referencePeriod, referenceYear, kriType))
        businessDatabaseConnection.commit()
    except Exception as e:
        print(e)

    for scheme in trustee.schemes_managed:
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, calcID, value, referencePeriod,
                             referenceYear, KRI_Number, onsiteValue, regulatoryValue)


def insertDataValues(BusinessDB, Calc_ID, data, dataset):
    """
    TODO
    This function inserts data rows into the dataset table on business Database level.
    These data rows added are added alongside the calculation ID for which the rows were used.
    This is to know which data has been used to calculate which KRI values
    :param BusinessDB: Database connection object
    :param Calc_ID: int, calculation ID
    :param data:
    :param dataset:
    :return: None
    """

    try:
        cursor = BusinessDB.cursor()

    except Exception as e:
        print(e)


# Entity Getters
def getSchemesList(partnerDatabaseConnection):
    """
    This function returns a list of all schemes in the partner database
    :param partnerDatabaseConnection: connection object
    :return: list of scheme objects
    """
    try:
        cursor = partnerDatabaseConnection.cursor()
        cursor.execute('''
            SELECT id, Scheme_ID, Scheme_Name, Scheme_Type_ID FROM schemes_scheme;
        ''')
        schemesList = []
        for scheme in cursor.fetchall():
            schemesList.append(Scheme(scheme[0], scheme[1], scheme[2], scheme[3]))

        cursor.execute('''
            SELECT id, tier_type FROM schemes_schemetype;
        ''')

        return schemesList
    except Exception as e:
        print(e)


def getTrusteesList(partnerDatabaseConnection):
    """
    This function returns a list of all trustees in the partner database
    :param partnerDatabaseConnection: connection object
    :return: list of trustee objects
    """
    try:
        cursor = partnerDatabaseConnection.cursor()
        cursor.execute('''
            SELECT id, Trustee_ID FROM schemes_corporatetrustee;
        ''')
        trusteesList = []
        for trustee in cursor.fetchall():
            trusteesList.append(Trustee(trustee[0], trustee[1]))
        return trusteesList
    except Exception as e:
        print(e)


# Get reference periods

def getDatasetCurrentMonth(stageDatabaseConnection, datasetID, entityID):
    """
    This function returns the current month for a dataset (the month of the last entry in the dataset)
    :param stageDatabaseConnection: connection object
    :param datasetID: String
    :param entityID: String
    :return: string
    """
    tableName = "Dataset_" + datasetID
    cursor = stageDatabaseConnection.cursor()

    # Select the last entry in the dataset for the relevant entityID
    executionString = f'SELECT "referenceperiod" FROM "{tableName}" WHERE "{tableName}"."entity_id" = \'{entityID}\''
    cursor.execute(executionString)
    result = cursor.fetchall()
    return result[-1][0]


def getDatasetCurrentYear(stageDatabaseConnection, datasetID, entityID):
    """
    This function returns the current year for a dataset (the year of the last entry in the dataset)
    :param stageDatabaseConnection: connection object
    :param datasetID: String
    :param entityID: String
    :return: string
    """
    cursor = stageDatabaseConnection.cursor()
    tableName = "Dataset_" + datasetID
    executionString = f'SELECT "referenceperiodyear" FROM "{tableName}" WHERE "{tableName}"."entity_id" = \'{entityID}\''
    cursor.execute(executionString)
    return cursor.fetchall()[-1][0]


def getDatasetCurrentQuarter(stageDatabaseConnection, datasetID, entityID):
    """
    This function returns the current quarter for a dataset
    :param stageDatabaseConnection: connection object
    :param datasetID: String
    :param entityID: String
    :return: string
    """
    month = getDatasetCurrentMonth(stageDatabaseConnection, datasetID, entityID)
    if month in quarters["Q1"] or month == "Q1":
        return "Q1"
    elif month in quarters["Q2"] or month == "Q2":
        return "Q2"
    elif month in quarters["Q3"] or month == "Q3":
        return "Q3"
    elif month in quarters["Q4"] or month == "Q4":
        return "Q4"


def getLatestTimeStamp(stageDatabaseConnection, tableName, entityID):
    """
    This function returns the latest timestamp in a table for a certain entity
    :param stageDatabaseConnection: connection object
    :param tableName: String
    :param entityID: String
    :return: String
    """
    if 'Dataset' not in tableName:
        tableName = "Dataset_" + tableName
    cursor = stageDatabaseConnection.cursor()
    executionString = f"""
        SELECT "time_added_to_db" FROM "{tableName}" WHERE "{tableName}"."entity_id" = '{entityID}'
        ORDER BY "time_added_to_db" DESC LIMIT 1;
        """

    cursor.execute(executionString)
    return cursor.fetchall()[0][0]


# Get relevant mitigation data
def getMitigationData(stageDatabaseConnection, KRI_Type, scheme, referencePeriod, referenceYear):
    """
    This function returns the mitigation data for a scheme KRI, which is used to calculate the mitigation values
    :param stageDatabaseConnection: connection object
    :param KRI_Type: int
    :param scheme: scheme object
    :param referencePeriod: string
    :param referenceYear: int
    :return: tuple
    """
    # Convert reference period to month if it is a quarter
    referencePeriods = ['P1', 'P2', 'P3']
    if referencePeriod == "Q1":
        referencePeriods = quarters["Q1"]
        referencePeriod = "March"
    elif referencePeriod == "Q2":
        referencePeriod = "June"
        referencePeriods = quarters["Q2"]
    elif referencePeriod == "Q3":
        referencePeriod = "September"
        referencePeriods = quarters["Q3"]
    elif referencePeriod == "Q4":
        referencePeriod = "December"
        referencePeriods = quarters["Q4"]

    # Get the Onsite mitigation value for the relevant KRI type
    cursor = stageDatabaseConnection.cursor()
    onsiteMitigationValue = None
    regulatoryMitigationValue = None
    entityID = scheme.scheme_id
    kri_Type = f'000{KRI_Type}' if KRI_Type < 10 else f'00{KRI_Type}'
    try:
        FieldName = f'kri_id_{kri_Type}-onsite'
        cursor.execute(f'''
            SELECT "{FieldName}" FROM "Dataset_0607"
            WHERE entity_id = %s AND referenceperiodyear = %s AND (referenceperiod = %s OR referenceperiod = %s OR 
            referenceperiod = %s OR referenceperiod = %s);
        ''', (entityID, referenceYear, referencePeriod, referencePeriods[0], referencePeriods[1],
              referencePeriods[2]))
        onsiteMitigationValue = cursor.fetchall()[0][-1]
    except Exception as e:
        print(e)

    # Get the Regulatory mitigation value for the relevant KRI type
    try:
        FieldName = f'kri_id_{kri_Type}-regulatory'
        cursor.execute(f'''
            SELECT "{FieldName}" FROM "Dataset_0606"
            WHERE entity_id = %s AND referenceperiodyear = %s AND (referenceperiod = %s OR referenceperiod = %s OR 
            referenceperiod = %s OR referenceperiod = %s);
        ''', (entityID, referenceYear, referencePeriod, referencePeriods[0], referencePeriods[1],
              referencePeriods[2]))
        regulatoryMitigationValue = cursor.fetchall()[0][-1]
    except Exception as e:
        print(e)

    return onsiteMitigationValue, regulatoryMitigationValue


# KRI calculators

def calculateKRI1(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO: KRI 1
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None

    from dataset 0404 Monthly Contribution Data
    count until the first negative value of the 14th day of the month
    minus CONTRIBUTIONS_RECEIPT_DATE

    date format dd/mm/yyyy
    if less than or equal 0, KRI = 4 else KRI = 1

    Calculated monthly, for every employer in a scheme
    """
    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0404', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0404', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0601', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "contributions_receipt_date" FROM "Dataset_0404" WHERE "entity_id" = '{0}' AND 
            "referenceperiod" = '{1}' AND "referenceperiodyear" = '{2}' AND "time_added_to_db" = '{3}'
            ORDER BY "contributions_receipt_date" ASC LIMIT 1;
            '''.format(entityID, month, year, latestTimeStamp))

        date = cursor.fetchone()
        dayInDate = date.split('/')[0]
        if int(dayInDate) <= 14:
            KRI = 1
        else:
            KRI = 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 1, scheme, month, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, month, year, 1, onsite,
                             regulatory)

        logCSV(entityID, 1, month, year, 'day Received', dayInDate)

    except Exception as e:
        printKRI_Error(1, scheme.getSchemeID(), e)


def calculateKRI2(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO: KRI2, what does tier 3 contributions in the formula mean?
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None

    Deals with dataset 0601
    x = Type of Complaint(s) Non Payment of contributions Tier #2
    divided by
    dataset 0403 EMPLOYER DATA
    Number of Contributors

    KRI = 1 if x = 0, KRI = 2 if 0.1< x<0.3, KRI = 4 if x >= 0.3
    calculated quarterly
    """
    try:
        entityID = scheme.getSchemeID()
        quarter = getDatasetCurrentQuarter(stageDatabaseConnection, '0601', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0601', entityID)
        months = quarters[quarter]
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0601', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM "Dataset_0601" WHERE (referenceperiod='{0}' OR referenceperiod='{1}' OR 
            referenceperiod='{2}') AND referenceperiodyear = '{3}' AND 
            entity_id = '{4}' AND "type of complaint(s)" = 'Non Payment of contributions Tier #2' AND
            "time_added_to_db" = '{5}';
            '''.format(months[0], months[1], months[2], year, entityID, latestTimeStamp))

        count = cursor.fetchall()[0][0]

        cursor.execute('''
            SELECT COUNT(*) FROM "Dataset_0403" WHERE referenceperiod = '{0}' AND referenceperiodyear = '{1}' AND entity_id = '{2}';
            '''.format(months[0], year, entityID))

        numberOfContributors = cursor.fetchall()[0][0]

        try:
            x = count / numberOfContributors
        except ZeroDivisionError:
            x = 0

        KRI = 1 if x == 0 else 2 if 0.1 < x < 0.3 else 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 2, scheme, quarter, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, quarter, year, 2, onsite,
                             regulatory)

        logCSV(entityID, 2, quarter, year, 'Count', count)

    except Exception as e:
        printKRI_Error(2, scheme.getSchemeID(), e)


def calculateKRI3(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None
    from dataset 0602 COMMUNICATION MONITORING
    Counts LOG-Status=Misleading
    Generates for every quarter, risk score = 1 if count == 0, else 4
    """
    try:
        entityID = scheme.getSchemeID()
        quarter = getDatasetCurrentQuarter(stageDatabaseConnection, '0602', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0602', entityID)
        months = quarters[quarter]
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0602', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM "Dataset_0602" WHERE (referenceperiod='{0}' OR referenceperiod='{1}' OR 
            referenceperiod='{2}') AND referenceperiodyear = '{3}' AND entity_id = '{4}' AND status = 'Misleading'
            AND "time_added_to_db" = '{5}';
            '''.format(months[0], months[1], months[2], year, entityID, latestTimeStamp))
        count = cursor.fetchone()[0]

        KRI = 1 if count == 0 else 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 3, scheme, quarter, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, quarter, year, 3, onsite,
                             regulatory)

        logCSV(entityID, 3, quarter, year, 'Count', count)

    except Exception as e:
        printKRI_Error(3, scheme.getSchemeID(), e)


def calculateKRI4(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None
    Deals with dataset 0601 REFID-COMPLAINTS CASE REGISTER-Type of Complaint(s)
    Counts complaints of type Lack of communication
    Generates for every quarter, risk score = 1 if between 0 and 4, 2 if between 5 and 10, 3 if between 11 and 20,
    4 if bigger than 20
    """
    try:
        entityID = scheme.getSchemeID()
        quarter = getDatasetCurrentQuarter(stageDatabaseConnection, '0601', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0601', entityID)
        months = quarters[quarter]
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0601', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM "Dataset_0601" WHERE (referenceperiod='{0}' OR referenceperiod='{1}' OR 
            referenceperiod='{2}') AND referenceperiodyear = '{3}' AND entity_id = '{4}' AND 
            "type of complaint(s)" = 'Lack of communication' AND "time_added_to_db" = '{5}';
            '''.format(months[0], months[1], months[2], year, entityID, latestTimeStamp))

        count = cursor.fetchone()[0]

        KRI = 1 if count <= 4 else 2 if count <= 10 else 3 if count <= 20 else 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 4, scheme, quarter, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, quarter, year, 4, onsite,
                             regulatory)

        logCSV(entityID, 4, quarter, year, 'Count', count)
    except Exception as e:
        printKRI_Error(4, scheme.getSchemeID(), e)


def calculateKRI5(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None

    Deals with dataset 0601 REFID-COMPLAINTS CASE REGISTER-Type of Complaint(s)
    Counts complaints of type Inaccurate information to scheme members
    Generates for every quarter, risk score = 0 if zero, and 4 if not zero
    """
    try:
        entityID = scheme.getSchemeID()
        quarter = getDatasetCurrentQuarter(stageDatabaseConnection, '0601', entityID)
        months = quarters[quarter]
        year = getDatasetCurrentYear(stageDatabaseConnection, '0601', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0601', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM "Dataset_0601" WHERE (referenceperiod = '{0}' OR referenceperiod = '{1}' OR 
            referenceperiod = '{2}') AND referenceperiodyear = '{3}' AND entity_id = '{4}' 
            AND "type of complaint(s)" = 'Inaccurate information to scheme members' AND "time_added_to_db" = '{5}';
            '''.format(months[0], months[1], months[2], year, entityID, latestTimeStamp))

        count = cursor.fetchone()[0]

        KRI = 1 if count == 0 else 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 5, scheme, quarter, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, quarter, year, 5, onsite,
                             regulatory)

        logCSV(entityID, 5, quarter, year, 'Count', count)
    except Exception as e:
        printKRI_Error(5, scheme.getSchemeID(), e)


def calculateKRI6(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None

    Deals with dataset 0601 REFID-COMPLAINTS CASE REGISTER-Employment Status
    Counts complaints of type 59 years and above(upcoming retiree)
    AND counts type of complaint lack of communication
    Generates for every quarter, risk score = 1 if between 0 and 4, 2 if between 5 and 10, 3 if between 11 and 20,
    4 if bigger than 20
    Calculated per quarter
    """
    try:
        entityID = scheme.getSchemeID()
        quarter = getDatasetCurrentQuarter(stageDatabaseConnection, '0601', entityID)
        months = quarters[quarter]
        year = getDatasetCurrentYear(stageDatabaseConnection, '0601', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0601', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM "Dataset_0601" WHERE (referenceperiod = '{0}' OR referenceperiod = '{1}' 
            OR referenceperiod = '{2}') AND referenceperiodyear = '{3}' AND entity_id = '{4}' AND "type of complaint(s)" 
            = 'Lack of communication' AND "employment status" = '59 years and above(upcoming retiree)'
            AND "time_added_to_db" = '{5}';
            '''.format(months[0], months[1], months[2], year, entityID, latestTimeStamp))

        count = cursor.fetchone()[0]

        KRI = 1 if count <= 4 else 2 if count <= 10 else 3 if count <= 20 else 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 6, scheme, quarter, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, quarter, year, 6, onsite,
                             regulatory)

        logCSV(entityID, 6, quarter, year, 'Count', count)
    except Exception as e:
        printKRI_Error(6, scheme.getSchemeID(), e)


def calculateKRI7(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None
    from dataset 0601 COMPLAINTS CASE REGISTER
    Check by rowID from previous month to current quarter if it is repeated
    Calculated per quarter
    """
    try:
        entityID = scheme.getSchemeID()
        quarter = getDatasetCurrentQuarter(stageDatabaseConnection, '0601', entityID)
        months = quarters[quarter]
        year = getDatasetCurrentYear(stageDatabaseConnection, '0601', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0601', entityID)

        cursor = stageDatabaseConnection.cursor()

        # Count for first month:
        cursor.execute(
            '''
            SELECT "compliant id_number" FROM "Dataset_0601" WHERE referenceperiod = '{0}' AND referenceperiodyear = '{1}' 
            AND entity_id = '{2}' AND "time_added_to_db" = '{3}'
            '''.format(months[0], year, entityID, latestTimeStamp)
        )
        month1 = cursor.fetchall()
        # Count for second month:
        cursor.execute(
            '''
            SELECT "compliant id_number" FROM "Dataset_0601" WHERE referenceperiod = '{0}' AND referenceperiodyear = '{1}' 
            AND entity_id = '{2}' AND "time_added_to_db" = '{3}'
            '''.format(months[1], year, entityID, latestTimeStamp)
        )
        month2 = cursor.fetchall()
        # Count for third month:
        cursor.execute(
            '''
            SELECT "compliant id_number" FROM "Dataset_0601" WHERE referenceperiod = '{0}' AND referenceperiodyear = '{1}' 
            AND entity_id = '{2}' AND "time_added_to_db" = '{3}'
            '''.format(months[2], year, entityID, latestTimeStamp)
        )
        month3 = cursor.fetchall()
        # Find intersections between each month:
        intersection1 = [x for x in month1 if x in month2]
        intersection2 = [x for x in month2 if x in month3]

        count = len(intersection1 + intersection2)

        KRI = 1 if count == 0 else 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 7, scheme, quarter, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, quarter, year, 7, onsite,
                             regulatory)

        logCSV(entityID, 7, quarter, year, 'Count', count)
    except Exception as e:
        printKRI_Error(7, scheme.getSchemeID(), e)


def calculateKRI8(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None

    from dataset 0301 INVESTMENT DETAILS get net return
    from dataset 0603 NPRA Benchmark get GFIM
    calculate (net return - GFIM)/GFIM * 100 = x (percentage)
    x = abs(x)
    KRI = 1 if x<30%, 2 if 30%<x<60%, 3 if 60%<x<80%, 4 if x>80%

    Calculated per month
    """
    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0301', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "total portfolio return (net return)" FROM "Dataset_0301" WHERE referenceperiod = '{0}' AND 
            referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        netReturn = cursor.fetchone()[0]

        cursor.execute('''
            SELECT "gfim" FROM "Dataset_0603" WHERE referenceperiod = '{0}' AND referenceperiodyear = '{1}' AND 
            entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        GFIM = cursor.fetchone()[0]

        x = abs((netReturn - GFIM) / GFIM * 100)

        KRI = 1 if x < 30 else 2 if x < 60 else 3 if x < 80 else 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 8, scheme, month, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, month, year, 8, onsite,
                             regulatory)

        logCSV(entityID, 8, month, year, 'NetReturn', netReturn)
        logCSV(entityID, 8, month, year, 'GFIM', GFIM)
        logCSV(entityID, 8, month, year, 'x', x)

    except Exception as e:
        printKRI_Error(8, scheme.getSchemeID(), e)


def calculateKRI9(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None
    from dataset 0303 DAILY NET ASSET VALUE & NET ASSET BASED FEES
    Unit Price
    Compare each day to the day after, if the price changed at all, KRI = 4, else KRI = 1
    Calculate per quarter
    """
    try:
        entityID = scheme.getSchemeID()
        quarter = getDatasetCurrentQuarter(stageDatabaseConnection, '0303', entityID)
        months = quarters[quarter]
        year = getDatasetCurrentYear(stageDatabaseConnection, '0303', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0303', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "unit price" FROM "Dataset_0303" WHERE (referenceperiod = '{0}' OR referenceperiod = '{1}' OR 
            referenceperiod = '{2}') AND referenceperiodyear = '{3}' AND entity_id = '{4}' AND "time_added_to_db" = '{5}';
            '''.format(months[0], months[1], months[2], year, entityID, latestTimeStamp))

        unitPriceList = cursor.fetchall()

        KRI = 4 if unitPriceList[0][0] != unitPriceList[1][0] else 1

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 9, scheme, quarter, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, quarter, year, 9, onsite,
                             regulatory)

        logCSV(entityID, 9, quarter, year, 'UnitPriceList', unitPriceList)

    except Exception as e:
        printKRI_Error(9, scheme.getSchemeID(), e)


def calculateKRI10(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None

    Deals with dataset 0301 INVESTMENT DETAILS
    x1 = GOVERNMENT SECURITIES/Total Assets Under Management
    x2 = Local Government Bonds/Total Assets Under Management
    x3 = Corporate Debt Securities/Total Assets Under Management
    x4 = Bank Securities/Total Assets Under Management
    x5 = Ordinary Shares/Total Assets Under Management
    x6 = Collective Investment Schemes/Total Assets Under Management
    x7 = Alternative Investments/Total Assets Under Management

    kri1 = 0 if x1 <= 0.75, else 1
    kri2 = 0 if x2 <= 0.25, else 1
    kri3 = 0 if x3 <= 0.35, else 1
    kri4 = 0 if x4 <= 0.35, else 1
    kri5 = 0 if x5 <= 0.20, else 1
    kri6 = 0 if x6 <= 0.15, else 1
    kri7 = 0 if x7 <= 0.25, else 1

    KRI = kri1 + kri2 + kri3 + kri4 + kri5 + kri6 + kri7
    KRI = 1 if KRI==0, else 4
    Calculated monthly
    """
    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0301', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "government securities", "local government / statutory agency securities", "corporate debt securities", 
            "bank securities", "ordinary shares / preference shares", "collective investment scheme", 
            "alternative investments", "total assets under management" FROM 
            "Dataset_0301" WHERE referenceperiod = '{0}' AND referenceperiodyear = '{1}' AND entity_id = '{2}'
            AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        result = cursor.fetchone()

        x1 = result[0] / result[7]
        x2 = result[1] / result[7]
        x3 = result[2] / result[7]
        x4 = result[3] / result[7]
        x5 = result[4] / result[7]
        x6 = result[5] / result[7]
        x7 = result[6] / result[7]

        kri1 = 0 if x1 <= 0.75 else 1
        kri2 = 0 if x2 <= 0.25 else 1
        kri3 = 0 if x3 <= 0.35 else 1
        kri4 = 0 if x4 <= 0.35 else 1
        kri5 = 0 if x5 <= 0.20 else 1
        kri6 = 0 if x6 <= 0.15 else 1
        kri7 = 0 if x7 <= 0.25 else 1

        KRI = kri1 + kri2 + kri3 + kri4 + kri5 + kri6 + kri7
        KRI = 1 if KRI == 0 else 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 10, scheme, month, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, month, year, 10, onsite,
                             regulatory)

        logCSV(entityID, 10, month, year, 'x1', x1)

    except Exception as e:
        printKRI_Error(10, scheme.getSchemeID(), e)


def calculateKRI11(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None
    From dataset 0302 INVESTMENT REPORT
    x = Amount Impaired (GHS)/Total Assets Under Management TODO: Check if this is correct as it is not in the dataset
    if less than 0.1 KRI = 1, if between 0.1 and 0.2 KRI = 2, if between 0.2 and 0.3 KRI = 3, if bigger than 0.3 KRI = 4
    Calculated per month
    """
    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0302', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0302', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0302', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "amount impaired (ghs)", "total assets" FROM "Dataset_0302" WHERE referenceperiod = '{0}' AND 
            referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        result = cursor.fetchone()

        x = result[0] / result[1]
        KRI = 1 if x < 0.1 else 2 if x < 0.2 else 3 if x < 0.3 else 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 11, scheme, month, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, month, year, 11, onsite,
                             regulatory)

        logCSV(entityID, 11, month, year, 'x', x)

    except Exception as e:
        printKRI_Error(11, scheme.getSchemeID(), e)


def calculateKRI12(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None

    from dataset 0301 INVESTMENT DETAILS
    Investment Receivables/Total Assets Under Management * 100 = x
    KRI = 1 if less than 1% else 2 if less than 2% else 3 if less than 3% else 4 if more than 3%
    """
    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0301', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "investment receivables", "total assets under management" FROM "Dataset_0301" WHERE referenceperiod 
            = '{0}' AND referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        result = cursor.fetchone()

        x = result[0] / result[1] * 100
        KRI = 1 if x < 1 else 2 if x < 2 else 3 if x < 3 else 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 12, scheme, month, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, month, year, 12, onsite,
                             regulatory)

        logCSV(entityID, 12, month, year, 'x', x)

    except Exception as e:
        printKRI_Error(12, scheme.getSchemeID(), e)


def calculateKRI13(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None

    For tier 2 schemes:

    from dataset 0302 Investment Report
    if Maturity Date minus reporting date<=365 days, sum all values and divide by
    from dataset 0205 Projected Benefits Payout
    Total Projected Benefits payout within a year = x

    If x > or = 0.2 score 4
    If x < 0.2 score 1

    Every month, for every tier 3 scheme:
    If x >= 0.5 score 4
    If x < 0.5 score 1

    Calculated every month for tier 2 schemes only

    """

    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0302', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0302', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0302', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "maturity date", "reporting date" FROM "Dataset_0302" WHERE referenceperiod = '{0}' AND 
            referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        maturityDate = cursor.fetchone()[0]
        reportingDate = cursor.fetchone()[1]
        if maturityDate - reportingDate <= 365:
            cursor.execute('''
                SELECT SUM("yearly projected withdrawals") FROM "Dataset_0205" WHERE referenceperiod = '{0}' AND 
                referenceperiodyear = '{1}' AND entity_id = '{2}';
                '''.format(month, year, entityID))
            result = cursor.fetchone()

            KRI = None
            if scheme.tier_type == 2:
                x = result[0] / result[1]
                KRI = 4 if x >= 0.2 else 1
            elif scheme.tier_type == 3:
                x = result[0] / result[1]
                KRI = 4 if x >= 0.5 else 1

            if KRI:
                onsite, regulatory = getMitigationData(stageDatabaseConnection, 13, scheme, month, year)
                insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, month, year, 13,
                                     onsite, regulatory)

        logCSV(entityID, 13, month, year, 'x', x)
    except Exception as e:
        printKRI_Error(13, scheme.getSchemeID(), e)


def calculateKRI14(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None
    This KRI checks if the values submitted by the scheme, custodian and fund manager are the same
    from dataset 0301 INVESTMENT DETAILS
    field is total assets under management

    calculated per month
    """
    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0301', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "total assets under management" FROM "Dataset_0301" WHERE referenceperiod = '{0}' AND 
            referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        schemeSubmitted = cursor.fetchone()

        # Get custodian submitted value
        custodian = scheme.getCustodians()
        if custodian:
            custodian = custodian[0]
            custodianID = custodian.trustee_id()
            cursor.execute('''
                SELECT "total assets under management" FROM "Dataset_0301" WHERE referenceperiod = '{0}' AND 
                referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
                '''.format(month, year, custodianID, latestTimeStamp))

            custodianSubmitted = cursor.fetchone()
        else:
            custodianSubmitted = None

        # Get fund manager submitted value
        fundManager = scheme.getFundManagers()
        if fundManager:
            fundManager = fundManager[0]
            fundManagerID = fundManager.trustee_id()
            cursor.execute('''
                SELECT "total assets under management" FROM "Dataset_0301" WHERE referenceperiod = '{0}' AND 
                referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
                '''.format(month, year, fundManagerID, latestTimeStamp))

            fundManagerSubmitted = cursor.fetchone()
        else:
            fundManagerSubmitted = None

        # Check if values are the same
        KRI = 1
        if custodianSubmitted and fundManagerSubmitted:
            if custodianSubmitted[0] == schemeSubmitted[0] and fundManagerSubmitted[0] == schemeSubmitted[0]:
                KRI = 1
            else:
                KRI = 4
        elif custodianSubmitted:
            if custodianSubmitted[0] == schemeSubmitted[0]:
                KRI = 1
            else:
                KRI = 4
        elif fundManagerSubmitted:
            if fundManagerSubmitted[0] == schemeSubmitted[0]:
                KRI = 1
            else:
                KRI = 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 14, scheme, month, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, month, year, 14, onsite,
                             regulatory)

        logCSV(entityID, 14, month, year, 'scheme', schemeSubmitted[0])
    except Exception as e:
        printKRI_Error(14, scheme.getSchemeID(), e)


def calculateKRI15(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection:
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None
    dataset 0302 INVESTMENT REPORT
    Every month, for the investment report submitted by the schemes, the accrued interest reported is checked with:
    x = Face Value (GHS) * (Coupon Rate (%) / 364) * t

    t = max(30.33, Reporting Date - Date of Investment)
    Y = Accrued Paid Interest/Coupon for the month
    Z = absolute value (X minus Y) divided by 364

    Where Z = 0, give a risk score of 1
    Where 0 < Z <= 0.0027, give a risk score of 2
    Where Z > 0.0027, give a risk score of 4

    Calculated per month
    """
    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0302', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0302', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0302', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "face value  (ghs)", "coupon rate percent" FROM "Dataset_0302" WHERE referenceperiod = '{0}' 
            AND referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))
        faceValue = cursor.fetchone()[0]
        couponRate = cursor.fetchone()[1]

        cursor.execute('''
            SELECT "reporting date", "date of investment" FROM "Dataset_0302" WHERE referenceperiod = '{0}' AND 
            referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        reportingDate = cursor.fetchone()[0]
        dateOfInvestment = cursor.fetchone()[1]
        t = max(30.33, reportingDate - dateOfInvestment)

        x = faceValue * (couponRate / 364) * t

        cursor.execute('''
            SELECT "accrued interest / coupon for the month" FROM "Dataset_0302" WHERE referenceperiod = '{0}' AND 
            referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        y = cursor.fetchone()[0]
        z = abs(x - y) / 364
        KRI = 1
        if 0 < z <= 0.0027:
            KRI = 2
        elif z > 0.0027:
            KRI = 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 15, scheme, month, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, month, year, 15, onsite,
                             regulatory)

        logCSV(entityID, 15, month, year, 'scheme', z)
    except Exception as e:
        printKRI_Error(15, scheme.getSchemeID(), e)


def calculateKRI16(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None

    from dataset 0303 Daily Net Asset Value & Net Asset Based Fees
    x1 = NPRA Fees
    x2 = Trustee Fees
    x3 = Fund Manager Fees
    x4 = Custodian Fees

    score1 = 0 if less than or equal to 0.0033 else 1
    score2 = 0 if less than or equal 0.013 else 1
    score3 = 0 if less than or equal 0.0050 else 1
    score4 = 0 if less than or equal 0.0028 else 1

    KRI = score1 + score2 + score3 + score4
    KRI = 1 if KRI==0, else 4

    Calculated per month
    """
    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0303', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0303', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0303', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "npra fees", "trustee fees", "fund manager fees", "fund custodian fees" FROM "Dataset_0303" WHERE 
            referenceperiod = '{0}' AND referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))
        x1 = cursor.fetchone()[0]
        x2 = cursor.fetchone()[1]
        x3 = cursor.fetchone()[2]
        x4 = cursor.fetchone()[3]

        score1 = 0 if x1 <= 0.0033 else 1
        score2 = 0 if x2 <= 0.013 else 1
        score3 = 0 if x3 <= 0.0050 else 1
        score4 = 0 if x4 <= 0.0028 else 1

        score = score1 + score2 + score3 + score4

        KRI = 1 if score == 0 else 4

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 16, scheme, month, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, month, year, 16, onsite,
                             regulatory)

        logCSV(entityID, 16, month, year, 'scheme', score)
    except Exception as e:
        printKRI_Error(16, scheme.getSchemeID(), e)


def calculateKRI17(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO: Check which time with which time
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None
    from dataset 0406 Suspense Account Report
    x = TOTAL_BALANCE_AS_AT_REPORTING_DATE (t) -
    TOTAL_BALANCE_AS_AT_REPORTING_DATE (t-1) ) / TOTAL_BALANCE_AS_AT_REPORTING_DATE (t-1)) * 100

    KRI = 1 if x = 0 and TOTAL_BALANCE_AS_AT_REPORTING_DATE (t-1)=0 else 3 if x=0 else 4
    Calculated per month
    """
    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0406', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0406', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0406', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "total_balance_as_at_reporting_date" FROM "Dataset_0406" WHERE referenceperiod = '{0}' AND 
            referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        x = cursor.fetchone()[0]


    except Exception as e:
        printKRI_Error(17, scheme.getSchemeID(), e)


def calculateKRI18(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None
    from dataset 0201 EMPLOYEE PORTABILITY
    x = Date of Receipt of Accrued Benefits - Date of Allocation of Accrued Benefits Received
    if x <= 5 days KRI = 1 else KRI = 4
    Every Quarter Check for every transfer during that quarter
    """
    try:
        entityID = scheme.getSchemeID()
        quarter = getDatasetCurrentQuarter(stageDatabaseConnection, '0201', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0201', entityID)
        months = quarters[quarter]
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0201', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "date of receipt of accrued benefits", "date of allocation of accrued benefits received" FROM 
            "Dataset_0201" WHERE (referenceperiod = '{0}' OR referenceperiod = '{1}' OR referenceperiod = '{2}') AND 
            referenceperiodyear = '{3}' AND entity_id = '{4}' AND "time_added_to_db" = '{5}';
            '''.format(months[0], months[1], months[2], year, entityID, latestTimeStamp))

        rows = cursor.fetchall()
        KRI = 1
        for row in rows:
            x = row[0] - row[1]
            if x > 5:
                KRI = 4
                break

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 18, scheme, quarter, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, quarter, year, 18, onsite,
                             regulatory)

        logCSV(entityID, 18, quarter, year, 'data', rows)

    except Exception as e:
        printKRI_Error(18, scheme.getSchemeID(), e)


def calculateKRI19(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None
    from dataset 0405 MONTHLY CONTRIBUTION DATA
    Checking is done at the month end for every individual
    Inner joining should be done to extract only data that are in both months
    The Ghana Card ID/SSNIT numbers in both data should be used in this comparison

    X =
    Regulation 58 Monthly Accrued Benefits Data-Value(D)= (A+B+C) [CURRENT MONTH REPORT]
    divided
    Regulation 58 Monthly Accrued Benefits Data-Value (D)= (A+B+C)  [PREVIOUS MONTH REPORT]

    If (x) not equal to 1; give a score of 1
    If (x)= 1; give a score of 4
    Calculated per month
    """
    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0404', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0404', entityID)
        prevMonth, prevYear = getPreviousMonth(month, year)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0404', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "contribution (a)", "gain (b)","withdrawal (c)" FROM "Dataset_0405" WHERE referenceperiod = '{0}' 
            AND referenceperiodyear = '{1}' AND entity_id = '{2}' INNER JOIN "Dataset_0405" WHERE 
            referenceperiod = '{3}' AND referenceperiodyear = '{4}' AND entity_id = '{2}' AND "time_added_to_db" = '{5}'
            ON "Dataset_0405"."ghana_card_id" = "Dataset_0405"."ghana_card_id" AND 
            "Dataset_0405"."ssnit_employer_enrolment_number" = "Dataset_0405"."ssnit_employer_enrolment_number";
            '''.format(month, year, entityID, prevMonth, prevYear, latestTimeStamp))

        rows = cursor.fetchall()
        KRI = 1
        for row in rows:
            x = (row[0] + row[1] + row[2]) / (row[0] + row[1] + row[2])
            if x != 1:
                KRI = 4
                break

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 19, scheme, month, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, month, year, 19, onsite,
                             regulatory)

        logCSV(entityID, 19, month, year, 'data', rows)
    except Exception as e:
        printKRI_Error(19, scheme.getSchemeID(), e)


def calculateKRI20(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None
    from dataset 0204 BENEFITS WITHDRAWAL SCHEDULE
    x = Date of request - Date of payment
    if any x is bigger than 60, if yes, KRI=4, else KRI=1
    Calculate for quarter
    """
    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0204', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0204', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0204', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "date of request", "date of payment" FROM "Dataset_0204" WHERE referenceperiod = '{0}' AND 
            referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))
        rows = cursor.fetchall()
        KRI = 1
        for row in rows:
            if subtractDates(row[0], row[1]) > 60:
                KRI = 4
                break

        onsite, regulatory = getMitigationData(stageDatabaseConnection, 20, scheme, month, year)
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, month, year, 20, onsite,
                             regulatory)

        logCSV(entityID, 20, month, year, 'data', rows)
    except Exception as e:
        printKRI_Error(20, scheme.getSchemeID(), e)


def calculateKRI21(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO: KRI 21: Check the datasets
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None
    from dataset 0204 BENEFITS WITHDRAWAL SCHEDULE
    x = Beneficiary Ghana Card ID No.

    from dataset 0401 Employees Beneficiaries
    y = Ghana Card Number

    from dataset 0402 Employee Details
    z = ID number

    if x is a subset of y or z=0, KRI=1
    if x is not a subset of y or z=1, KRI=4

    Calculated quarterly
    """
    try:
        entityID = scheme.getSchemeID()
        month = getDatasetCurrentQuarter(stageDatabaseConnection, '0204', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0204', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0204', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "beneficiary ghana card id no." FROM "Dataset_0204" WHERE referenceperiod = '{0}' AND 
            referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        ghanaCardNumbers = cursor.fetchall()

        cursor.execute('''
            SELECT "ghana card number" FROM "Dataset_0401" WHERE referenceperiod = '{0}' AND referenceperiodyear = 
            '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

    except Exception as e:
        printKRI_Error(21, scheme.getSchemeID(), e)


def calculateKRI22(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO: KRI 22 - Check datetime format
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return:
    from dataset 0205 REFID-UNCLAIMED BENEFIT
    Current Value of Accrued Benefits - the previous day
    divided by
    from dataset 0301 REFID-INVESTMENT DETAILS Total Assets Under Management
    if less than or equal 0.01, KRI=1 else KRI=4
    Calculated per quarter
    """
    try:

        entityID = scheme.getSchemeID()
        quarter = getDatasetCurrentQuarter(stageDatabaseConnection, '0205', entityID)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0205', entityID)
        months = quarters[quarter]
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0205', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "current value of accrued benefits" FROM "Dataset_0205" WHERE referenceperiod = '{0}' AND 
            referenceperiodyear = '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(months[0], year, entityID, latestTimeStamp))

    except Exception as e:
        printKRI_Error(22, scheme.getSchemeID(), e)


def calculateKRI23(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return:

    Deals with dataset 0403
    count missing data fields in the table
    Deals with dataset 0402
    count missing data fields in the table

    x = total missing data fields in 0402 and 0403
    KRI = 1  if x is equal to 0 else KRI = 2 if x is between 0 and 4 else KRI = 3 if x is between 5 and 9 else KRI = 4

    Calculated Annually
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0403', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0403', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT * FROM "Dataset_0403" WHERE referenceperiodyear = '{0}' AND entity_id = '{1}' AND
            "time_added_to_db" = '{2}';
            '''.format(year, entityID, latestTimeStamp))

        rows = cursor.fetchall()
        missingData = 0
        for row in rows:
            missingData += row.count(None)

        cursor.execute('''
            SELECT * FROM "Dataset_0402" WHERE referenceperiodyear = '{0}' AND entity_id = '{1}' AND
            "time_added_to_db" = '{2}';
            '''.format(year, entityID, latestTimeStamp))

        rows = cursor.fetchall()
        for row in rows:
            missingData += row.count(None)

        KRI = 1
        if 0 < missingData <= 4:
            KRI = 2
        elif 4 < missingData <= 9:
            KRI = 3
        elif missingData > 9:
            KRI = 4

        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, 'Annual', year, 23)

    except Exception as e:
        printKRI_Error(23, scheme.getSchemeID(), e)


def calculateKRI24(trustee, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO: Check time added to DB format
    :param trustee: trustee object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None

    from dataset 0503 VULNERABILITY TEST REPORT

    A= NETWORK SCANNING_SERVICE DISRUPTION
    plus  NETWORK SCANNING_FRAUD
    plus NETWORK SCANNING_CYBER BULLYING
    plus SCANNING_MALICIOUS CODE

    where
    B= CLIENT SERVICE SCANNING_SERVICE DISRUPTION
    plus CLIENT SERVICE SCANNING_FRAUD
    plus CLIENT SERVICE SCANNING_CYBER BULLYING
    plus CLIENT SERVICE SCANNING_MALICIOUS CODE

    where
    C= SYSTEM SCANNING_SERVICE DISRUPTION
    plus SYSTEM SCANNING_FRAUD
    plus SYSTEM SCANNING_CYBER BULLYING
    plus SYSTEM SCANNING_MALICIOUS CODE

    where
    D= SERVER SCANNING_SERVICE DISRUPTION
    plus SERVER SCANNING_FRAUD
    plus SERVER SCANNING_CYBER BULLYING
    plus SERVER SCANNING_MALICIOUS CODE

    x = A + B + C + D
    KRI = 1 if X is between 0 and 3 else 2 if x is between 4 and 8 else 3 if x is between 9 and 12 else 4

    Calculated monthly
    """
    try:
        trusteeId = trustee.getTrusteeID()
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0503', trusteeId)
        year = getDatasetCurrentYear(stageDatabaseConnection, '0503', trusteeId)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0503', trusteeId)

        cursor = stageDatabaseConnection.cursor()
        count = 0
        try:

            cursor.execute('''
                SELECT "network scanning_service disruption", "network scanning_fraud", 
                "network scanning_cyber bullying", "network scanning_malicious code" FROM "Dataset_0503" WHERE 
                referenceperiod = '{0}' AND referenceperiodyear = '{1}' AND entity_id = '{2}'
                AND "time_added_to_db" = '{3}';
                '''.format(month, year, trusteeId, latestTimeStamp))

            rows = cursor.fetchall()
            for row in rows:
                for i in range(1, len(row)):
                    if row[i] is not None:
                        count += 1
        except IndexError:
            pass

        try:
            cursor.execute('''
                SELECT "client service scanning_service disruption", "client service scanning_fraud", 
                "client service scanning_cyber bullying", "client service scanning_malicious code" 
                FROM "Dataset_0503" WHERE referenceperiod = '{0}' AND referenceperiodyear = '{1}' AND entity_id = '{2}';
                '''.format(month, year, trusteeId))

            rows = cursor.fetchall()
            for row in rows:
                for i in range(1, len(row)):
                    if row[i] is not None:
                        count += 1
        except IndexError:
            pass

        try:
            cursor.execute('''
                SELECT "system scanning_servcie disruption", "system scanning_fraud", "system scanning_cyber bullying", 
                "system scanning_malicious code" FROM "Dataset_0503" WHERE referenceperiod = '{0}' AND 
                referenceperiodyear = '{1}' AND entity_id = '{2}';
                '''.format(month, year, trusteeId))

            rows = cursor.fetchall()
            for row in rows:
                for i in range(1, len(row)):
                    if row[i] is not None:
                        count += 1
        except IndexError:
            pass

        try:
            cursor.execute('''
                SELECT "server scanning_service disruption", "server scanning_fraud", "server scanning_cyber bullying", 
                "server scanning_malicious code" FROM "Dataset_0503" WHERE referenceperiod = '{0}' AND 
                referenceperiodyear = '{1}' AND entity_id = '{2}';
                '''.format(month, year, trusteeId))

            rows = cursor.fetchall()
            for row in rows:
                for i in range(1, len(row)):
                    if row[i] is not None:
                        count += 1
        except IndexError:
            pass

        KRI = 1 if 0 < count <= 3 else 2 if 3 < count <= 8 else 3 if 8 < count <= 12 else 4

        insertKRIValueTrustee(businessDatabaseConnection, trustee, Calc_ID, KRI, month, year, 24, None, None)

    except Exception as e:
        printKRI_Error(24, trustee.getTrusteeID(), e)


def calculateKRI25(trustee, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param trustee: trustee object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: int
    :return: None

    from dataset 0504 BUSINESS CONTINUITY PLAN
    X = Availability of BCP
    Y = Testing of BCP
    Z = REMEDIATION/MITIGATION of BCP
    Note: if X is NO, Y and Z should automatically be 2
    NB: If No is selected for X , Y and Z should automatically be 2 and 2 respectively
    if
    X=YES; 1
    X=NO; 2 (If X=NO, Y and Z should also be NO)
    Y=YES;1
    Y=NO;2
    Z=NO MITIGATION NEEDED; 0
    Z=YES; 1
    Z=NO;2

    A=X+Y+Z
    KRI = 1 if A==2 else 2 if A==3 else 3 if 4<A<5 else 4 if A==6 else 0
    calculated Annually
    """
    try:
        trusteeId = trustee.getTrusteeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0504', trusteeId)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0504', trusteeId)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "availability of a bcp", "testing of bcp", "remediation/mitigation" FROM "Dataset_0504" WHERE 
            referenceperiodyear = '{0}' AND entity_id = '{1}' AND "time_added_to_db" = {2};
            '''.format(year, trusteeId, latestTimeStamp))

        rows = cursor.fetchall()
        row = rows[0]
        x, y, z = row[0], row[1], row[2]
        if x == 'NO':
            x, y, z = 2, 2, 2
        else:
            x = 1
            if y == 'NO':
                y = 2
            else:
                y = 1
            if z == 'NO MITIGATION NEEDED':
                z = 0
            elif z == 'YES':
                z = 1
            else:
                z = 2

        A = x + y + z

        KRI = 1 if A == 2 else 2 if A == 3 else 3 if 4 < A < 5 else 4 if A == 6 else 4
        insertKRIValueTrustee(businessDatabaseConnection, trustee, Calc_ID, KRI, 'Annual', year, 25, None, None)

    except Exception as e:
        printKRI_Error(25, trustee.getTrusteeID(), e)


def calculateKRI26(trustee, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO: KRI 26: Check balance sheet dataset
    :param trustee: trustee object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: KRI ID
    :return: None

    from dataset
    """


def calculateKRI27(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO: KRI 27 check the dataset
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: KRI ID
    :return: None
    from dataset 0605 Reports Submission
    x = submission status
    KRI = 1 if x = 0 else 2 if x = 1 else 4
    calculated monthly
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0302', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0302', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0302', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT "status" FROM "Dataset_0605" WHERE referenceperiod = '{0}' AND referenceperiodyear = '{1}' 
            AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

    except Exception as e:
        printKRI_Error(27, scheme.getSchemeID(), e)


def calculateKRI28(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: KRI ID
    :return: None
    from dataset 0604 SANCTIONS REGISTER
    x = count of entityID
    If x = 0 give a score 1
    If 1 < x < 5 give a score 3
    If x > or = 5 give a score 4
    Calculated monthly
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0604', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0604', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0604', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM "Dataset_0604" WHERE referenceperiod = '{0}' AND referenceperiodyear = '{1}' AND 
            entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        rows = cursor.fetchall()
        count = rows[0][0]

        KRI = 1 if count == 0 else 3 if 1 < count < 5 else 4 if count >= 5 else 0
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, year, month, 28, None, None)

    except Exception as e:
        printKRI_Error(28, scheme.getSchemeID(), e)


def calculateKRI29(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    from dataset 0601 COMPLAINTS CASE REGISTER
    a = count of entityID

    from dataset 0403 Employer Data
    b = number of contributors
    x = a/b * 100
    KRI = 1 if x = 0 else 3 if x is between 0 and 10 else 4
    calculated monthly
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0601', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0601', entityID)
        latestTimeStamp = getLatestTimeStamp(stageDatabaseConnection, '0601', entityID)

        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM "Dataset_0601" WHERE referenceperiod = '{0}' AND referenceperiodyear = '{1}' AND 
            entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        rows = cursor.fetchall()
        a = rows[0][0]

        cursor.execute('''
            SELECT "number of contributors" FROM "Dataset_0403" WHERE referenceperiod = '{0}' AND referenceperiodyear = 
            '{1}' AND entity_id = '{2}' AND "time_added_to_db" = '{3}';
            '''.format(month, year, entityID, latestTimeStamp))

        rows = cursor.fetchall()
        b = rows[0][0]

        x = a / b * 100

        KRI = 1 if x == 0 else 3 if 0 < x < 10 else 4 if x >= 10 else 0
        insertKRIValueScheme(businessDatabaseConnection, scheme.scheme_id, Calc_ID, KRI, 0, year, 24)

    except Exception as e:
        printKRI_Error(29, scheme.getSchemeID(), e)


# KRI main functions

def newCalculationRow(businessDatabaseConnection):
    """
    This function creates a new row in the KRI table in the business database
    :param businessDatabaseConnection: connection object
    :return:
    """
    cursor = businessDatabaseConnection.cursor()
    cursor.execute('''
        INSERT INTO calculation VALUES (DEFAULT) RETURNING calculation_id;
        ''')
    calcID = cursor.fetchone()[0]
    businessDatabaseConnection.commit()
    return calcID


def calculateKRIValues(schemesList, trusteesList, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    This function calculates the KRI values for all schemes and trustees
    :param schemesList: list of scheme objects
    :param trusteesList: list of trustee objects
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: Calculation ID
    :return: None
    """

    for scheme in schemesList:
        calculateKRIValueScheme(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    for trustee in trusteesList:
        calculateKRIValueTrustee(trustee, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)


def calculateKRIValueScheme(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme: scheme object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: Calculation ID
    :return: None
    """
    calculateKRI1(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI2(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI3(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI4(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI5(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI6(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI7(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI8(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI9(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI10(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI11(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI12(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI13(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI14(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI15(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI16(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI17(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI18(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI19(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI20(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI21(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI22(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI23(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI27(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI28(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI29(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)


def calculateKRIValueTrustee(trustee, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param trustee: trustee object
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: Calculation ID
    :return: None
    """
    calculateKRI24(trustee, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI25(trustee, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateKRI26(trustee, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)


def calculateStatValues(schemesList, trusteesList, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    This function calculates the KRI values for all schemes and trustees
    :param schemesList: list of scheme objects
    :param trusteesList: list of trustee objects
    :param businessDatabaseConnection: connection object
    :param stageDatabaseConnection: connection object
    :param Calc_ID: Calculation ID
    :return: None
    """

    for scheme in schemesList:
        calculateStatValueScheme(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)


def calculateStatValueScheme(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    calculateStat1(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat2(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat3(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat4(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat5(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat6(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat7(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat8(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat9(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    # The next 4 stats are not calculated as they are no longer necessary
    # calculateStat10(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    # calculateStat11(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    # calculateStat12(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    # calculateStat13(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat14(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat15(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat16(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat17(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat18(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat19(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat20(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat21(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat22(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat23(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat24(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat25(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat26(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat27(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat28(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat29(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat30(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat31(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat32(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat33(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat34(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat35(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat36(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat37(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat38(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStat39(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)


def calculateStat1(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    SUM for all related schemes (REFID-INVESTMENT DETAILS-Total Assets Under Management)

    """

    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0302', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0302', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("total assets under management") FROM "Dataset_0301" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))
        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 1)
    except Exception as e:
        print(e, "Failed to calculate Stat1")


def calculateStat2(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    SUM for all related schemes (REFID-MEMBERSHIP STATISTICS-Number of Active Members)

    """

    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0208', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0208', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("number of active members") FROM "Dataset_0208" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))
        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 2)
    except Exception as e:
        print(e, "Failed to calculate Stat2")


def calculateStat3(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-Regulation 58 - Monthly Contribution_REGULAR-CONTRIBUTION AMOUNT by scheme
    filter by ReferencePeriodYear and ReferencePeriod"
    """

    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0404', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0404', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("contribution amount") FROM "Dataset_0404" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))
        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 3)
    except Exception as e:
        print(e, "Failed to calculate Stat3")


def calculateStat4(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    "Sum REFID-Regulation 58 Monthly Accrued Benefits Data-Value (D) =(A+B-C) by scheme
    filter by ReferencePeriodYear and ReferencePeriod"
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0405', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0405', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("value (d) =(aplusb-c)") FROM "Dataset_0405" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))
        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 4)

    except Exception as e:
        print(e, "Failed to calculate Stat4")


def calculateStat5(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-Benefits Withdrawal Schedule-ACCRUED BENEFITS D = (AplusB) by REFID-Benefits Withdrawal Schedule-Type of
    Benefit and count for each type of benefit REFID-Benefits Withdrawal Schedule-Type of Benefit
    filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0204', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0204', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("accrued benefits d = (aplusb)") FROM "Dataset_0204" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 5)
    except Exception as e:
        print(e, "Failed to calculate Stat5")


def calculateStat6(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Count REFID-MEMBERSHIP STATISTICS-NUMBER OF EMPLOYERS AT THE END OF THE PERIOD filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0208', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0208', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT("number of employees at the end of the period") FROM "Dataset_0208" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 6)

    except Exception as e:
        print(e, "Failed to calculate Stat6")


def calculateStat7(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO: Check by Number of Active Members
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Count REFID-MEMBERSHIP STATISTICS-NUMBER OF EMPLOYEES AT THE END OF THE PERIOD by Scheme by NUMBER OF ACTIVE MEMBERS
    filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0208', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0208', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT("number of employees at the end of the period") FROM "Dataset_0208" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 7)

    except Exception as e:
        print(e, "Failed to calculate Stat7")


def calculateStat8(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Count REFID-MEMBERSHIP STATISTICS-NUMBER OF EMPLOYEES AT THE END OF THE PERIOD by Scheme by NUMBER OF DORMANT MEMBERS  filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0208', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0208', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT("number of employees at the end of the period") FROM "Dataset_0208" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 8)

    except Exception as e:
        print(e, "Failed to calculate Stat8")


def calculateStat9(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Count REFID-MEMBERSHIP STATISTICS-NUMBER OF EMPLOYEES AT THE END OF THE PERIOD by Scheme by NUMBER OF INACTIVE MEMBERS
    filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0208', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0208', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT("number of employees at the end of the period") FROM "Dataset_0208" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 9)

    except Exception as e:
        print(e, "Failed to calculate Stat9")


def calculateStat10(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Count REFID-MEMBERSHIP STATISTICS-NUMBER OF EMPLOYERS AT THE BEGINNING OF THE PERIOD by scheme by NUMBER OF ACTIVE MEMBERS filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0208', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0208', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT("number of employees at the beginning of the period") FROM "Dataset_0208" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 10)

    except Exception as e:
        print(e, "Failed to calculate Stat10")


def calculateStat11(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Count REFID-MEMBERSHIP STATISTICS-NUMBER OF EMPLOYERS AT THE BEGINNING OF THE PERIOD by Scheme by NUMBER OF DORMANT MEMBERS filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0208', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0208', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT("number of employees at the beginning of the period") FROM "Dataset_0208" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 11)

    except Exception as e:
        print(e, "Failed to calculate Stat11")


def calculateStat12(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """

    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Count REFID-MEMBERSHIP STATISTICS-NUMBER OF EMPLOYERS AT THE BEGINNING OF THE PERIOD Scheme by NUMBER OF INACTIVE MEMBERS filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0208', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0208', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT("number of employees at the beginning of the period") FROM "Dataset_0208" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 12)

    except Exception as e:
        print(e, "Failed to calculate Stat12")


def calculateStat13(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Count REFID-MEMBERSHIP STATISTICS-NUMBER OF EMPLOYEES AT THE BEGINNING OF THE PERIOD by NUMBER OF ACTIVE MEMBERS  filter by ReferencePeriodYear and ReferencePeriod
    """

    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0208', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0208', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT COUNT("number of employees at the beginning of the period") FROM "Dataset_0208" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 13)

    except Exception as e:
        print(e, "Failed to calculate Stat13")


def calculateStat14(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-Statement of Changes in Net Assets Available for Benefits-Net Investment Income (F=D-E) for each scheme filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0202', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0202', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("net investment income (f=d-e)") FROM "Dataset_0202" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 14)

    except Exception as e:
        print(e, "Failed to calculate Stat14")


def calculateStat15(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-Statement of Changes in Net Assets Available for Benefits-Brokerage Fees/Levies/Commissions by schemes filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0202', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0202', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("brokerage fees/levies/commissions (e)") FROM "Dataset_0202" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 15)

    except Exception as e:
        print(e, "Failed to calculate Stat15")


def calculateStat16(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-Statement of Changes in Net Assets Available for Benefits-Audit (H) by Scheme
    filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0202', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0202', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("audit fees (h)") FROM "Dataset_0202" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 16)

    except Exception as e:
        print(e, "Failed to calculate Stat16")


def calculateStat17(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-Statement of Changes in Net Assets Available for Benefits-NAB FEES -  scheme (J) by Scheme
    filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0202', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0202', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("nab fees - trustee (j)") FROM "Dataset_0202" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 17)

    except Exception as e:
        print(e, "Failed to calculate Stat17")


def calculateStat18(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:

    Sum REFID-Statement of Changes in Net Assets Available for Benefits-NAB FEES - Custodian (L) by Scheme filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0202', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0202', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("nab fees - custodian (l)") FROM "Dataset_0202" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 18)

    except Exception as e:
        print(e, "Failed to calculate Stat18")


def calculateStat19(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-Statement of Changes in Net Assets Available for Benefits-NAB FEES -  Fund Manager (K) by Scheme filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0202', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0202', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("nab fees - fund manager (k)") FROM "Dataset_0202" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 19)

    except Exception as e:
        print(e, "Failed to calculate Stat19")


def calculateStat20(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-Statement of Changes in Net Assets Available for Benefits-NAB FEES -  NPRA (I) by Scheme
    filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0202', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0202', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("nab fees -  npra (i)") FROM "Dataset_0202" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 20)

    except Exception as e:
        print(e, "Failed to calculate Stat20")


def calculateStat21(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-Statement of Changes in Net Assets Available for Benefits-Total Administrative Expenses (M=HplusIplusJplusKplusLplusQ) by Scheme filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0202', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0202', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("total administrative expenses (m=hplusiplusjpluskpluslplusq)") FROM "Dataset_0202" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 21)

    except Exception as e:
        print(e, "Failed to calculate Stat21")


def calculateStat22(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum  REFID-Regulation 58 - Monthly Contribution_REGULAR-CONTRIBUTION RECEIVABLES by Scheme filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0404', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0404', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("contribution receivables") FROM "Dataset_0404" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 22)

    except Exception as e:
        print(e, "Failed to calculate Stat22")


def calculateStat23(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum  REFID-INVESTMENT DETAILS-Investment Receivables by Scheme filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("investment receivables") FROM "Dataset_0301" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 23)

    except Exception as e:
        print(e, "Failed to calculate Stat23")


def calculateStat24(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-INVESTMENT DETAILS-Bank Balances by Scheme filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("bank balances") FROM "Dataset_0301" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 24)

    except Exception as e:
        print(e, "Failed to calculate Stat24")


def calculateStat25(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-INVESTMENT DETAILS-Total Assets Under Management by Scheme by filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("total assets under management") FROM "Dataset_0301" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 25)

    except Exception as e:
        print(e, "Failed to calculate Stat25")


def calculateStat26(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-MONTHLY STATEMENT OF NET ASSETS AVAILABLE FOR BENEFITS-LIABILITIES - Benefits Payable (F) by Scheme filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0203', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0203', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("liabilities - benefits payable (f)") FROM "Dataset_0203" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 26)

    except Exception as e:
        print(e, "Failed to calculate Stat26")


def calculateStat27(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    TODO
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum schemes based on scheme type with their individual _REFID-MONTHLY STATEMENT OF NET ASSETS AVAILABLE FOR BENEFITS-LIABILITIES - Administrative Expenses Payable (G)
    filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0203', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0203', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("liabilities - administrative expenses payable (g)") FROM "Dataset_0203" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 27)

    except Exception as e:
        print(e, "Failed to calculate Stat27")


def calculateStat28(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-MONTHLY STATEMENT OF NET ASSETS AVAILABLE FOR BENEFITS-Total Liabilities (H=FplusG)by Scheme
    filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0203', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0203', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("total liabilities (h = f plus g)") FROM "Dataset_0203" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 28)

    except Exception as e:
        print(e, "Failed to calculate Stat28")


def calculateStat29(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Count REFID-EMPLOYEE PORTABILITY -GHANA CARD NO. by Scheme
    filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0201', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0201', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT count("ghana card no.") FROM "Dataset_0201" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 29)

    except Exception as e:
        print(e, "Failed to calculate Stat29")


def calculateStat30(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Average REFID-INVESTMENT REPORT-Total Portfolio Return (Net Return) by Scheme filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT AVG("total portfolio return (net return)") FROM "Dataset_0301" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 30)

    except Exception as e:
        print(e, "Failed to calculate Stat30")


def calculateStat31(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-INVESTMENT DETAILS-Alternative Investments by Scheme filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("alternative investments") FROM "Dataset_0301" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 31)

    except Exception as e:
        print(e, "Failed to calculate Stat31")


def calculateStat32(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-INVESTMENT DETAILS-Bank Securities by Scheme filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("bank securities") FROM "Dataset_0301" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 32)

    except Exception as e:
        print(e, "Failed to calculate Stat32")


def calculateStat33(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-INVESTMENT DETAILS-Government Securities by Scheme filter by ReferencePeriodYear and ReferencePeriod
    """

    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("government securities") FROM "Dataset_0301" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 33)

    except Exception as e:
        print(e, "Failed to calculate Stat33")


def calculateStat34(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-INVESTMENT DETAILS-Local Government / Statutory Agency Securities

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("local government / statutory agency securities") FROM "Dataset_0301" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 34)

    except Exception as e:
        print(e, "Failed to calculate Stat34")


def calculateStat35(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :return:
    sum REFID-INVESTMENT DETAILS-Corporate Debt Securities by Scheme filter by ReferencePeriodYear and ReferencePeriod


    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("corporate debt securities") FROM "Dataset_0301" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 35)

    except Exception as e:
        print(e, "Failed to calculate Stat35")


def calculateStat36(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-INVESTMENT DETAILS-Ordinary Shares / Preference Shares
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0301', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0301', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("ordinary shares / preference shares") FROM "Dataset_0301" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 36)

    except Exception as e:
        print(e, "Failed to calculate Stat36")


def calculateStat37(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-MONTHLY STATEMENT OF NET ASSETS AVAILABLE FOR BENEFITS-TOTAL NET ASSETS AVAILABLE FOR BENEFITS
    (L = J + Statement of Changes in Net Assets Available for Benefits-N) by Scheme
    filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0203', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0203', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("total net assets available for benefits  (l = j plus statement ") FROM "Dataset_0203" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 37)

    except Exception as e:
        print(e, "Failed to calculate Stat37")


def calculateStat38(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-Unclaimed Benefit-CURRENT VALUE OF ACCRUED BENEFITS by Scheme
    filter by ReferencePeriodYear and ReferencePeriod

    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0401', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0401', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("current value of accrued benefits") FROM "Dataset_0205" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 38)

    except Exception as e:
        print(e, "Failed to calculate Stat38")


def calculateStat39(scheme, businessDatabaseConnection, stageDatabaseConnection, Calc_ID):
    """
    :param scheme:
    :param businessDatabaseConnection:
    :param stageDatabaseConnection:
    :param Calc_ID:
    :return:
    Sum REFID-Regulation 58 Monthly Accrued Benefits Data- Value(D) =(AplusB-C) by scheme
    filter by ReferencePeriodYear and ReferencePeriod
    """
    try:
        entityID = scheme.getSchemeID()
        year = getDatasetCurrentYear(stageDatabaseConnection, '0405', entityID)
        month = getDatasetCurrentMonth(stageDatabaseConnection, '0405', entityID)
        cursor = stageDatabaseConnection.cursor()
        cursor.execute('''
            SELECT SUM("value (d) =(aplusb-c)") FROM "Dataset_0405" WHERE "entity_id" = %s AND referenceperiodyear = %s AND referenceperiod = %s;
            ''', (entityID, year, month))

        Sum = cursor.fetchone()[0]

        insertStatValue(businessDatabaseConnection, scheme, Calc_ID, Sum, year, month, 39)

    except Exception as e:
        print(e, "Failed to calculate Stat39")


# Main
def main():
    # Connect to the databases
    businessDatabaseConnection = connect(business_DatabaseInfo)
    stageDatabaseConnection = connect(stage_DatabaseInfo)
    partnerDatabaseConnection = connect(partner_DatabaseInfo)

    # Set database connections auto commit to true
    businessDatabaseConnection.autocommit = True
    stageDatabaseConnection.autocommit = True
    partnerDatabaseConnection.autocommit = True

    # Get the list of schemes and trustees from the partner database
    schemesList = getSchemesList(partnerDatabaseConnection)
    trusteesList = getTrusteesList(partnerDatabaseConnection)

    # Calculate the KRI values
    Calc_ID = newCalculationRow(businessDatabaseConnection)
    calculateKRIValues(schemesList, trusteesList, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)
    calculateStatValues(schemesList, trusteesList, businessDatabaseConnection, stageDatabaseConnection, Calc_ID)


if __name__ == '__main__':
    main()
