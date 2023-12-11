import psycopg2

from dotenv import load_dotenv
import os
load_dotenv()
# Settings

loginInfo = {'host': os.getenv('DB_SERVER'), 'user': os.getenv('DB_USER'), 'password': os.getenv('DB_PASSWORD'),
             'port': os.getenv('DB_PORT')}

business_DatabaseInfo = loginInfo.copy()
business_DatabaseInfo['DBName'] = os.getenv('DB_NAME_PENSION')

datamart_DatabaseInfo = loginInfo.copy()
datamart_DatabaseInfo['DBName'] = os.getenv('DB_NAME_KRI')

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

def createTable(database, tableName, tableColumns, primaryKey=None, foreignKey=None, foreignKeyTable=None):
    """
    This function creates a table in the database
    :param database: connection object
    :param tableName: string
    :param tableColumns: dictionary, keys are column names and values are data types.
    :param primaryKey: string
    :param foreignKey: string
    :param foreignKeyTable: string
    :return: None
    """
    try:
        cursor = database.cursor()

        statement = 'CREATE TABLE IF NOT EXISTS {} ('.format(tableName)
        for key in tableColumns.keys():
            if key == primaryKey:
                statement += '{} {} PRIMARY KEY,'.format(key, tableColumns[key])
            else:
                statement += '{} {},'.format(key, tableColumns[key])
        if foreignKey:
            statement += 'FOREIGN KEY ({}) REFERENCES {}({}),'.format(foreignKey, foreignKeyTable, foreignKey)

        statement = statement[:-1] + ');'

        cursor.execute(statement)
        database.commit()

    except Exception as e:
        print(e, "Error creating table {}".format(tableName))

def insertRows(database, tableName, rows):
    """
    This function inserts rows into a table
    :param database: connection object
    :param tableName: string
    :param rows: list of tuples
    :return: None
    """
    try:
        cursor = database.cursor()
        statement = 'INSERT INTO {} VALUES '.format(tableName)
        for row in rows:
            if row[0] == 'DEFAULT':
                line = '(DEFAULT,'
                for i in range(1, len(row)):
                    line += "'" + str(row[i]) + "'" + ','
                line = line[:-1] + '),'
                statement += line
            else:
                statement += str(row) + ','
        statement = statement[:-1] + ';'
        cursor.execute(statement)
        database.commit()
    except Exception as e:
        print(e, "Error inserting rows into table {}".format(tableName))

# Initialize tables
def initializeSchemeTable(database):
    """
    :param database: database connection object
    :return: None
    """
    tableColumns = {
        'SCHEME_ID': 'VARCHAR(10) PRIMARY KEY',
        'SCHEME_NAME': 'VARCHAR(100)',
        'SCHEME_TYPE_CODE_ID': 'VARCHAR(200)',
        'TRUSTEE_ID': 'VARCHAR(10)',
        'FC_ID': 'VARCHAR(10)'
    }
    createTable(database, 'SCHEME', tableColumns)


def initializeTrusteeTable(database):
    """
    This function initializes the trustee table
    :param database: connection object
    :return: None
    """

    tableColumns = {
        'TRUSTEE_ID': 'VARCHAR(10) PRIMARY KEY',
        'TRUSTEE_NAME': 'VARCHAR(100)'
    }
    createTable(database, 'TRUSTEE', tableColumns)


def initializeSchemeTypeTable(database):
    """
    :param database: connection object
    :return: None
    """

    tableColumns = {
        'ID': 'VARCHAR(10) PRIMARY KEY',
        'SCHEME_TYPE_CODE_ID': 'VARCHAR(50)',
        'SCHEME_TYPE_Weight': 'NUMERIC',
        'TIMESTAMP': 'timestamp default current_timestamp NOT NULL'
    }
    createTable(database, 'SCHEME_TYPE', tableColumns)


def initializeRiskCategoryTable(database):
    """
    :param database: connection object
    :return: None
    """

    tableColumns = {
        'RISK_CATEGORY_ID': 'VARCHAR(15) PRIMARY KEY',
        'RISK_CATEGORY_NAME': 'VARCHAR(200) NOT NULL',
        'RISK_CATEGORY_WEIGHT': 'NUMERIC NOT NULL',
        'TIMESTAMP': 'timestamp default current_timestamp NOT NULL'
    }
    createTable(database, 'RISK_CATEGORY', tableColumns)


def initializeRiskTable(database):
    """
    :param database: connection object
    :return: None
    """

    tableColumns = {
        'KRI_ID': 'VARCHAR(20) PRIMARY KEY',
        'KRI_COMPONENT_NAME': 'VARCHAR(200)',
        'RISK_CATEGORY_ID': 'VARCHAR(75)',
        'KRI_Weight_Value': 'NUMERIC',
        'Qualitative_Quantitative': 'VARCHAR(50)',
        'KRI_Formula': 'VARCHAR(500)',
        'TIMESTAMP': 'timestamp default current_timestamp NOT NULL'
    }
    createTable(database, 'RISK', tableColumns)


def initializeSchemeRITable(datamartDB):
    """
    :param datamartDB: connection object
    :return: None
    """
    tableColumns = {
        'Scheme_KRI_ID': 'VARCHAR(20) Primary Key',
        'SCHEME_ID': 'VARCHAR(10)',
        'Calc_ID': 'VARCHAR(20)',
        'Valid_Invalid': 'VARCHAR(10)',
        'ReferencePeriodYear': 'VARCHAR(10)',
        'ReferencePeriod': 'VARCHAR(10)',
        'KRI_Calc_Date': 'DATE',
        'KRI_ID': 'VARCHAR(20)',
        'KRI_Value': 'NUMERIC',
        'Regulatory_Mitigation_Value': 'NUMERIC',
        'Onsite_Mitigation_Value': 'NUMERIC',
    }
    createTable(datamartDB, 'SCHEME_RI', tableColumns)


def initializeTrusteeRITable(datamartDB):
    """
    :param datamartDB: connection object
    :return: None
    """

    tableColumns = {
        'Trustee_KRI_ID': 'VARCHAR(20) Primary Key',
        'TRUSTEE_ID': 'VARCHAR(10)',
        'Calc_ID': 'VARCHAR(20)',
        'Valid_Invalid': 'VARCHAR(10)',
        'ReferencePeriodYear': 'VARCHAR(10)',
        'ReferencePeriod': 'VARCHAR(10)',
        'KRI_Calc_Date': 'DATE',
        'KRI_ID': 'VARCHAR(20)',
        'KRI_Value': 'NUMERIC',
        'Regulatory_Mitigation_Value': 'NUMERIC',
        'Onsite_Mitigation_Value': 'NUMERIC',
    }
    createTable(datamartDB, 'TRUSTEE_RI', tableColumns)

def initializeStatKITable(datamartDB):
    """
    :param datamartDB: connection object
    :return: None
    """

    tableColumns = {
        'StatKI_Calc_ID': 'VARCHAR(20) Primary Key',
        'Scheme_ID': 'VARCHAR(10)',
        'Calc_ID': 'VARCHAR(20)',
        'Valid_Invalid': 'VARCHAR(10)',
        'ReferencePeriodYear': 'VARCHAR(10)',
        'ReferencePeriod': 'VARCHAR(10)',
        'StatKI_Calc_Date': 'DATE',
        'StatKI_ID': 'VARCHAR(20)',
        'StatKI_Value': 'Decimal(20,2)',
    }
    createTable(datamartDB, 'STAT_KI', tableColumns)

def initializeStatKISumTable(datamartDB):
    """
    :param datamartDB: connection object
    :return: None
    """

    tableColumns = {
        'StatKISum_Calc_ID': 'SERIAL Primary Key NOT NULL',
        'Calc_ID': 'VARCHAR(20)',
        'Valid_Invalid': 'VARCHAR(10)',
        'ReferencePeriodYear': 'VARCHAR(10)',
        'ReferencePeriod': 'VARCHAR(10)',
        'StatKI_Calc_Date': 'DATE',
        'StatKI_ID': 'VARCHAR(20)',
        'StatKI_Value': 'Decimal(30,2)',
    }
    createTable(datamartDB, 'STAT_KI_SUM', tableColumns)
def initialize_Datamart_Tables(datamartDB):
    """
    This function initializes the datamart tables
    :param datamartDB: connection object
    :return: None
    """
    initializeSchemeTable(datamartDB)
    initializeTrusteeTable(datamartDB)
    initializeSchemeTypeTable(datamartDB)
    initializeRiskTable(datamartDB)
    initializeRiskCategoryTable(datamartDB)
    initializeSchemeRITable(datamartDB)
    initializeTrusteeRITable(datamartDB)
    initializeStatKITable(datamartDB)
    initializeCalcParamValue(datamartDB)
    initializeImpactFactorValues(datamartDB)
    initializeStatKISumTable(datamartDB)


def initializeCalculationTable(businessDB):
    """
    This function creates a table in the business database to store the results of the KRI checks
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'Calculation_ID': 'Serial Primary Key',
        'timestamp': 'timestamp default current_timestamp',
    }
    createTable(businessDB, 'Calculation', tableColumns)


def initializeSchemeKriTable(businessDB):
    """
    This function creates a table in the business database to store the results of the scheme KRI checks
    :param businessDB: connection object
    :return: None
    """

    tableColumns = {
        'Scheme_KRI_ID': 'serial primary key not null',
        'Scheme_ID': 'varchar(20) not null',
        'Calculation_ID': 'int not null',
        'Raw_Value': 'int not null',
        'regulatory_mitigation_value': 'int',
        'onsite_mitigation_value': 'int',
        'Reference_Period': 'varchar(255) not null',
        'Reference_Year': 'int not null',
        'KRI_Type': 'varchar(20) not null',
        'FOREIGN KEY (Calculation_ID) REFERENCES "calculation"(calculation_id)': ''
    }
    createTable(businessDB, 'Scheme_KRI', tableColumns)

def initializeTrusteeKriTableBusiness(businessDB):
    """
    This function creates a table in the business database to store the results of the trustee KRI checks
    :param businessDB: connection object
    :return: None
    """

    tableColumns = {
        'Trustee_KRI_ID': 'SERIAL primary key',
        'Trustee_ID': 'varchar(20) not null',
        'Calculation_ID': 'int not null',
        'Raw_Value': 'int not null',
        'regulatory_mitigation_value': 'int',
        'onsite_mitigation_value': 'int',
        'Reference_Period': 'varchar(255) not null',
        'Reference_Year': 'int not null',
        'KRI_Type': 'varchar(20) not null',
        'FOREIGN KEY (Calculation_ID) REFERENCES Calculation(Calculation_ID)': ''
    }
    createTable(businessDB, 'Trustee_KRI', tableColumns)


# Insert Constant Data into the business database
def insertRiskCategoryTable(businessDB):
    """
    :param businessDB: Business Database
    :return: None
    """
    rows = [
        ('RCATID_001', 'Employer risk category', 0.12),
        ('RCATID_002', 'Market conduct risk category', 0.13),
        ('RCATID_003', 'Prudential risk category', 0.21),
        ('RCATID_004', 'Operational A risk category', 0.17),
        ('RCATID_005', 'Governace risk category', 0.22),
        ('RCATID_006', 'Operational B risk category', 0.15)
    ]

    insertRows(businessDB, 'Risk_Category', rows)

def insertRiskTable(businessDB):
    """
    :param businessDB: Business Database
    :return: None
    """
    dataRows = [
        ("KRI_ID_0001", "Contributions remitted late", "RCATID_001", 50.00, "Quantitative Measure"),
        ("KRI_ID_0002", "Contributions remitted not at all", "RCATID_001", 50.00, "Quantitative Measure"),
        ("KRI_ID_0003", "Mis-selling", "RCATID_002", 26.00, "Quantitative Measure"),
        ("KRI_ID_0004", "Lack of communication with scheme members", "RCATID_002", 14.00, "Quantitative Measure"),
        ("KRI_ID_0005", "Dessemination of Inaccurate Information to scheme members", "RCATID_002", 20.00,
         "Quantitative Measure"),
        ("KRI_ID_0006", "Lack of communication and inaccurate communication to upcoming retirees", "RCATID_002", 20.00,
         "Quantitative Measure"),
        ("KRI_ID_0007", "Unresolved complaints and disputes", "RCATID_002", 20.00, "Quantitative Measure"),
        ("KRI_ID_0008", "portfolio performance", "RCATID_003", 12.00, "Quantitative Measure"),
        ("KRI_ID_0009", "Porfolio Valuation", "RCATID_003", 12.00, "Quantitative Measure"),
        ("KRI_ID_0010", "Investment strategy implementation", "RCATID_003", 12.00, "Quantitative Measure"),
        ("KRI_ID_0011", "Credit risk", "RCATID_003", 10.00, "Quantitative Measure"),
        ("KRI_ID_0012", "Credit risk", "RCATID_003", 10.00, "Quantitative Measure"),
        ("KRI_ID_0013", "Liquidity risk", "RCATID_003", 12.00, "Quantitative Measure"),
        ("KRI_ID_0014", "Asset valuation risk", "RCATID_003", 10.00, "Quantitative Measure"),
        ("KRI_ID_0015", "Asset valuation risk", "RCATID_003", 12.00, "Quantitative Measure"),
        ("KRI_ID_0016", "Disparity in fee payment", "RCATID_003", 10.00, "Quantitative Measure"),
        ("KRI_ID_0017", "Timely & accurate recording of contributions", "RCATID_004", 18.33, "Quantitative Measure"),
        ("KRI_ID_0018", "Timely & accurate recording of transfers in", "RCATID_004", 16.67, "Quantitative Measure"),
        ("KRI_ID_0019", "Crediting returns to member accounts", "RCATID_004", 16.67, "Quantitative Measure"),
        ("KRI_ID_0020", "Benefit payments or transfers out", "RCATID_004", 16.67, "Quantitative Measure"),
        ("KRI_ID_0021", "Benefit accrual for or payments to non-existent members (ghosts or phantom members)",
         "RCATID_004", 18.33, "Qualitative Measure"),
        ("KRI_ID_0022", "Unclaimed Benefits", "RCATID_004", 13.33, "Quantitative Measure"),
        ("KRI_ID_0023", "Data Integrity", "RCATID_006", 30.00, "Qualitative Measure"),
        ("KRI_ID_0024", "Cyber-Security", "RCATID_006", 23.33, "Qualitative Measure"),
        ("KRI_ID_0025", "Business Continuity and Disaster Planning", "RCATID_006", 30.00, "Qualitative Measure"),
        ("KRI_ID_0026", "Corporate Trustee Governance & Financial Health", "RCATID_006", 16.67, "Qualitative Measure"),
        ("KRI_ID_0027", "Expertise/competence of Board of Trustees", "RCATID_005", 36.67, "Qualitative Measure"),
        ("KRI_ID_0028", "Expertise/competence of Board of Trustees", "RCATID_005", 31.67, "Qualitative Measure"),
        ("KRI_ID_0029", "Expertise/competence of Board of Trustees", "RCATID_005", 31.67, "Qualitative Measure")
    ]

    insertRows(businessDB, 'Risk', dataRows)

def initializeLatestKRITable(database):
    """
    This function initializes the latest KRI table, which saves the latest KRI values for each scheme
    :param database: connection object
    :return: None
    """
    table_Columns = {
        'Entity_ID': 'text',
        'EntityName': 'text',
        'referenceperiodyear': 'text',
        'referenceperiod': 'text',
    }
    createTable(database, 'Latest_KRI', table_Columns)

def initializeStatisticalTable(database):
    """
    This function initializes the statistical table, which saves the statistical values for each scheme
    :param database: connection object
    :return: None
    """
    table_Columns = {
        'StatisticalCalculation_ID': 'Serial Primary Key',
        'Statistical_ID': 'text',
        'calculation_ID': 'int',
        'Entity_ID': 'text',
        'EntityName': 'text',
        'referenceperiodyear': 'text',
        'referenceperiod': 'text',
        'value': 'decimal',
    }
    createTable(database, 'StatisticalCalculation', table_Columns, foreignKey='calculation_ID',
                foreignKeyTable='Calculation')

def initializeImpactFactorValues(database):
    """
    This function initializes the impact factor values table, which saves the impact factor values for each scheme
    :param database: connection object
    :return: None
    """
    table_Columns = {
        'Impact_factor_weights_ID': 'Serial Primary Key',
        'AUM_limit_from': 'decimal',
        'AUM_limit_to': 'decimal',
        'AUM_impact_factor_value': 'decimal',
        'MM_limit_from': 'decimal',
        'MM_limit_to': 'decimal',
        'MM_impact_factor_value': 'decimal',
        'ARRV_Calib_limit_from': 'decimal',
        'ARRV_Calib_limit_to': 'decimal',
        'ARRV_Calib_limit_value': 'decimal',
        'Impact_factor_values_Data_Time_Stamp': 'timestamp default current_timestamp NOT NULL'
    }
    createTable(database, 'Impact_Factor_Values', table_Columns)

def initializeCalcParamValue(database):
    """
    :param database: connection object
    :return: None
    Scheme_RI_Calc_ID
    Impact_factor_weights_ID
    Impact_factor_values_Data_Time_Stamp
    Scheme_Type_weight_Code_ID
    Scheme_Type_weight_Data_Time_Stamp
    RISK_Category_ID
    Risk_Category_Data_Time_Stamp
    KRI ID
    Risk_Data_Time_Stamp
    """
    table_Columns = {
        'Calculation_Parameter_Value_ID': 'Serial Primary Key',
        'Scheme_RI_Calc_ID': 'varchar(25)',
        'Impact_factor_weights_ID': 'varchar(25)',
        'Impact_factor_values_Data_Time_Stamp': 'timestamp default current_timestamp NOT NULL',
        'Scheme_Type_weight_Code_ID': 'varchar(25)',
        'Scheme_Type_weight_Data_Time_Stamp': 'timestamp default current_timestamp NOT NULL',
        'RISK_Category_ID': 'varchar(25)',
        'Risk_Category_Data_Time_Stamp': 'timestamp default current_timestamp NOT NULL',
        'KRI_ID': 'varchar(25)',
        'Risk_Data_Time_Stamp': 'timestamp default current_timestamp NOT NULL'
    }
    createTable(database, 'Calculation_Parameter_Value', table_Columns)


def initialize_Business_Tables(businessDB):
    """
    This function initializes all tables in the business database
    :param businessDB: connection object
    :return: None
    """
    initializeCalculationTable(businessDB)
    initializeSchemeKriTable(businessDB)
    initializeTrusteeKriTableBusiness(businessDB)
    initializeLatestKRITable(businessDB)
    initializeStatisticalTable(businessDB)
    initializeSchemeTypeTable(businessDB)
    initializeSchemeTable(businessDB)
    initializeTrusteeTable(businessDB)
    initializeRiskTable(businessDB)
    initializeRiskCategoryTable(businessDB)
    initializeCalcParamValue(businessDB)
    initializeImpactFactorValues(businessDB)


def initializeBusinessDatasetTables(businessDB):
    """
    This function initializes the business dataset tables, same as the ones in the stage layer, but with the addition
    of the calculation ID column
    :param businessDB: connection object
    :return: None
    """
    createDataset_0201Table(businessDB)
    createDataset_0204Table(businessDB)
    createDataset_0205Table(businessDB)
    createDataset_0301Table(businessDB)
    createDataset_0302Table(businessDB)
    createDataset_0303Table(businessDB)
    createDataset_0402Table(businessDB)
    createDataset_0403Table(businessDB)
    createDataset_0404Table(businessDB)
    createDataset_0405Table(businessDB)
    createDataset_0406Table(businessDB)
    createDataset_0503Table(businessDB)
    createDataset_0504Table(businessDB)
    createDataset_0601Table(businessDB)
    createDataset_0602Table(businessDB)
    createDataset_0603Table(businessDB)
    createDataset_0604Table(businessDB)
    createDataset_0605Table(businessDB)
    createDataset_0606Table(businessDB)
    createDataset_0607Table(businessDB)

def createDataset_0201Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"other parties scheme id"': 'text',
        '"transfer party"': 'text',
        'surname': 'text',
        '"first name"': 'text',
        '"other names"': 'text',
        '"date of birth"': 'text',
        'nationality': 'text',
        '"date of enrolment"': 'text',
        '"employee ssno"': 'text',
        '"ghana card no."': 'text',
        '"total contribution paid(a)"': 'bigint',
        '"date of receipt of accrued benefits"': 'text',
        '"total contribution receivables (b)"': 'bigint',
        '"total returns (c)"': 'bigint',
        '"accrued benefit (d = a plus c)"': 'bigint',
        '"date of allocation of accrued benefits received"': 'text',
        '"custody bank details (transferor)"': 'text',
    }
    createTable(businessDB, 'Dataset_0201', tableColumns)

def createDataset_0204Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"name of beneficiary"': 'text',
        '"beneficiary ghana card id no."': 'text',
        '"ssnit number of the contributor"': 'text',
        '"type of benefit"': 'text',
        '"date of birth"': 'text',
        '"date joined scheme"': 'text',
        '"date of request"': 'text',
        '"total contributions paid (a)"': 'bigint',
        '"total returns (b)"': 'bigint',
        '"accrued benefits d = (aplusb)"': 'bigint',
        '"indicate paid / payable"': 'text',
        '"date of payment"': 'text',
        'foreign key (calculation_id) references "calculation"(calculation_id)': '',
    }
    createTable(businessDB, 'Dataset_0204', tableColumns)

def createDataset_0205Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"name of contributor"': 'text',
        '"ghana card id"': 'text',
        '"ssnit number"': 'text',
        '"ssnit number of the contributor"': 'text',
        '"first name"': 'text',
        '"date of declaration of unclaimed status"': 'text',
        '"other name"': 'text',
        '"number of months held as unclaimed"': 'bigint',
        'surname': 'text',
        '"accrued benefits at date declared unclaimed"': 'bigint',
        '"date of birth"': 'text',
        '"total additional income earned in preservation"': 'bigint',
        '"telephone numbers"': 'text',
        'foreign key (calculation_id) references "calculation"(calculation_id)': ''
    }
    createTable(businessDB, 'Dataset_0205', tableColumns)

def createDataset_0301Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"investment receivables"': 'bigint',
        '"total assets under management"': 'bigint',
        '"government securities"': 'bigint',
        '"local government / statutory agency securities"': 'bigint',
        '"corporate debt securities"': 'bigint',
        '"bank securities"': 'bigint',
        '"ordinary shares / preference shares"': 'bigint',
        '"collective investment scheme"': 'bigint',
        '"alternative investments"': 'bigint',
        '"bank balances"': 'bigint',
        '"total portfolio return (net return)"': 'double precision',
        '"total portfolio return (gross return)"': 'double precision',
    }
    createTable(businessDB, 'Dataset_0301', tableColumns)

def createDataset_0302Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """

    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        'instrument': 'text',
        '"issuer name"': 'text',
        '"investment id"': 'text',
        '"asset tenure"': 'text',
        '"issue date"': 'text',
        '"asset class"': 'text',
        '"asset allocation actual percent"': 'double precision',
        '"date of investment"': 'text',
        '"maturity date"': 'text',
        '"reporting date"': 'text',
        '"remaining days to maturity"': 'bigint',
        '"days run"': 'bigint',
        '"interest rate percent"': 'double precision',
        '"discount rate percent"': 'double precision',
        '"coupon rate percent"': 'double precision',
        '"coupon paid"': 'bigint',
        '"face value  (ghs)"': 'bigint',
        '"amount invested (ghs)"': 'bigint',
        '"type of investment charge"': 'text',
        '"investment charge rate percent"': 'double precision',
        '"investment charge amount (ghs)"': 'bigint',
        '"market value (ghs)"': 'bigint',
        '"accrued interest (since purchase) / coupons (since last payment"': 'bigint',
        '"accrued interest / coupon for the month"': 'bigint',
        '"outstanding interest to maturity"': 'bigint',
        '"amount impaired (ghs)"': 'bigint',
        '"price per unit / share at purchase"': 'bigint',
        '"price per unit / share at value date"': 'bigint',
        '"capital gains"': 'bigint',
        '"dividend received"': 'bigint',
        '"number of units / shares"': 'bigint',
        '"price per unit / share at last value date"': 'bigint',
        '"currency conversion rate"': 'double precision',
        'currency': 'text',
        '"amount invested in foreign currency (eurobond / external inves"': 'bigint',
        '"holding period return per an investment (percent)"': 'double precision',
        '"holding period return per an investment weighted percent"': 'double precision',
        '"disposal proceeds (ghs)"': 'bigint',
        '"disposal instructions"': 'text',
        '"yield on disposal (ghs)"': 'bigint'
    }
    createTable(businessDB, 'Dataset_0302', tableColumns)

def createDataset_0303Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"unit price"': 'bigint',
        '"unit number"': 'bigint',
        '"daily nav"': 'bigint',
        '"date of valuation"': 'text',
        '"npra fees"': 'bigint',
        '"trustee fees"': 'bigint',
        '"fund manager fees"': 'bigint',
        '"fund custodian fees"': 'bigint'
    }
    createTable(businessDB, 'Dataset_0303', tableColumns)

def createDataset_0402Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"ssnit ssno"': 'text',
        'surname': 'text',
        '"other names"': 'text',
        '"first name"': 'text',
        '"previous name (s)"': 'text',
        '"date of birth"': 'text',
        'gender': 'text',
        'nationality': 'text',
        '"digital address"': 'text',
        '"permanent address"': 'text',
        '"postal address"': 'text',
        'region': 'text',
        'hometown': 'text',
        '"marital status"': 'text',
        '"telephone numbers"': 'bigint',
        '"email address"': 'text',
        '"type of identification"': 'text',
        '"id number"': 'text',
        '"name of father"': 'text',
        '"father\'s telephone numbers"': 'bigint',
        '"name of mother"': 'text',
        '"mother\'s telephone number"': 'bigint',
        'eerno': 'text',
        '"previous employer (if any)"': 'text',
        '"previous ssno (contributor enrolment number ) "': 'text',
        '"nature of employment"': 'bigint',
        '"date joined scheme"': 'text',
        '"date of retirement"': 'text',
        '"nature of employment.1"': 'text',
        '"frequency of income"': 'text',
        '"annual basic salary (ghs)"': 'text',
        '"contribution rate for tier 2"': 'double precision',
        '"contribution rate  for tier 3"': 'double precision',
        '"employee position"': 'text'
    }
    createTable(businessDB, 'Dataset_0402', tableColumns)

def createDataset_0403Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"rgd / roc  number"': 'text',
        '"business location"': 'text',
        '"postal address"': 'text',
        '"email address"': 'text',
        '"telephone numbers"': 'bigint',
        'town': 'text',
        'region': 'text',
        '"date established"': 'text',
        '"industry category"': 'text',
        '"name of contact person"': 'text',
        '"position of contact person"': 'text',
        '"contact person telephone numbers"': 'bigint',
        '"names of directors"': 'text',
        '"telephone numbers of the director"': 'bigint',
        '"email address of the directors"': 'text',
        '"digital address of company"': 'text',
        '"number of employees"': 'bigint',
        '"date of enrolment to the scheme"': 'text',
        '"employer status"': 'text',
        '"number of contributors"': 'bigint'
    }
    createTable(businessDB, 'Dataset_0403', tableColumns)

def createDataset_0404Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        'surname': 'text',
        '"first name"': 'text',
        '"other names"': 'text',
        '"social security number"': 'text',
        '"ghana card number"': 'text',
        '"employee number"': 'text',
        '"date of birth"': 'text',
        '"employer name"': 'text',
        '"hired date"': 'text',
        '"pay period"': 'text',
        '"contributions allocation date"': 'text',
        '"contributions receipt date"': 'text',
        '"original date earned"': 'text',
        '"contribution amount"': 'bigint',
        '"contribution receivables"': 'bigint',
        '"basic salary"': 'bigint'
    }
    createTable(businessDB, 'Dataset_0404', tableColumns)

def createDataset_0405Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"ssnit employer enrolment number"': 'text',
        '"employer name"': 'text',
        '"ghana card id"': 'text',
        '"scheme enrolment id"': 'text',
        'surname': 'text',
        '"first name"': 'text',
        '"other names"': 'text',
        '"contribution (a)"': 'bigint',
        '"withdrawal (c)"': 'bigint',
        '"gain (b)"': 'bigint',
        '"value (d) =(aplusb-c)"': 'bigint'
    }
    createTable(businessDB, 'Dataset_0405', tableColumns)

def createDataset_0406Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"known (source)"': 'bigint',
        '"unknown (source)"': 'bigint',
        '"name of employer (if known)"': 'text',
        '"creation date of suspense account"': 'text',
        '"total balance as at reporting date"': 'bigint',
        '"amount reconciled within the month"': 'bigint',
        '"percentage reconciled within the month"': 'double precision',
        '"amount reconciled cumulatively"': 'bigint',
        '"percentage reconciled cumulatively"': 'double precision'
    }
    createTable(businessDB, 'Dataset_0406', tableColumns)

def createDataset_0503Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"network scanning_service disruption"': 'text',
        '"network scanning_fraud"': 'text',
        '"network scanning_cyber bullying"': 'text',
        '"network scanning_malicious code"': 'text',
        '"client service scanning_service disruption"': 'text',
        '"client service scanning_fraud"': 'text',
        '"client service scanning_cyber bullying"': 'text',
        '"client service scanning_malicious code"': 'text',
        '"system scanning_servcie disruption"': 'text',
        '"system scanning_fraud"': 'text',
        '"system scanning_cyber bullying"': 'text',
        '"system scanning_malicious code"': 'text',
        '"server scanning_service disruption"': 'text',
        '"server scanning_fraud"': 'text',
        '"server scanning_cyber bullying"': 'text',
        '"server scanning_malicious code"': 'text'
    }
    createTable(businessDB, 'Dataset_0503', tableColumns)

def createDataset_0504Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"availability of a bcp"': 'text',
        '"testing of bcp"': 'text',
        '"remediation/mitigation"': 'text'
    }
    createTable(businessDB, 'Dataset_0504', tableColumns)

def createDataset_0601Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"compliant id_number"': 'text',
        '"date of birth"': 'text',
        '"date of receipt of complaint"': 'text',
        '"sponsor of scheme"': 'text',
        '"name of complainant"': 'text',
        '"address of complainant"': 'text',
        '"telephone numbers of complainant"': 'bigint',
        '"staff numbers (public sector workers)"': 'text',
        '"ssnit numbers"': 'text',
        '"ghana card number"': 'text',
        '"type of complaint(s)"': 'text',
        '"description of complaint(s)"': 'text',
        '"employment status"': 'text',
        '"name of current employer"': 'text',
        '"current employers numbers"': 'bigint',
        '"current employers location"': 'text',
        '"name of previous employer"': 'text',
        '"previous employers number"': 'bigint',
        '"previous employers location, address"': 'text',
        '"days run (date as now - date of receipt)"': 'bigint',
        '"status of complaint"': 'text'
    }
    createTable(businessDB, 'Dataset_0601', tableColumns)

def createDataset_0602Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"date of review"': 'text',
        '"source of information"': 'text',
        '"type of message"': 'text',
        '"owner of message"': 'text',
        '"scheme id"': 'text',
        '"description of message"': 'text',
        '"beneficiary of mis- selling information"': 'text',
        'status': 'text'
    }
    createTable(businessDB, 'Dataset_0602', tableColumns)

def createDataset_0603Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"gse index"': 'bigint',
        'gfim': 'bigint'
    }
    createTable(businessDB, 'Dataset_0603', tableColumns)

def createDataset_0604Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"date of sanction"': 'text',
        'offence': 'text',
        '"sanctions amount"': 'bigint',
        '"due date of payment"': 'text',
        '"date of payment of sanction"': 'text'
    }
    createTable(businessDB, 'Dataset_0604', tableColumns)

def createDataset_0605Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'calculation_id': 'int',
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"submission of pfm renewal documents"': 'text',
        '"submission of ct  management accounts"': 'text',
        '"submission of scheme annual audited report"': 'text',
        '"submission of pfc renewal documents"': 'text',
        '"submission of ct renewal documents"': 'text',
        '"submission of renewal of indtrustees"': 'text',
        '"submission of reg58"': 'text',
        '"submission of inv_monthly"': 'text',
        '"submission of sqr"': 'text',
        '"submission of ct  annual audited report"': 'text'
    }
    createTable(businessDB, 'Dataset_0605', tableColumns)

def createDataset_0606Table(businessDB):
    """
    :param businessDB: connection Object
    :return: None
    """
    tableColumns = {
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"kri_id_0001-risk management"': 'bigint',
        '"kri_id_0002-regulatory"': 'bigint',
        '"kri_id_0002-risk management"': 'bigint',
        '"kri_id_0003-regulatory"': 'bigint',
        '"kri_id_0004-regulatory"': 'bigint',
        '"kri_id_0005-regulatory"': 'bigint',
        '"kri_id_0006-regulatory"': 'bigint',
        '"kri_id_0007-regulatory"': 'bigint',
        '"kri_id_0008-regulatory"': 'bigint',
        '"kri_id_0009-regulatory"': 'bigint',
        '"kri_id_0010-regulatory"': 'bigint',
        '"kri_id_0011-regulatory"': 'bigint',
        '"kri_id_0012-regulatory"': 'bigint',
        '"kri_id_0013-regulatory"': 'bigint',
        '"kri_id_0014-regulatory"': 'bigint',
        '"kri_id_0015-regulatory"': 'bigint',
        '"kri_id_0016-regulatory"': 'bigint',
        '"kri_id_0017-regulatory"': 'bigint',
        '"kri_id_0018-regulatory"': 'bigint',
        '"kri_id_0019-regulatory"': 'bigint',
        '"kri_id_0020-regulatory"': 'bigint',
        '"kri_id_0021-regulatory"': 'bigint',
        '"kri_id_0022-regulatory"': 'bigint',
        '"kri_id_0023-regulatory"': 'bigint',
        '"kri_id_0024-regulatory"': 'bigint',
        '"kri_id_0025-regulatory"': 'bigint',
        '"kri_id_0026-regulatory"': 'bigint',
        '"kri_id_0027-regulatory"': 'bigint',
        '"kri_id_0028-regulatory"': 'bigint',
        '"kri_id_0029-regulatory"': 'bigint',
        '"total regulatory mitigation"': 'bigint'
    }
    createTable(businessDB, 'Dataset_0606', tableColumns)

def createDataset_0607Table(businessDB):
    """
    :param businessDB: connection object
    :return: None
    """
    tableColumns = {
        'reportcode': 'text',
        'entity_id': 'text',
        'entityname': 'text',
        'referenceperiodyear': 'bigint',
        'referenceperiod': 'text',
        '"beginning date of inspection"': 'text',
        '"ending date of inspection"': 'text',
        '"date of submission of onsite report"': 'text',
        '"date of issuing management letter"': 'text',
        '"kri_id_0001-onsite"': 'bigint',
        '"kri_id_0002-onsite"': 'bigint',
        '"kri_id_0003-onsite"': 'bigint',
        '"kri_id_0004-onsite"': 'bigint',
        '"kri_id_0005-onsite"': 'bigint',
        '"kri_id_0006-onsite"': 'bigint',
        '"kri_id_0007-onsite"': 'bigint',
        '"kri_id_0008-onsite"': 'bigint',
        '"kri_id_0009-onsite"': 'bigint',
        '"kri_id_0010-onsite"': 'bigint',
        '"kri_id_0011-onsite"': 'bigint',
        '"kri_id_0012-onsite"': 'bigint',
        '"kri_id_0013-onsite"': 'bigint',
        '"kri_id_0014-onsite"': 'bigint',
        '"kri_id_0015-onsite"': 'bigint',
        '"kri_id_0016-onsite"': 'bigint',
        '"kri_id_0017-onsite"': 'bigint',
        '"kri_id_0018-onsite"': 'bigint',
        '"kri_id_0019-onsite"': 'bigint',
        '"kri_id_0020-onsite"': 'bigint',
        '"kri_id_0021-onsite"': 'bigint',
        '"kri_id_0022-onsite"': 'bigint',
        '"kri_id_0023-onsite"': 'bigint',
        '"kri_id_0024-onsite"': 'bigint',
        '"kri_id_0025-onsite"': 'bigint',
        '"kri_id_0026-onsite"': 'bigint',
        '"kri_id_0027-onsite"': 'bigint',
        '"kri_id_0028-onsite"': 'bigint',
        '"kri_id_0029-onsite"': 'bigint',
        '"total onsite mitigation"': 'bigint',
        '"total employer mitigation"': 'bigint',
        '"total market conduct mitigation"': 'bigint',
    }
    createTable(businessDB, 'Dataset_0607', tableColumns)

def insertImpactFactorTable(database):
    """
    :param database: connection object
    :return: None
        (0, 0.01, 1, 0, 0.01, 1),
        (0.01, 0.05, 2, 0.01, 0.05, 2),
        (0.05, 0.1, 3, 0.05, 0.1, 3),
        (0.1, 0.84, 4, 0.1, 0.84, 4)
    """
    dataRows = [
        ('DEFAULT', 0, 0.01, 1, 0, 0.01, 1, 1, 4, 1),
        ('DEFAULT', 0.01, 0.05, 2, 0.01, 0.05, 2, 4, 8, 2),
        ('DEFAULT', 0.05, 0.1, 3, 0.05, 0.1, 3, 8, 12, 3),
        ('DEFAULT', 0.1, 0.84, 4, 0.1, 0.84, 4, 12, 16, 4)
    ]
    insertRows(database, 'impact_factor_values', dataRows)


def insertConstantDataBusiness(businessDB):
    insertRiskCategoryTable(businessDB)
    insertRiskTable(businessDB)
def insertConstantDataDatamart(datamartDB):
    insertRiskCategoryTable(datamartDB)
    insertRiskTable(datamartDB)
    insertImpactFactorTable(datamartDB)

def main():
    datamartDB = connect(datamart_DatabaseInfo)
    businessDB = connect(business_DatabaseInfo)

    datamartDB.autocommit = True
    businessDB.autocommit = True

    initialize_Datamart_Tables(datamartDB)
    initialize_Business_Tables(businessDB)
    # initializeBusinessDatasetTables(businessDB)

    insertConstantDataBusiness(businessDB)
    insertConstantDataDatamart(datamartDB)


if __name__ == '__main__':
    main()
