import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
# Settings
valid = os.getenv('VALID').lower() == 'yes'

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


validity = 'Valid' if valid else 'Invalid'


# Classes
class Scheme:
    def __init__(self):
        self.scheme_id = None
        self.scheme_name = None
        self.scheme_type_code_ID = None
        self.trustee_id = None
        self.FM_ID = None
        self.FC_ID = None

    def setSchemeID(self, scheme_id):
        self.scheme_id = scheme_id

    def setSchemeName(self, scheme_name):
        self.scheme_name = scheme_name

    def setSchemeType(self, scheme_type):
        self.scheme_type_code_ID = scheme_type

    def setTrusteeID(self, trustee_id):
        self.trustee_id = trustee_id

    def setFM_ID(self, FM_ID):
        self.FM_ID = FM_ID

    def setFC_ID(self, FC_ID):
        self.FC_ID = FC_ID

    def getSchemeID(self):
        return self.scheme_id

    def getSchemeName(self):
        return self.scheme_name

    def getSchemeType(self):
        return self.scheme_type_code_ID

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


class Scheme_Type:
    def __init__(self):
        self.ID = None
        self.scheme_Type_Code_ID = None
        self.scheme_type_weight = None
        self.timestamp = None

    def setID(self, ID):
        self.ID = ID

    def getID(self):
        return self.ID

    def setSchemeType(self, scheme_Type):
        self.scheme_Type_Code_ID = scheme_Type

    def setWeight(self, weight):
        self.scheme_type_weight = weight

    def getSchemeType(self):
        return self.scheme_Type_Code_ID

    def getWeight(self):
        return self.scheme_type_weight

    def setTimestamp(self, timestamp):
        self.timestamp = timestamp

    def getTimestamp(self):
        return self.timestamp


class Risk_Category:
    def __init__(self):
        self.risk_category_id = None
        self.risk_category_name = None
        self.risk_category_weight = None
        self.timestamp = None

    # Setters and Getters
    def setRiskCategoryID(self, risk_category_id):
        self.risk_category_id = risk_category_id

    def setRiskCategoryName(self, risk_category_name):
        self.risk_category_name = risk_category_name

    def setRiskCategoryWeight(self, risk_category_weight):
        self.risk_category_weight = risk_category_weight

    def getRiskCategoryID(self):
        return self.risk_category_id

    def getRiskCategoryName(self):
        return self.risk_category_name

    def getRiskCategoryWeight(self):
        return self.risk_category_weight

    def setTimestamp(self, timestamp):
        self.timestamp = timestamp

    def getTimestamp(self):
        return self.timestamp


class Risk:
    def __init__(self):
        self.KRI_ID = None
        self.KRI_Component_Name = None
        self.Risk_Category_ID = None
        self.KRI_Weight_Value = None
        self.Qualitative_Quantitative = None
        self.KRI_Formula = None
        self.timestamp = None

    # Setters and Getters
    def setKRI_ID(self, KRI_ID):
        self.KRI_ID = KRI_ID

    def setKRIComponentName(self, KRI_Component_Name):
        self.KRI_Component_Name = KRI_Component_Name

    def setRiskCategoryID(self, Risk_Category_ID):
        self.Risk_Category_ID = Risk_Category_ID

    def setKRIWeightValue(self, KRI_Weight_Value):
        self.KRI_Weight_Value = KRI_Weight_Value

    def setQualitativeQuantitative(self, Qualitative_Quantitative):
        self.Qualitative_Quantitative = Qualitative_Quantitative

    def setKRIFormula(self, KRI_Formula):
        self.KRI_Formula = KRI_Formula

    def getKRI_ID(self):
        return self.KRI_ID

    def getKRIComponentName(self):
        return self.KRI_Component_Name

    def getRiskCategoryID(self):
        return self.Risk_Category_ID

    def getKRIWeightValue(self):
        return self.KRI_Weight_Value

    def getQualitativeQuantitative(self):
        return self.Qualitative_Quantitative

    def getKRIFormula(self):
        return self.KRI_Formula

    def setTimestamp(self, timestamp):
        self.timestamp = timestamp

    def getTimestamp(self):
        return self.timestamp


class Scheme_RI:
    def __init__(self):
        self.scheme_KRI_ID = None
        self.scheme_id = None
        self.calc_id = None
        self.valid_invalid = None
        self.KRI_calc_date = None
        self.referencePeriodYear = None
        self.referencePeriod = None
        self.KRI_ID = None
        self.KRI_Value = None
        self.regulatory_mitigation_value = None
        self.onsite_mitigation_value = None
        self.kr_weight_value = None
        self.risk_category_weight_value = None

    # Getters and Setters
    def setSchemeKRIID(self, scheme_KRI_ID):
        self.scheme_KRI_ID = scheme_KRI_ID

    def setSchemeID(self, scheme_id):
        self.scheme_id = scheme_id

    def setValidInvalid(self, valid_invalid):
        self.valid_invalid = valid_invalid

    def setKRICalcDate(self, KRI_calc_date):
        self.KRI_calc_date = KRI_calc_date

    def setKRI_ID(self, KRI_ID):
        self.KRI_ID = KRI_ID

    def setKRIValue(self, KRI_Value):
        self.KRI_Value = KRI_Value

    def setRegulatoryMitigationValue(self, regulatory_mitigation_value):
        self.regulatory_mitigation_value = regulatory_mitigation_value

    def setOnsiteMitigationValue(self, onsite_mitigation_value):
        self.onsite_mitigation_value = onsite_mitigation_value

    def setKRWeightValue(self, kr_weight_value):
        self.kr_weight_value = kr_weight_value

    def setRiskCategoryWeightValue(self, risk_category_weight_value):
        self.risk_category_weight_value = risk_category_weight_value

    def getSchemeID(self):
        return self.scheme_id

    def getValidInvalid(self):
        return self.valid_invalid

    def getKRICalcDate(self):
        return self.KRI_calc_date

    def getKRI_ID(self):
        return self.KRI_ID

    def getKRIValue(self):
        return self.KRI_Value

    def getRegulatoryMitigationValue(self):
        return self.regulatory_mitigation_value

    def getOnsiteMitigationValue(self):
        return self.onsite_mitigation_value

    def getKRWeightValue(self):
        return self.kr_weight_value

    def getRiskCategoryWeightValue(self):
        return self.risk_category_weight_value

    def setReferencePeriodYear(self, referencePeriodYear):
        self.referencePeriodYear = referencePeriodYear

    def getReferencePeriodYear(self):
        return self.referencePeriodYear

    def setReferencePeriod(self, referencePeriod):
        self.referencePeriod = referencePeriod

    def getReferencePeriod(self):
        return self.referencePeriod

    def setCalcID(self, calc_id):
        self.calc_id = calc_id

    def getCalcID(self):
        return self.calc_id

    def getSchemeKRIID(self):
        return self.scheme_KRI_ID


class Trustee_RI:
    def __init__(self):
        self.trustee_KRI_ID = None
        self.trustee_id = None
        self.calc_id = None
        self.valid_invalid = None
        self.KRI_calc_date = None
        self.referencePeriodYear = None
        self.referencePeriod = None
        self.KRI_ID = None
        self.KRI_Value = None
        self.regulatory_mitigation_value = None
        self.onsite_mitigation_value = None
        self.kr_weight_value = None
        self.risk_category_weight_value = None

    # Getters and Setters
    def setTrusteeKRIID(self, trustee_KRI_ID):
        self.trustee_KRI_ID = trustee_KRI_ID

    def setTrusteeID(self, trustee_id):
        self.trustee_id = trustee_id

    def setValidInvalid(self, valid_invalid):
        self.valid_invalid = valid_invalid

    def setKRICalcDate(self, KRI_calc_date):
        self.KRI_calc_date = KRI_calc_date

    def setKRIValue(self, KRI_Value):
        self.KRI_Value = KRI_Value

    def setRegulatoryMitigationValue(self, regulatory_mitigation_value):
        self.regulatory_mitigation_value = regulatory_mitigation_value

    def setOnsiteMitigationValue(self, onsite_mitigation_value):
        self.onsite_mitigation_value = onsite_mitigation_value

    def setKRWeightValue(self, kr_weight_value):
        self.kr_weight_value = kr_weight_value

    def setRiskCategoryWeightValue(self, risk_category_weight_value):
        self.risk_category_weight_value = risk_category_weight_value

    def getTrusteeID(self):
        return self.trustee_id

    def getValidInvalid(self):
        return self.valid_invalid

    def getKRICalcDate(self):
        return self.KRI_calc_date

    def getKRIValue(self):
        return self.KRI_Value

    def getRegulatoryMitigationValue(self):
        return self.regulatory_mitigation_value

    def getOnsiteMitigationValue(self):
        return self.onsite_mitigation_value

    def getKRWeightValue(self):
        return self.kr_weight_value

    def getRiskCategoryWeightValue(self):
        return self.risk_category_weight_value

    def setReferencePeriodYear(self, referencePeriodYear):
        self.referencePeriodYear = referencePeriodYear

    def getReferencePeriodYear(self):
        return self.referencePeriodYear

    def setReferencePeriod(self, referencePeriod):
        self.referencePeriod = referencePeriod

    def getReferencePeriod(self):
        return self.referencePeriod

    def setCalcID(self, calc_id):
        self.calc_id = calc_id

    def getCalcID(self):
        return self.calc_id

    def setKRI_ID(self, KRI_ID):
        self.KRI_ID = KRI_ID

    def getKRI_ID(self):
        return self.KRI_ID

    def getTrusteeKRIID(self):
        return self.trustee_KRI_ID


class Stat_KI:
    def __init__(self):
        self.Stat_KRI_ID = None
        self.scheme_id = None
        self.calc_id = None
        self.valid_invalid = None
        self.referencePeriodYear = None
        self.referencePeriod = None
        self.StatKI_Calc_Date = None
        self.StatKI_ID = None
        self.StatKI_Value = None

    # Getters and Setters
    def setStatKRIID(self, Stat_KRI_ID):
        self.Stat_KRI_ID = Stat_KRI_ID

    def setSchemeID(self, scheme_id):
        self.scheme_id = scheme_id

    def setValidInvalid(self, valid_invalid):
        self.valid_invalid = valid_invalid

    def setStatKI_CalcDate(self, StatKI_Calc_Date):
        self.StatKI_Calc_Date = StatKI_Calc_Date

    def setStatKI_ID(self, StatKI_ID):
        self.StatKI_ID = StatKI_ID

    def setStatKI_Value(self, StatKI_Value):
        self.StatKI_Value = StatKI_Value

    def getSchemeID(self):
        return self.scheme_id

    def getValidInvalid(self):
        return self.valid_invalid

    def getStatKI_CalcDate(self):
        return self.StatKI_Calc_Date

    def getStatKI_ID(self):
        return self.StatKI_ID

    def getStatKI_Value(self):
        return self.StatKI_Value

    def setReferencePeriodYear(self, referencePeriodYear):
        self.referencePeriodYear = referencePeriodYear

    def getReferencePeriodYear(self):
        return self.referencePeriodYear

    def setReferencePeriod(self, referencePeriod):
        self.referencePeriod = referencePeriod

    def getReferencePeriod(self):
        return self.referencePeriod

    def setCalcID(self, calc_id):
        self.calc_id = calc_id

    def getCalcID(self):
        return self.calc_id

    def getStatKRIID(self):
        return self.Stat_KRI_ID


class Stat_KI_Sum:
    def __init__(self):
        self.Stat_KRI_ID = None
        self.calc_id = None
        self.valid_invalid = None
        self.referencePeriodYear = None
        self.referencePeriod = None
        self.StatKI_Calc_Date = None
        self.StatKI_ID = None
        self.StatKI_Value = None

    # Getters and Setters
    def setStatKRIID(self, Stat_KRI_ID):
        self.Stat_KRI_ID = Stat_KRI_ID

    def setValidInvalid(self, valid_invalid):
        self.valid_invalid = valid_invalid

    def setStatKI_CalcDate(self, StatKI_Calc_Date):
        self.StatKI_Calc_Date = StatKI_Calc_Date

    def setStatKI_ID(self, StatKI_ID):
        self.StatKI_ID = StatKI_ID

    def setStatKI_Value(self, StatKI_Value):
        self.StatKI_Value = StatKI_Value

    def getValidInvalid(self):
        return self.valid_invalid

    def getStatKI_CalcDate(self):
        return self.StatKI_Calc_Date

    def getStatKI_ID(self):
        return self.StatKI_ID

    def getStatKI_Value(self):
        return self.StatKI_Value

    def setReferencePeriodYear(self, referencePeriodYear):
        self.referencePeriodYear = referencePeriodYear

    def getReferencePeriodYear(self):
        return self.referencePeriodYear

    def setReferencePeriod(self, referencePeriod):
        self.referencePeriod = referencePeriod

    def getReferencePeriod(self):
        return self.referencePeriod

    def setCalcID(self, calc_id):
        self.calc_id = calc_id

    def getCalcID(self):
        return self.calc_id

    def getStatKRIID(self):
        return self.Stat_KRI_ID


# Database Functions
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


# Setup Constant Tables in the business database


# Retrieve data from partner database
def retrieveSchemeData(partnerDB):
    partnerConnector = partnerDB.cursor()
    # Get all the schemes from the scheme table
    partnerConnector.execute('''
            SELECT scheme_id, scheme_name, scheme_type_id FROM schemes_scheme;
        ''')

    schemes = partnerConnector.fetchall()
    schemesList = []  # List of all schemes objects
    for scheme in schemes:
        ID = scheme[0]
        name = scheme[1]
        schemeType = scheme[2]

        # Find the schemeType ID on the partner register
        partnerConnector.execute('''
            SELECT scheme_type_id_code FROM schemes_schemetype WHERE id = %s;
        ''', (schemeType,))
        try:
            schemeType = partnerConnector.fetchone()[0]
        except TypeError:
            pass
        newScheme = Scheme()
        newScheme.setSchemeID(ID)
        newScheme.setSchemeName(name)
        newScheme.setSchemeType(schemeType)

        # Get trustee ID and FM ID
        # Get the scheme ID first
        fundManagerID = None
        trusteeID = None
        partnerConnector.execute('''
                    SELECT id FROM schemes_scheme WHERE scheme_id = %s;
                ''', (ID,))
        schemeID = partnerConnector.fetchone()

        partnerConnector.execute('''
            SELECT trustee_id FROM schemes_corporatetrusteeappointment WHERE scheme_id = %s;
        ''', (schemeID,))
        trustee = partnerConnector.fetchone()
        # If there is a trustee, get the trustee ID from the trustee table
        if trustee:
            trusteeID = trustee[0]
            partnerConnector.execute('''
                SELECT trustee_id FROM schemes_corporatetrustee WHERE id = %s;
            ''', (trusteeID,))
            trusteeID = partnerConnector.fetchone()[0]

        partnerConnector.execute('''
            SELECT fund_manager_id FROM schemes_fundmanagerappointment WHERE scheme_id = %s;
        ''', (schemeID,))

        fundManager = partnerConnector.fetchone()
        # If there is a fund manager, get the fund manager ID from the fund manager table
        if fundManager:
            fundManagerID = fundManager[0]
            partnerConnector.execute('''
                SELECT fund_manager_id FROM schemes_fundmanager WHERE id = %s;
            ''', (fundManagerID,))

        newScheme.setTrusteeID(trusteeID)
        newScheme.setFM_ID(fundManagerID)
        schemesList.append(newScheme)

    return schemesList


def retrieveTrusteeData(partnerDB):
    """
    :param partnerDB - connection object to the partner database
    :return: list of trustee objects
    """
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


def retrieveSchemeTypeData(partnerDB):
    """
    :param partnerDB: connection object to the partner database
    :return: list of scheme type objects
    """
    partnerConnector = partnerDB.cursor()
    # Get all the scheme types from the scheme type table
    partnerConnector.execute('''
            SELECT ID, scheme_type_id_code FROM schemes_schemetype;
        ''')

    schemeTypes = partnerConnector.fetchall()
    schemeTypesList = []  # List of all scheme types objects
    for schemeType in schemeTypes:
        ID = schemeType[0]
        Type = schemeType[1]
        weight = 0.125
        newSchemeType = Scheme_Type()
        newSchemeType.setID(ID)
        newSchemeType.setSchemeType(Type)
        newSchemeType.setWeight(weight)
        schemeTypesList.append(newSchemeType)

    return schemeTypesList


def retrieveRiskCategoryData(businessDB):
    """
    :param businessDB: connection object to the business database
    :return: list of risk category objects
    """
    businessConnector = businessDB.cursor()
    # Get all the risk categories from the risk category table
    businessConnector.execute('''
            SELECT * FROM risk_category;
        ''')

    riskCategories = businessConnector.fetchall()
    riskCategoriesList = []  # List of all risk categories objects
    for riskCategory in riskCategories:
        ID = riskCategory[0]
        name = riskCategory[1]
        weight = riskCategory[2]
        timestamp = riskCategory[3]
        newRiskCategory = Risk_Category()
        newRiskCategory.setRiskCategoryName(name)
        newRiskCategory.setRiskCategoryWeight(weight)
        newRiskCategory.setRiskCategoryID(ID)
        newRiskCategory.setTimestamp(timestamp)
        riskCategoriesList.append(newRiskCategory)

    return riskCategoriesList


def retrieveRiskData(businessDB):
    """
    :param businessDB: connection object to the business database
    :return: list of risk objects
    """
    businessConnector = businessDB.cursor()
    # Get all the risks from the risk table
    businessConnector.execute('''
            SELECT * FROM risk;
        ''')

    risks = businessConnector.fetchall()
    risksList = []  # List of all risks objects

    for risk in risks:
        KRI_ID = risk[0]
        KRI_Component_Name = risk[1]
        Risk_Category_ID = risk[2]
        KRI_Weight_Value = risk[3]
        Qualitative_Quantitative = risk[4]
        KRI_Formula = risk[5]
        timestamp = risk[6]

        newRisk = Risk()

        newRisk.setKRI_ID(KRI_ID)
        newRisk.setKRIComponentName(KRI_Component_Name)
        newRisk.setRiskCategoryID(Risk_Category_ID)
        newRisk.setKRIWeightValue(KRI_Weight_Value)
        newRisk.setQualitativeQuantitative(Qualitative_Quantitative)
        newRisk.setKRIFormula(KRI_Formula)
        newRisk.setTimestamp(timestamp)

        risksList.append(newRisk)

    return risksList


def retrieveSchemeRI(businessDB):
    """
    :param businessDB: connection object to the business database
    :return: list of scheme risk indicator objects
    """
    businessConnector = businessDB.cursor()
    # Get all the scheme risk indicators from the scheme risk indicator table
    businessConnector.execute('''
            SELECT scheme_id, calculation_id, raw_value, regulatory_mitigation_value, onsite_mitigation_value, 
            reference_period, reference_year, kri_type, Scheme_KRI_ID
             FROM scheme_kri;
        ''')

    schemeRIs = businessConnector.fetchall()
    schemeRIList = []  # List of all scheme risk indicator objects
    for schemeRI in schemeRIs:
        schemeID = schemeRI[0]
        calculationID = schemeRI[1]
        rawValue = schemeRI[2]
        regulatoryMitigationValue = schemeRI[3]
        onsiteMitigationValue = schemeRI[4]
        referencePeriod = schemeRI[5]
        referenceYear = schemeRI[6]
        KRIType = schemeRI[7]
        newSchemeRI = Scheme_RI()
        newSchemeRI.setSchemeID(schemeID)
        newSchemeRI.setCalcID(calculationID)
        newSchemeRI.setKRIValue(rawValue)
        newSchemeRI.setRegulatoryMitigationValue(regulatoryMitigationValue)
        newSchemeRI.setOnsiteMitigationValue(onsiteMitigationValue)
        newSchemeRI.setReferencePeriod(referencePeriod)
        newSchemeRI.setReferencePeriodYear(referenceYear)
        newSchemeRI.setKRI_ID(str(KRIType))
        newSchemeRI.setValidInvalid(validity)
        newSchemeRI.setSchemeKRIID(schemeRI[8])
        # Get the calculation date
        businessConnector.execute('''
            SELECT timestamp FROM "calculation" WHERE calculation_id = %s;
        ''', (calculationID,))
        calculationDate = businessConnector.fetchone()
        newSchemeRI.setKRICalcDate(calculationDate[0])
        schemeRIList.append(newSchemeRI)

    return schemeRIList


def retrieveTrusteeRI(businessDB):
    businessConnector = businessDB.cursor()
    # Get all the trustee risk indicators from the trustee risk indicator table
    businessConnector.execute('''
            SELECT trustee_id, calculation_id, raw_value, regulatory_mitigation_value, onsite_mitigation_value, 
            reference_period, reference_year, kri_type, Trustee_KRI_ID
             FROM trustee_kri;
        ''')

    trusteeRIs = businessConnector.fetchall()
    trusteeRIList = []  # List of all trustee risk indicator objects
    for trusteeRI in trusteeRIs:
        trusteeID = trusteeRI[0]
        calculationID = trusteeRI[1]
        rawValue = trusteeRI[2]
        regulatoryMitigationValue = trusteeRI[3]
        onsiteMitigationValue = trusteeRI[4]
        referencePeriod = trusteeRI[5]
        referenceYear = trusteeRI[6]
        KRIType = trusteeRI[7]
        newTrusteeRI = Trustee_RI()
        newTrusteeRI.setTrusteeID(trusteeID)
        newTrusteeRI.setCalcID(calculationID)
        newTrusteeRI.setKRIValue(rawValue)
        newTrusteeRI.setRegulatoryMitigationValue(regulatoryMitigationValue)
        newTrusteeRI.setOnsiteMitigationValue(onsiteMitigationValue)
        newTrusteeRI.setReferencePeriod(referencePeriod)
        newTrusteeRI.setReferencePeriodYear(referenceYear)
        newTrusteeRI.setKRI_ID(str(KRIType))
        newTrusteeRI.setValidInvalid(validity)
        newTrusteeRI.setTrusteeKRIID(trusteeRI[8])
        # Get the calculation date
        businessConnector.execute('''
            SELECT timestamp FROM "calculation" WHERE calculation_id = %s;
        ''', (calculationID,))
        calculationDate = businessConnector.fetchone()
        newTrusteeRI.setKRICalcDate(calculationDate[0])
        trusteeRIList.append(newTrusteeRI)

    return trusteeRIList


def retrieveStatData(businessDB):
    """
    :param businessDB: connection object to the business database
    :return: list of stat objects
    """
    businessConnector = businessDB.cursor()
    StatList = []
    businessConnector.execute('''
            SELECT statistical_id, calculation_id, entity_id, referenceperiod, 
            referenceperiodyear, value, statisticalcalculation_id FROM "statisticalcalculation";
        ''')
    rows = businessConnector.fetchall()
    for stat in rows:
        newStat = Stat_KI()
        newStat.setStatKI_ID(stat[0])
        newStat.setCalcID(stat[1])
        newStat.setSchemeID(stat[2])
        newStat.setReferencePeriod(stat[3])
        newStat.setReferencePeriodYear(stat[4])
        newStat.setStatKI_Value(stat[5])
        newStat.setValidInvalid(validity)
        newStat.setStatKRIID(stat[6])
        # Get the calculation date
        businessConnector.execute('''
            SELECT timestamp FROM "calculation" WHERE calculation_id = %s;
        ''', (stat[1],))
        calculationDate = businessConnector.fetchone()
        newStat.setStatKI_CalcDate(calculationDate[0])
        StatList.append(newStat)
    return StatList


def calculateStatKISumData(statDataList):
    """
    This function calculates the sum of each stat value per calculation ID
    :param statDataList: list of stat objects
    :return: list of stat objects with the sum of the stat values
    """
    calcIDList = {stat.getCalcID() for stat in statDataList}
    statKISumList = []
    for calcID in calcIDList:
        try:
            statIDs = {stat.getStatKI_ID() for stat in statDataList if stat.getCalcID() == calcID}
            for statID in statIDs:
                statSum = 0
                # Get a random stat object to get the time period and year and timestamp
                statObj = [stat for stat in statDataList if stat.getStatKI_ID() == statID].pop()
                # Sum the stat values for all schemes under the current calculation ID and stat ID
                statSum += float({stat.getStatKI_Value() for stat in statDataList if stat.getStatKI_ID() == statID}.pop())
                newStat = Stat_KI()
                newStat.setCalcID(calcID)
                newStat.setStatKRIID(statObj.getStatKI_ID())
                newStat.setValidInvalid(validity)
                newStat.setStatKI_Value(statSum)
                newStat.setReferencePeriod(statObj.getReferencePeriod())
                newStat.setReferencePeriodYear(statObj.getReferencePeriodYear())
                newStat.setStatKI_CalcDate(statObj.getStatKI_CalcDate())
                statKISumList.append(newStat)
        except Exception as e:
            print(e)

    return statKISumList


def retrieveData(businessDB, partnerDB):
    print('Retrieving Data')
    statDataList = retrieveStatData(businessDB)
    schemeTypesList = retrieveSchemeTypeData(partnerDB)
    schemesList = retrieveSchemeData(partnerDB)
    trusteesList = retrieveTrusteeData(partnerDB)
    risksList = retrieveRiskData(businessDB)
    riskCategoriesList = retrieveRiskCategoryData(businessDB)
    schemeRIList = retrieveSchemeRI(businessDB)
    trusteeRIList = retrieveTrusteeRI(businessDB)
    StatKISumList = calculateStatKISumData(statDataList)

    print('Retrieved Data')

    return schemesList, trusteesList, schemeTypesList, risksList, riskCategoriesList, \
        schemeRIList, trusteeRIList, statDataList, StatKISumList


def connectToDatabases():
    # Connect to business database
    businessDB = connect(business_DatabaseInfo)
    # Connect to partner database
    partnerDB = connect(partner_DatabaseInfo)
    # Connect to stage database
    stageDB = connect(stage_DatabaseInfo)

    return businessDB, partnerDB, stageDB


def insert_Scheme(datamartDB, scheme):
    datamartConnector = datamartDB.cursor()
    datamartConnector.execute('''
        INSERT INTO scheme (scheme_id, scheme_name, scheme_type_code_ID, trustee_id, fc_id)
        VALUES (%s, %s, %s, %s, %s);
    ''', (scheme.getSchemeID(), scheme.getSchemeName(), scheme.getSchemeType(), scheme.getTrusteeID(),
          scheme.getFCID()))
    datamartDB.commit()


def insert_Trustee(datamartDB, trustee):
    datamartConnector = datamartDB.cursor()
    datamartConnector.execute('''
        INSERT INTO trustee (trustee_id, trustee_name)
        VALUES (%s, %s);
    ''', (trustee.getTrusteeID(), trustee.getTrusteeName()))
    datamartDB.commit()


def insert_Scheme_Type(datamartDB, schemeType):
    datamartConnector = datamartDB.cursor()
    datamartConnector.execute('''
        INSERT INTO scheme_type (ID, scheme_type_code_id, scheme_type_weight)
        VALUES (%s, %s, %s);
    ''', (schemeType.getID(), schemeType.getSchemeType(), schemeType.getWeight()))
    datamartDB.commit()


def insert_Risk_Category(datamartDB, riskCategory):
    datamartConnector = datamartDB.cursor()
    datamartConnector.execute('''
        INSERT INTO risk_category (risk_category_id, risk_category_name, risk_category_weight, timestamp)
        VALUES (%s, %s, %s, %s);
    ''', (riskCategory.getRiskCategoryID(), riskCategory.getRiskCategoryName(), riskCategory.getRiskCategoryWeight(),
          riskCategory.getTimestamp()))
    datamartDB.commit()


def insert_Risk(datamartDB, risk):
    datamartConnector = datamartDB.cursor()
    datamartConnector.execute('''
        INSERT INTO risk (kri_id, kri_component_name, risk_category_id, kri_weight_value, qualitative_quantitative, 
        kri_formula, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    ''', (risk.getKRI_ID(), risk.getKRIComponentName(), risk.getRiskCategoryID(), risk.getKRIWeightValue(),
          risk.getQualitativeQuantitative(), risk.getKRIFormula(), risk.getTimestamp()))
    datamartDB.commit()


def insert_Scheme_RI(datamartDB, schemeRI):
    datamartConnector = datamartDB.cursor()
    datamartConnector.execute('''
        INSERT INTO scheme_ri (scheme_id, calc_id, valid_invalid, referenceperiodyear, referenceperiod, kri_calc_date, 
        kri_id, kri_value, regulatory_mitigation_value, onsite_mitigation_value, Scheme_KRI_ID)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    ''', (schemeRI.getSchemeID(), schemeRI.getCalcID(), schemeRI.getValidInvalid(), schemeRI.getReferencePeriodYear(),
          schemeRI.getReferencePeriod(), schemeRI.getKRICalcDate(), schemeRI.getKRI_ID(), schemeRI.getKRIValue(),
          schemeRI.getRegulatoryMitigationValue(), schemeRI.getOnsiteMitigationValue(), schemeRI.getSchemeKRIID()))
    datamartDB.commit()


def insert_Trustee_RI(datamartDB, trusteeRI):
    datamartConnector = datamartDB.cursor()
    datamartConnector.execute('''
        INSERT INTO trustee_ri (trustee_id, calc_id, valid_invalid, referenceperiodyear, referenceperiod, kri_calc_date, 
        kri_id, kri_value, regulatory_mitigation_value, onsite_mitigation_value, Trustee_KRI_ID)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    ''', (
        trusteeRI.getTrusteeID(), trusteeRI.getCalcID(), trusteeRI.getValidInvalid(),
        trusteeRI.getReferencePeriodYear(),
        trusteeRI.getReferencePeriod(), trusteeRI.getKRICalcDate(), trusteeRI.getKRI_ID(), trusteeRI.getKRIValue(),
        trusteeRI.getRegulatoryMitigationValue(), trusteeRI.getOnsiteMitigationValue(), trusteeRI.getTrusteeKRIID()))
    datamartDB.commit()


def insert_Statistical_Data(datamartDB, statData):
    """
    :param datamartDB:
    :param statData:
    :return:
    """
    datamartConnector = datamartDB.cursor()
    datamartConnector.execute('''
        INSERT INTO stat_ki (scheme_id, calc_id, valid_invalid, referenceperiodyear, referenceperiod, statki_calc_date, 
        statki_id, statki_value, StatKI_Calc_ID)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    ''', (statData.getSchemeID(), statData.getCalcID(), statData.getValidInvalid(), statData.getReferencePeriodYear(),
          statData.getReferencePeriod(), statData.getStatKI_CalcDate(), statData.getStatKI_ID(),
          statData.getStatKI_Value(), statData.getStatKRIID()))
    datamartDB.commit()


def insert_Statistical_KI_Sum(datamartDB, statKISum):
    """
    :param datamartDB:
    :param statKISum:
    :return:
    """
    datamartConnector = datamartDB.cursor()
    datamartConnector.execute('''
        INSERT INTO STAT_KI_SUM (calc_id, valid_invalid, referenceperiodyear, referenceperiod, statki_calc_date, 
        statki_id, statki_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    ''', (statKISum.getCalcID(), statKISum.getValidInvalid(),
          statKISum.getReferencePeriodYear(), statKISum.getReferencePeriod(), statKISum.getStatKI_CalcDate(),
          statKISum.getStatKRIID(), statKISum.getStatKI_Value()))
    datamartDB.commit()


def insertCalculationParameterValue(datamartDB):
    """
    :param datamartDB:
    :return:
    This function inserts the most recent calculation parameter value into the datamart database
    Parameters: Scheme_RI_Calc_ID, Impact_Factor_Weights_ID, Impact_Factor_Values_Data_Time_Stamp,
    Scheme_Type_Weight_Code_ID, Scheme_Type_Weight_Data_Time_Stamp, Risk_Category_ID, Risk_Category_Data_Time_Stamp,
    KRI_ID, Risk_Data_Time_Stamp
    """
    # Inserts one record starting from KRI_0001 up to KRI_0029
    try:
        datamartConnector = datamartDB.cursor()
        # Get latest Calculation ID from the last record in scheme_ri table
        datamartConnector.execute('''
            SELECT calc_id FROM scheme_ri ORDER BY calc_id DESC LIMIT 1;
        ''')
        calc_id = datamartConnector.fetchone()[0]
        # Get Risks from the risks table
        datamartConnector.execute('''
            SELECT kri_id, risk_category_id, timestamp FROM risk;
        ''')
        risks = datamartConnector.fetchall()
        # Get latest Impact Factor Weights ID from the last record in impact_factor_weights table
        datamartConnector.execute('''
            SELECT impact_factor_weights_id, impact_factor_values_data_time_stamp FROM impact_factor_values ORDER BY impact_factor_weights_id DESC LIMIT 1;
        ''')
        impact_factor_weights = datamartConnector.fetchone()
        impact_factor_weights_id = impact_factor_weights[0]
        impact_factor_values_data_time_stamp = impact_factor_weights[1]

        # Get the 8 latest Scheme Type Weight Code ID from the last record in scheme_type_weight table
        datamartConnector.execute('''
            SELECT scheme_type_code_id, timestamp FROM scheme_type ORDER BY scheme_type_code_id DESC LIMIT 8;
        ''')
        scheme_type_weights = datamartConnector.fetchall()

        # Enter a record for each of the 29 KRIs for each of the 8 Scheme Type Weight Code IDs
        for schemeType in range(8):
            scheme_type_weight_code_id = scheme_type_weights[schemeType][0]
            scheme_type_weight_data_time_stamp = scheme_type_weights[schemeType][1]
            for KRI in range(29):
                kri_id = risks[KRI][0]
                risk_category_id = risks[KRI][1]
                risk_category_data_time_stamp = risks[KRI][2]
                datamartConnector.execute('''
                    INSERT INTO calculation_parameter_value (calculation_parameter_value_id, scheme_ri_calc_id, 
                    impact_factor_weights_id, impact_factor_values_data_time_stamp, scheme_type_weight_code_id, 
                    scheme_type_weight_data_time_stamp, risk_category_id, risk_category_data_time_stamp, kri_id, 
                    risk_data_time_stamp)
                    VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                ''', (calc_id, impact_factor_weights_id, impact_factor_values_data_time_stamp,
                      scheme_type_weight_code_id, scheme_type_weight_data_time_stamp, risk_category_id,
                      risk_category_data_time_stamp, kri_id, risk_category_data_time_stamp))
        datamartDB.commit()

    except Exception as e:
        print('Error in insertCalculationParameterValue: ' + str(e))

def insertData(datamartDB, schemesList, trusteesList, schemeTypesList, risksList, riskCategoriesList,
               schemeRIList, trusteeRIList, statDataList, statKISumList):
    """
    :param datamartDB: connection to the datamart database
    :param schemesList: list of schemes
    :param trusteesList: list of trustees
    :param schemeTypesList: list of scheme types
    :param risksList: list of risks
    :param riskCategoriesList: list of risk categories
    :param schemeRIList: list of scheme risk indicators
    :param trusteeRIList: list of trustee risk indicators
    :param statDataList: list of statistical data
    :param statKISumList: list of statistical KI sums
    :return: None
    """

    # Calculate Stat KI Sums per calculation ID per stat KI ID
    print('Updating Stat KI Sums')
    for statKISum in statKISumList:
        try:
            insert_Statistical_KI_Sum(datamartDB, statKISum)
        except Exception as e:
            print(e)

    # Insert all trustee risk indicators into the datamart database
    print('Updating trustee risk indicators')
    for trusteeRI in trusteeRIList:
        try:
            insert_Trustee_RI(datamartDB, trusteeRI)
        except Exception as e:
            print(e)

    # Insert all statistical data into the datamart database
    print('Updating statistical data')
    if statDataList:
        for statData in statDataList:
            try:
                insert_Statistical_Data(datamartDB, statData)
            except Exception as e:
                print(e)

    # Insert all risk categories into the datamart database
    print('Updating risk categories')
    for riskCategory in riskCategoriesList:
        try:
            insert_Risk_Category(datamartDB, riskCategory)
        except Exception as e:
            print(e)

    # Insert all scheme risk indicators into the datamart database
    print('Updating scheme risk indicators')
    for schemeRI in schemeRIList:
        try:
            insert_Scheme_RI(datamartDB, schemeRI)
        except Exception as e:
            print(e)

    # Insert all schemes into the datamart database
    print('Updating schemes')
    for scheme in schemesList:
        try:
            insert_Scheme(datamartDB, scheme)
        except Exception as e:
            print(e)

    # Insert all trustees into the datamart database
    print('Updating trustees')
    for trustee in trusteesList:
        try:
            insert_Trustee(datamartDB, trustee)
        except Exception as e:
            print(e)

    # Insert all scheme types into the datamart database
    print('Updating scheme types')
    for schemeType in schemeTypesList:
        try:
            insert_Scheme_Type(datamartDB, schemeType)
        except Exception as e:
            print(e)

    # Insert all risks into the datamart database
    print('Updating risks')
    for risk in risksList:
        try:
            insert_Risk(datamartDB, risk)
        except Exception as e:
            print(e)

    insertCalculationParameterValue(datamartDB)

def main():
    # Connect to datamart database
    datamartDB = connect(datamart_DatabaseInfo)
    insertCalculationParameterValue(datamartDB)
    # Connect to all databases
    businessDB, partnerDB, stageDB = connectToDatabases()

    # Retrieve all data from the databases
    schemesList, trusteesList, schemeTypesList, risksList, riskCategoriesList, \
        schemeRIList, trusteeRIList, statDataList, statKISumList = retrieveData(businessDB, partnerDB)

    # Set database connections auto commit to true
    datamartDB.rollback()
    datamartDB.autocommit = True

    # Insert all data into the datamart database
    insertData(datamartDB, schemesList, trusteesList, schemeTypesList, risksList, riskCategoriesList,
               schemeRIList, trusteeRIList, statDataList, statKISumList)


if __name__ == '__main__':
    main()
