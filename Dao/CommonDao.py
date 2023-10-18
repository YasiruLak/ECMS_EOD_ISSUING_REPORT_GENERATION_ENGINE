import pandas as pd
from app import app

# import dbconnection
from DatabaseConnection import *
from Utils.Configuration import *


def UpdateEodMerchantPaymentTableBillingDone(merchantCusNo, type):

    global status, sql

    try:
        con = conn()
        cursor = con.cursor()

        if type == 'Location':

            status = 'MLBC'
            sql = ''' UPDATE EODMERCHANTPAYMENT SET STATUS = :status WHERE MERCHANTID = :merchantCusNo AND STATUS = 'EPEN'  '''

        elif type == 'Customer':

            status = 'MCBC'
            sql = '''UPDATE EODMERCHANTPAYMENT SET STATUS = :status WHERE MERCHANTCUSTID = :merchantCusNo AND STATUS = 'EPEN'  '''

        values = (status, merchantCusNo)

        cursor.execute(sql, values)

        con.commit()
        cursor.close()
        con.close()

    except Exception as err:
        app.logger.error('Error while getting data for fees sub report '.format(str(err)))