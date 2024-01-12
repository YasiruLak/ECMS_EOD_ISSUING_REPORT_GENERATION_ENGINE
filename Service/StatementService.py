import os
from Utils import *
from app import app
import platform


def cardNumberMasking(card_number):
    global masked_cardnumber
    try:
        pattern = PATTERN_CHAR[0] * (END_INDEX - START_INDEX)
        masked_cardnumber = card_number[:START_INDEX] + pattern + card_number[END_INDEX:]

    except Exception as err:
        app.logger.error('Error while masking card number  {}'.format(str(err)))
    return masked_cardnumber


def genarateCustomerStatement(accNo, eodDate, statementid):
    try:
        os_name = platform.system()
        # datetemp = str(eodDate)
        # date = "20" + datetemp[0:2] + "-" + datetemp[2:4] + "-" + datetemp[4:6]

        if os_name.upper() == 'WINDOWS':
            filepath = STATEMENT_FILE_PATH + '\\' + accNo + '\\'
        else:
            filepath = STATEMENT_FILE_PATH + '/' + accNo + '/'

        # Create a new directory  in the 'C:\' directory
        path = os.path.join(filepath)
        if not os.path.exists(path):
            os.makedirs(path)
            app.logger.info(accNo + ': directory was created')
        else:
            app.logger.info(accNo + ' :directory already exists.')

        filename = filepath + str(statementid) + ".pdf"
        # app.logger.info('successfully created ' + filename)

        return filename, filepath

    except Exception as err:
        app.logger.error('Error in Service {}'.format(str(err)))
