from flask import send_file, request
from View.PDF import *
from app import app
from flask import jsonify


@app.route('/eod-engine/customerStatement', methods=['POST'])
def generateCustomerStatementController():
    app.logger.info('Request Recived')
    global successno, errorno

    eodDate = request.args.get("eodDate")
    startingEodStatus = request.args.get("eodStatus")

    try:
        successno, errorno = tobeGenerateCustomerStatementFile(eodDate, startingEodStatus)

    except Exception as err:
        app.logger.error('Error in customer controller {}'.format(str(err)))

    data = {'successno': successno, 'errorno': errorno}

    print(successno, errorno)
    return jsonify(data)



