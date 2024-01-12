import concurrent

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Spacer
from reportlab.graphics import shapes
from multiprocessing.dummy import Pool as ThreadPool
import threading
from concurrent.futures import ThreadPoolExecutor
import requests
import pandas as pd
from datetime import datetime

import Dao
import Service
from Dao import *
from Dao import CustomerStatementDao
from app import app

pool = ThreadPool(4)


def tobeGenerateCustomerStatementFile(eodDate):
    global successno, errorno, start, end, batchSize, temp, processed_statement_ids, statementenddate, statementstartdate

    try:

        count = Dao.totalStmtGenerationCount()

        successno = 0
        errorno = 0
        start = 0
        end = count
        batchSize = 50

        df2 = Dao.getStatementIdsForStatementFileCreation(start, end)

        app.logger.info('Statement Count : ' + format(str(count)))

        processed_statement_ids = set()

        for ind in df2.index:
            errorcount = errorno
            successcount = successno
            statementid = df2['stmtid'][ind]

            if statementid not in processed_statement_ids:
                successno, errorno = genarateCustomerStatement(statementid, eodDate, errorcount, successcount)

                processed_statement_ids.add(statementid)

        processed_statement_ids.clear()
    except Exception as err:
        app.logger.error('Error in Customer Statement Generate controller {}'.format(str(err)))

    processed_statement_ids.clear()
    return successno, errorno


def genarateCustomerStatement(statementid, eodDate, errorcount, successcount):
    try:
        # app.logger.info(statementid)
        # get data from db
        df1 = CustomerStatementDao.getdataFromMainQuery(statementid)
        # df1 = CustomerStatementDao.getStatementIdsForStatementFileCreation(startEodStatus, start, end)
        filename, filepath = Service.genarateCustomerStatement(df1["accountno"].values[0], eodDate, statementid)

        # get billing address and name
        name, address1, address2, address3 = CustomerStatementDao.getBillingAddress(df1["cardcategorycode"].values[0],
                                                                                    df1["cardno"].values[0])

        df2 = CustomerStatementDao.getDatafromSecondQuery(df1["accountno"].values[0], df1["starteodid"].values[0],
                                                          df1["endeodid"].values[0])

        # define page size and create a SimpleDocTemplate  object
        bottom_margin = 0.5 * inch
        top_margin = 0.4 * inch
        left_margin = 0.5 * inch
        right_margin = 0.5 * inch
        doc = SimpleDocTemplate(filename=filename, pagesize=letter, bottomMargin=bottom_margin,
                                topMargin=top_margin, leftMargin=left_margin, rightMargin=right_margin)
        # row headers
        row_x = 0
        row_y = 0
        row_width = 7.5 * inch
        row_height = 20
        # row = shapes.Rect(row_x, row_y, row_width, row_height, fillColor=colors.white, strokeColor=colors.white)

        # create the content for the template
        elements = []

        # title
        customer_name = shapes.Drawing(row_width, 10)
        address_row_1 = shapes.Drawing(row_width, 10)
        address_row_2 = shapes.Drawing(row_width, 10)
        address_row_3 = shapes.Drawing(row_width, 8)

        if name is None:
            name = ''
        else:
            name = name
        customer = shapes.String(0, 8, name, fontName="Helvetica-Bold", fontSize=8, fillColor=colors.black)
        customer_name.add(customer)
        elements.append(customer_name)

        if address1 is None:
            address1 = ''
        else:
            address1 = address1
        addr1 = shapes.String(0, 8, address1, fontName="Helvetica", fontSize=8, fillColor=colors.black)
        address_row_1.add(addr1)
        elements.append(address_row_1)

        if address2 is None:
            address2 = ''
        else:
            address2 = address2
        addr2 = shapes.String(0, 8, address2, fontName="Helvetica", fontSize=8, fillColor=colors.black)
        address_row_2.add(addr2)
        elements.append(address_row_2)

        if address3 is None:
            address3 = ''
        else:
            address3 = address3
        addr3 = shapes.String(0, 8, address3, fontName="Helvetica", fontSize=8, fillColor=colors.black)
        address_row_3.add(addr3)
        elements.append(address_row_3)

        # horizontal line
        horizontal_line = shapes.Drawing(row_width, 5)
        row_1 = shapes.Rect(row_x, row_y, row_width, 1, fillColor=colors.black, strokeColor=colors.black)
        horizontal_line.add(row_1)
        elements.append(horizontal_line)

        ## page header
        header_row_1 = shapes.Drawing(row_width, 10)
        string_cardnumber = shapes.String(0, 0, 'Card Number', fontName="Helvetica-Bold", fontSize=8)
        # header_row_1.add(row)
        header_row_1.add(string_cardnumber)

        string_date = shapes.String(1.5 * inch, 0, 'Statement Date', fontName="Helvetica-Bold", fontSize=8)
        header_row_1.add(string_date)

        string_date = shapes.String(3 * inch, 0, 'Total Outstanding', fontName="Helvetica-Bold", fontSize=8)
        header_row_1.add(string_date)

        string_date = shapes.String(4.5 * inch, 0, 'Credit limit', fontName="Helvetica-Bold", fontSize=8)
        header_row_1.add(string_date)

        string_date = shapes.String(5.5 * inch, 0, 'Due Date', fontName="Helvetica-Bold", fontSize=8)
        header_row_1.add(string_date)

        string_date = shapes.String(6.5 * inch, 0, 'Minimum Payment', fontName="Helvetica-Bold", fontSize=8)
        header_row_1.add(string_date)

        elements.append(header_row_1)

        # row 2
        header_row_2 = shapes.Drawing(row_width, 15)
        cardno = card_number_masking(str(df1['cardno'].values[0]))
        parameter_cardnumber = shapes.String(0, 0, cardno, fontName="Helvetica", fontSize=8)
        # header_row_1.add(row)
        header_row_2.add(parameter_cardnumber)

        # Assuming df1["statementenddate"].values[0] is a string representing a date in the format "YYYY-MM-DD"
        date_str = str(df1["statementenddate"].values[0])[:10]
        date_object = datetime.strptime(date_str, "%Y-%m-%d")

        # Format the date as "YYYY/MM/DD"
        formatted_date = date_object.strftime("%Y/%m/%d")

        parameter_date = shapes.String(1.5 * inch, 0, str(formatted_date), fontName="Helvetica",
                                       fontSize=8)
        header_row_2.add(parameter_date)

        # $P{ClosingBalance}<0 ? " CR" : " "
        if df1["closingbalance"].values[0] < 0:
            addon = 'CR'
        else:
            addon = ' '
        parameter_date = shapes.String(3 * inch, 0, str(df1["closingbalance"].values[0]) + ' ' + addon,
                                       fontName="Helvetica", fontSize=8)
        header_row_2.add(parameter_date)

        credit_limit = round(df1["creditlimit"].values[0], 2)

        parameter_date = shapes.String(4.5 * inch, 0, str(credit_limit), fontName="Helvetica",
                                       fontSize=8)
        header_row_2.add(parameter_date)

        date_str = str(df1["duedate"].values[0])[:10]
        date_object = datetime.strptime(date_str, "%Y-%m-%d")

        # Format the date as "YYYY/MM/DD"
        formatted_due_date = date_object.strftime("%Y/%m/%d")

        parameter_date = shapes.String(5.5 * inch, 0, str(formatted_due_date), fontName="Helvetica",
                                       fontSize=8)
        header_row_2.add(parameter_date)

        parameter_date = shapes.String(6.5 * inch, 0, str(df1["minamount"].values[0]), fontName="Helvetica", fontSize=8)
        header_row_2.add(parameter_date)

        elements.append(header_row_2)
        elements.append(horizontal_line)

        # row 3
        header_row_3 = shapes.Drawing(row_width, 10)
        string_cardnumber = shapes.String(0, 0, 'Page Number', fontName="Helvetica-Bold", fontSize=8)
        # header_row_3.add(row)
        header_row_3.add(string_cardnumber)

        string_date = shapes.String(1.5 * inch, 0, 'Opening Balance', fontName="Helvetica-Bold", fontSize=8)
        header_row_3.add(string_date)

        string_date = shapes.String(3.5 * inch, 0, 'Debits', fontName="Helvetica-Bold", fontSize=8)
        header_row_3.add(string_date)

        string_date = shapes.String(5 * inch, 0, 'Credits', fontName="Helvetica-Bold", fontSize=8)
        header_row_3.add(string_date)

        string_date = shapes.String(6.5 * inch, 0, 'Total Outstanding', fontName="Helvetica-Bold", fontSize=8)
        header_row_3.add(string_date)

        elements.append(header_row_3)

        # header row 4
        header_row_4 = shapes.Drawing(row_width, 15)
        parameter_cardnumber = shapes.String(0, 0, 'Page 1 of 1', fontName="Helvetica", fontSize=8)
        # header_row_1.add(row)
        header_row_4.add(parameter_cardnumber)

        if df1["openingbalance"].values[0] < 0:
            opening_balance = -1 * df1["openingbalance"].values[0]
            addon = 'CR'
        else:
            opening_balance = df1["openingbalance"].values[0]
            addon = ' '
            # $P{OpenBalance}<0 ? " CR" : " "
        parameter_date = shapes.String(1.5 * inch, 0, str(opening_balance) + ' ' + addon, fontName="Helvetica",
                                       fontSize=8)
        header_row_4.add(parameter_date)

        outstanding_ttl = df1["purchases"].values[0] + df1["cashadvance"].values[0] + df1["interest"].values[0] + \
                          df1["dradjustment"].values[0] + df1["charges"].values[0]

        outstanding_ttl_rounded = round(outstanding_ttl, 2)

        parameter_date = shapes.String(3.5 * inch, 0, str(outstanding_ttl_rounded), fontName="Helvetica", fontSize=8)
        header_row_4.add(parameter_date)

        parameter_date = shapes.String(5 * inch, 0, str(df1["payment"].values[0]), fontName="Helvetica", fontSize=8)
        header_row_4.add(parameter_date)

        if df1["closingbalance"].values[0] < 0:
            closing_balance = -1 * df1["closingbalance"].values[0]
            addon = 'CR'
        else:
            closing_balance = df1["closingbalance"].values[0]
            addon = ' '
        parameter_date = shapes.String(6.5 * inch, 0, str(closing_balance) + ' ' + addon, fontName="Helvetica",
                                       fontSize=8)
        header_row_4.add(parameter_date)

        elements.append(header_row_4)
        elements.append(horizontal_line)
        # end page header

        # column header
        column_row = shapes.Drawing(row_width, 15)
        string_billing_date = shapes.String(0, 0, 'Billed Date', fontName="Helvetica-Bold", fontSize=8)
        column_row.add(string_billing_date)

        string_txn_date = shapes.String(1 * inch, 0, 'Txn Date', fontName="Helvetica-Bold", fontSize=8)
        column_row.add(string_txn_date)

        string_description = shapes.String(2 * inch, 0, 'Transaction Description', fontName="Helvetica-Bold",
                                           fontSize=8)
        column_row.add(string_description)

        string_transaction_amount = shapes.String(6.5 * inch, 0, 'Transaction Amount', fontName="Helvetica-Bold",
                                                  fontSize=8)
        column_row.add(string_transaction_amount)
        elements.append(column_row)
        elements.append(horizontal_line)

        # detail
        total_txn_amount = 0.00
        txn = 0.00
        txn_amount = 0.00
        cr_amount = 0.00
        # get data from query 2
        for ind in df2.index:

            if df2['transactionamount'][ind] is not None and int(df2['transactionamount'][ind]) > 0:

                # row 1
                detail_row1 = shapes.Drawing(row_width, 15)
                string_billing_date = shapes.String(0, 0, str(df2['settlementdate'][ind])[6:10], fontName="Helvetica",
                                                    fontSize=8)
                detail_row1.add(string_billing_date)

                string_txn_date = shapes.String(1 * inch, 0, str(df2['transactiondate'][ind])[6:10],
                                                fontName="Helvetica",
                                                fontSize=8)
                detail_row1.add(string_txn_date)

                string_description = shapes.String(2 * inch, 0, str(df2['transactiondescription'][ind]),
                                                   fontName="Helvetica",
                                                   fontSize=8)
                detail_row1.add(string_description)

                if df2['crdr'][ind] == 'CR':
                    addon = " CR"
                    cr_amount = df2['transactionamount'][ind]

                    print("cr : " + str(cr_amount))
                else:
                    addon = " "

                string_transaction_amount = shapes.String(6.5 * inch, 0, str(df2['transactionamount'][ind]) + addon,
                                                          fontName="Helvetica",
                                                          fontSize=8)
                detail_row1.add(string_transaction_amount)
                elements.append(detail_row1)

                if df2['transactionamount'][ind] is None:
                    txn_amount = 0.00
                else:
                    txn_amount = df2['transactionamount'][ind] + txn_amount

                    print("txn_amount : " + str(txn_amount))

                round(total_txn_amount, 2)

                txn = txn_amount - cr_amount

                print("txn : " + str(txn))
                round(txn, 2)

        if df2['cashadvancefee'][ind] is not None and int(df2['cashadvancefee'][ind]) > 0:
            # # row 2
            detail_row2 = shapes.Drawing(row_width, 15)

            date_str = str(df2['settlementdate'][ind][:10])
            # Split the date string into month and day
            month, day = map(int, date_str.split('-'))

            # Create a datetime object with a dummy year (e.g., 2000)
            date_object = datetime(year=2000, month=month, day=day)

            # Format the date as "M/DD"
            settlemet_d = date_object.strftime("%m/%d")
            print(settlemet_d)

            string_billing_date = shapes.String(0, 0, str(settlemet_d)[:10], fontName="Helvetica",
                                                fontSize=8)
            detail_row2.add(string_billing_date)

            # Assuming df1["duedate"].values[0] is in the format "8-23"
            date_str = str(df2['transactiondate'][ind][:10])
            # Split the date string into month and day
            month, day = map(int, date_str.split('-'))

            # Create a datetime object with a dummy year (e.g., 2000)
            date_object = datetime(year=2000, month=month, day=day)

            # Format the date as "M/DD"
            txn_date = date_object.strftime("%m/%d")
            print(txn_date)

            string_txn_date = shapes.String(1 * inch, 0, str(txn_date)[:10], fontName="Helvetica",
                                            fontSize=8)
            detail_row2.add(string_txn_date)

            string_description = shapes.String(2 * inch, 0, 'CASH ADVANCE FEE', fontName="Helvetica",
                                               fontSize=8)
            detail_row2.add(string_description)

            string_transaction_amount = shapes.String(6.5 * inch, 0, str(df2['cashadvancefee'][ind]) + addon,
                                                      fontName="Helvetica",
                                                      fontSize=8)
            detail_row2.add(string_transaction_amount)

            elements.append(detail_row2)

            # # row 3
        if txn is not None and int(txn) > 0:
            cardno = card_number_masking(str(df2['cardnumber'][ind]))
            detail_row3 = shapes.Drawing(row_width, 15)
            string_txn_date = shapes.String(2 * inch, 0, cardno, fontName="Helvetica-Bold", fontSize=8)
            detail_row3.add(string_txn_date)

            name_on_card = "--" if df2['nameoncard'][ind] is None else df2['nameoncard'][ind]
            string_description = shapes.String(3.0 * inch, 0, name_on_card, fontName="Helvetica-Bold",
                                               fontSize=8)
            detail_row3.add(string_description)

            string_transaction_amount = shapes.String(4.5 * inch, 0, 'SUB TOTAL', fontName="Helvetica",
                                                      fontSize=8)
            detail_row3.add(string_transaction_amount)

            string_debits = shapes.String(5.5 * inch, 0, '-DEBITS', fontName="Helvetica",
                                          fontSize=8)
            detail_row3.add(string_debits)

            # $V{total card debits}==null ? 0.00 : ($V{total card debits}  - $V{total_refunds})
            string_ttl_debits = shapes.String(6.5 * inch, 0, str(round(txn, 2)), fontName="Helvetica",
                                              fontSize=8)
            detail_row3.add(string_ttl_debits)
            elements.append(detail_row3)
            # end

        #### sub report two
        # get data to sub report two
        df3 = CustomerStatementDao.get_data_for_subreport_two(df1["cardno"].values[0])
        sub_report_two(df3, row_width, elements, str(df1["statementenddate"].values[0])[6:10], df2)

        #### sub report one
        df4 = CustomerStatementDao.get_data_for_subreport_one(df1["cardno"].values[0],
                                                              str(df1["statementenddate"].values[0])[:10])
        sub_report_one(df4, elements, row_width, df2)

        # account group footer1
        # account footer row 1

        # # Add white space (Spacer) with a height of 0.5 inch
        # elements.append(Spacer(1, 5 * inch))

        if df2['avlcbamount'][ind] is not None and int(df2['avlcbamount'][ind]) > 0:
            account_footer_row1 = shapes.Drawing(row_width, 15)
            string_txn_date = shapes.String(0 * inch, 0, 'CashBack Rewards', fontName="Helvetica-Bold", fontSize=8)
            account_footer_row1.add(string_txn_date)
            elements.append(account_footer_row1)

            # row 2
            account_footer_row2 = shapes.Drawing(row_width, 15)
            string_txn_date = shapes.String(0 * inch, 0, 'Opening Balance', fontName="Helvetica", fontSize=8)
            account_footer_row2.add(string_txn_date)

            string_cashback = shapes.String(1.5 * inch, 0, 'CashBack for this Statement', fontName="Helvetica",
                                            fontSize=8)
            account_footer_row2.add(string_cashback)

            string_redeemed = shapes.String(4 * inch, 0, 'Redeemed', fontName="Helvetica", fontSize=8)
            account_footer_row2.add(string_redeemed)

            string_expired = shapes.String(5 * inch, 0, 'Expired/Adjusted', fontName="Helvetica", fontSize=8)
            account_footer_row2.add(string_expired)

            string_ttl_cashback = shapes.String(6.5 * inch, 0, 'Total CashBack', fontName="Helvetica", fontSize=8)
            account_footer_row2.add(string_ttl_cashback)
            elements.append(account_footer_row2)

            # row 3
            space = '            '
            account_footer_row3 = shapes.Drawing(row_width, 15)
            # $F{OPENNINGCASHBACK} == null ? 0 : $F{OPENNINGCASHBACK}
            cashback = 0.00 if df2['openningcashback'][ind] is None else df2['openningcashback'][ind]
            string_txn_date = shapes.String(0.25 * inch, 0, str(cashback) + space + '     +', fontName="Helvetica",
                                            fontSize=8)
            account_footer_row3.add(string_txn_date)

            # $F{CASHBACKAMOUNTWITHOUTADJ}
            string_cashback = shapes.String(1.75 * inch, 0,
                                            str(round(df2['cashbackamountwithoutadj'][ind], 2)) + space + '          -',
                                            fontName="Helvetica",
                                            fontSize=8)
            account_footer_row3.add(string_cashback)

            string_redeemed = shapes.String(4 * inch, 0,
                                            str(round(df2['redeemtotalcb'][ind], 2)) + space + '      -',
                                            fontName="Helvetica",
                                            fontSize=8)
            account_footer_row3.add(string_redeemed)

            # $F{CBEXPAMOUNTWITHADJ}
            string_expired = shapes.String(5.25 * inch, 0,
                                           str(round(df2['cbexpamountwithadj'][ind], 2)) + space + '          =',
                                           fontName="Helvetica",
                                           fontSize=8)
            account_footer_row3.add(string_expired)

            # $F{AVLCBAMOUNT}avlcbamount
            string_ttl_cashback = shapes.String(6.75 * inch, 0, str(round(df2['avlcbamount'][ind], 2)),
                                                fontName="Helvetica",
                                                fontSize=8)
            account_footer_row3.add(string_ttl_cashback)
            elements.append(account_footer_row3)

            # row 4
            account_footer_row4 = shapes.Drawing(row_width, 30)
            string_credited = shapes.String(0 * inch, 15, 'Total CashBack to be', fontName="Helvetica", fontSize=8)
            string_txn_date = shapes.String(0.4 * inch, 0, 'credited', fontName="Helvetica", fontSize=8)
            account_footer_row4.add(string_txn_date)
            account_footer_row4.add(string_credited)

            # outstanding_ttl_rounded = round(outstanding_ttl, 2)

            # redeem_cash_balane(round(df2['redeemablecashback'][ind],2))
            # $F{REDEEMABLECASHBACK}
            string_redeemablecashback = shapes.String(1.5 * inch, 10,
                                                      ":   " + (str(round(df2['redeemablecashback'][ind], 2))),
                                                      fontName="Helvetica")
            account_footer_row4.add(string_redeemablecashback)
            elements.append(account_footer_row4)

            # row 5
            account_footer_row5 = shapes.Drawing(row_width, 15)
            string_cashback = shapes.String(0.3 * inch, 0, 'CashBack Credit Account', fontName="Helvetica-Bold",
                                            fontSize=8)
            account_footer_row5.add(string_cashback)
            elements.append(account_footer_row5)

            # row 6
            account_footer_row6 = shapes.Drawing(row_width, 15)
            # df2['cbaccountname'][ind] == null ? "--" : df2['cbaccountname'][ind]
            acc_holder = "--" if df2['cbaccountname'][ind] is None else df2['cbaccountname'][ind]
            string_acc_holder = shapes.String(0.3 * inch, 0, 'Account Holder :  ' + acc_holder,
                                              fontName="Helvetica-Bold",
                                              fontSize=8)
            account_footer_row6.add(string_acc_holder)
            elements.append(account_footer_row6)

            # row 7
            account_footer_row7 = shapes.Drawing(row_width, 15)
            acc_no = "--" if df2['cbaccountno'][ind] is None else df2['cbaccountno'][ind]
            string_acc_holder = shapes.String(0.3 * inch, 0, 'Account No :  ' + acc_no, fontName="Helvetica-Bold",
                                              fontSize=8)
            account_footer_row7.add(string_acc_holder)
            elements.append(account_footer_row7)

            # row 8
            account_footer_row8 = shapes.Drawing(row_width, 15)
            string_acc_holder = shapes.String(0.3 * inch, 0,
                                              'Total CashBack amount indicated above will be credited within 30 days. '
                                              'Conditions apply.',
                                              fontName="Helvetica-Bold", fontSize=8)
            account_footer_row8.add(string_acc_holder)
            elements.append(account_footer_row8)
            # end cashback

        # # Add white space (Spacer) with a height of 0.5 inch
        # elements.append(Spacer(1, 5 * inch))
        #
        # # Add a Spacer to push the footer to the bottom
        # footer_height = 1.0 * inch  # Adjust the height as needed
        # elements.append(Spacer(1, footer_height))

        elements.append(horizontal_line)

        # footer
        # row 1
        footer_row1 = shapes.Drawing(row_width, 15)
        string_acc_holder = shapes.String(0.0 * inch, 0, 'Cardholder Name', fontName="Helvetica-Bold", fontSize=8)
        footer_row1.add(string_acc_holder)

        string_outstanding = shapes.String(6.5 * inch, 0, 'Total Outstanding', fontName="Helvetica-Bold", fontSize=8)
        footer_row1.add(string_outstanding)
        elements.append(footer_row1)

        # row 2
        footer_row2 = shapes.Drawing(row_width, 15)
        string_acc_holder = shapes.String(0.0 * inch, 0, name, fontName="Helvetica-Bold", fontSize=8)
        footer_row2.add(string_acc_holder)

        add = "  CR" if df1["closingbalance"].values[0] < 0 else " "
        string_acc_holder = shapes.String(6.5 * inch, 0, str(df1["closingbalance"].values[0]) + add,
                                          fontName="Helvetica", fontSize=8)
        footer_row2.add(string_acc_holder)
        elements.append(footer_row2)

        # row 3
        footer_row3 = shapes.Drawing(row_width, 15)
        string_acc_holder = shapes.String(0.0 * inch, 0, 'Card Number', fontName="Helvetica-Bold", fontSize=8)
        footer_row3.add(string_acc_holder)

        string_acc_holder = shapes.String(6.5 * inch, 0, 'Credit Limit', fontName="Helvetica-Bold", fontSize=8)
        footer_row3.add(string_acc_holder)
        elements.append(footer_row3)

        # row 4
        footer_row4 = shapes.Drawing(row_width, 15)
        cardno = card_number_masking(str(df1['cardno'].values[0]))
        string_acc_holder = shapes.String(0.0 * inch, 0, cardno, fontName="Helvetica", fontSize=8)
        footer_row4.add(string_acc_holder)

        credit_limit = round(df1['creditlimit'].values[0], 2)

        string_acc_holder = shapes.String(6.5 * inch, 0, str(credit_limit), fontName="Helvetica",
                                          fontSize=8)
        footer_row4.add(string_acc_holder)
        elements.append(footer_row4)

        # row 5
        footer_row5 = shapes.Drawing(row_width, 15)
        string_acc_holder = shapes.String(0.0 * inch, 0, 'Due Date', fontName="Helvetica-Bold", fontSize=8)
        footer_row5.add(string_acc_holder)

        string_acc_holder = shapes.String(6.5 * inch, 0, 'Minimum Payment', fontName="Helvetica-Bold", fontSize=8)
        footer_row5.add(string_acc_holder)
        elements.append(footer_row5)

        date_str = str(df1["duedate"].values[0])[:10]
        date_object = datetime.strptime(date_str, "%Y-%m-%d")

        # Format the date as "YYYY/MM/DD"
        footer_due_date = date_object.strftime("%Y/%m/%d")

        # row 6
        footer_row6 = shapes.Drawing(row_width, 15)
        string_acc_holder = shapes.String(0.0 * inch, 0, str(footer_due_date)[:10], fontName="Helvetica",
                                          fontSize=8)
        footer_row6.add(string_acc_holder)

        string_acc_holder = shapes.String(6.5 * inch, 0, str(df1["minamount"].values[0]), fontName="Helvetica",
                                          fontSize=8)
        footer_row6.add(string_acc_holder)
        elements.append(footer_row6)

        # build the template
        doc.build(elements)

        # doc.build(elements, onFirstPage=addPageNumber, onLaterPages=addPageNumber)
        app.logger.info('successfully created : ' + filename)
        successcount += 1
        Dao.updateStatus(format(str(statementid)))

    except Exception as err:
        app.logger.error(
            'Error while generating  Customer Statement PDF  : ' + format(str(statementid)).format(str(err)))
        errorcount += 1
        Dao.updateErrorFileStatus(format(str(statementid)))

    return successcount, errorcount


def card_number_masking(card_number):
    global masked_cardnumber
    try:
        pattern = PATTERN_CHAR[0] * (END_INDEX - START_INDEX)
        masked_cardnumber = card_number[:START_INDEX] + pattern + card_number[END_INDEX:]

    except Exception as err:
        app.logger.error('Error while masking card number : ' + format(str(masked_cardnumber)).format(str(err)))
    return masked_cardnumber


def sub_report_two(df3, row_width, elements, statementenddate, df2):
    try:
        total_fee_r2 = 0
        total_interest_r2 = 0
        # sub report two loop
        for ind in df3.index:
            # row 1
            # print(df3['cardnumber'][ind], df3['interrest'][ind], ind)

            if df3['feeamount'][ind] is not None:
                column_row = shapes.Drawing(row_width, 15)
                string_billing_date = shapes.String(0, 0, str(df3['effectdate'][ind])[6:10], fontName="Helvetica",
                                                    fontSize=8)
                column_row.add(string_billing_date)

                string_txn_date = shapes.String(1 * inch, 0, str(df3['effectdate'][ind])[6:10], fontName="Helvetica",
                                                fontSize=8)
                column_row.add(string_txn_date)

                string_description = shapes.String(2 * inch, 0, str(df3['description'][ind]).upper(),
                                                   fontName="Helvetica",
                                                   fontSize=8)
                column_row.add(string_description)

                # $F{FEEAMOUNT}==null ? 0.00 : $F{FEEAMOUNT}
                feeamount = 0.00 if df3['feeamount'][ind] is None else df3['feeamount'][ind]
                total_fee_r2 += feeamount
                string_transaction_amount = shapes.String(6.5 * inch, 0, str(feeamount), fontName="Helvetica",
                                                          fontSize=8)
                column_row.add(string_transaction_amount)
                elements.append(column_row)

            # if df3['interrest'][ind] is None:
            #     interest = 0.00
            # else:
            #     interest = df3['interrest'][ind]
            # total_interest_r2 += interest

            # row two
            # column_row = shapes.Drawing(row_width, 15)
            # string_billing_date = shapes.String(0, 0, statementenddate, fontName="Helvetica", fontSize=8)
            # column_row.add(string_billing_date)
            #
            # string_txn_date = shapes.String(1 * inch, 0, statementenddate, fontName="Helvetica", fontSize=8)
            # column_row.add(string_txn_date)
            #
            # string_description = shapes.String(2 * inch, 0, 'INTEREST CHARGE', fontName="Helvetica",
            #                                    fontSize=8)
            # column_row.add(string_description)
            #
            # # $F{INTERREST}==null ? 0.00 : $F{INTERREST}
            # # interest = 0.00 if df3['interrest'][ind] is None else df3['interrest'][ind]
            # if df3['interrest'][ind] is None:
            #     interest = 0.00
            # else:
            #     interest = df3['interrest'][ind]
            # total_interest_r2 += interest
            # string_transaction_amount = shapes.String(6.5 * inch, 0, str(interest), fontName="Helvetica",
            #                                           fontSize=8)
            # column_row.add(string_transaction_amount)
            # elements.append(column_row)

        # row two
        if df3['interrest'][ind] is not None and int(df3['interrest'][ind]) > 0:

            column_row = shapes.Drawing(row_width, 15)
            string_billing_date = shapes.String(0, 0, statementenddate, fontName="Helvetica", fontSize=8)
            column_row.add(string_billing_date)

            string_txn_date = shapes.String(1 * inch, 0, statementenddate, fontName="Helvetica", fontSize=8)
            column_row.add(string_txn_date)

            string_description = shapes.String(2 * inch, 0, 'INTEREST CHARGE', fontName="Helvetica",
                                               fontSize=8)
            column_row.add(string_description)

            # $F{INTERREST}==null ? 0.00 : $F{INTERREST}
            # interest = 0.00 if df3['interrest'][ind] is None else df3['interrest'][ind]
            if df3['interrest'][ind] is None:
                interest = 0.00
            else:
                interest = df3['interrest'][ind]
            total_interest_r2 += interest
            string_transaction_amount = shapes.String(6.5 * inch, 0, str(interest), fontName="Helvetica",
                                                      fontSize=8)
            column_row.add(string_transaction_amount)
            elements.append(column_row)

            # row 3
            cardno = card_number_masking(str(df3['cardnumber'][ind]))
            detail_row3 = shapes.Drawing(row_width, 15)
            string_txn_date = shapes.String(2 * inch, 0, cardno, fontName="Helvetica-Bold", fontSize=8)
            detail_row3.add(string_txn_date)

            name_on_card = "--" if df2['nameoncard'].values[0] is None else df2['nameoncard'].values[0]
            string_description = shapes.String(3.0 * inch, 0, name_on_card, fontName="Helvetica-Bold",
                                               fontSize=8)
            detail_row3.add(string_description)

            string_transaction_amount = shapes.String(4.5 * inch, 0, 'SUB TOTAL', fontName="Helvetica",
                                                      fontSize=8)
            detail_row3.add(string_transaction_amount)

            string_debits = shapes.String(5.5 * inch, 0, '-DEBITS', fontName="Helvetica",
                                          fontSize=8)
            detail_row3.add(string_debits)

            # if df3['interrest'][ind] is None:
            #     interest = 0.00
            # else:
            #     interest = df3['interrest'][ind]
            # total_interest_r2 += interest

            # ($V{TotalFee}==null && $F{INTERREST} ==null) ? 0.00 :
            # ($F{INTERREST}==null &&$V{TotalFee}!= null) ? $V{TotalFee} :
            # ($V{TotalFee}==null &&$F{INTERREST}!=null) ?$F{INTERREST} :
            # ($F{INTERREST}!=null &&$V{TotalFee}!=null) ?$V{TotalFee}+$F{INTERREST}.doubleValue():0.00
            if total_fee_r2 is None and total_interest_r2 is None:
                sub_ttl_r2 = 0.00
            elif total_interest_r2 is None and total_fee_r2 is not None:
                sub_ttl_r2 = total_fee_r2
            elif total_fee_r2 is None and total_interest_r2 is not None:
                sub_ttl_r2 = total_interest_r2
            elif total_fee_r2 is not None and total_interest_r2 is not None:
                sub_ttl_r2 = float(total_fee_r2 + total_interest_r2)
            else:
                sub_ttl_r2 = 0.00

            sub_t = round(sub_ttl_r2, 2)

            string_ttl_debits = shapes.String(6.5 * inch, 0, str(sub_t), fontName="Helvetica",
                                              fontSize=8)
            detail_row3.add(string_ttl_debits)
            elements.append(detail_row3)

    except Exception as err:
        app.logger.error('Error while generating sub report two  {}'.format(str(err)))


def sub_report_one(df4, elements, row_width, df2):
    try:
        totaladj_reportone = 0
        loop = 0
        for ind in df4.index:
            loop = 1
            column_row = shapes.Drawing(row_width, 15)
            string_billing_date = shapes.String(0, 0, str(df4['adjustdate'][ind])[6:10], fontName="Helvetica",
                                                fontSize=8)
            column_row.add(string_billing_date)

            string_txn_date = shapes.String(1 * inch, 0, str(df4['adjustdate'][ind])[6:10], fontName="Helvetica",
                                            fontSize=8)
            column_row.add(string_txn_date)

            # $F{REMARKS}==null ? "" :$F{REMARKS}.toUpperCase()
            if df4['remarks'][ind] is None:
                remark = ''
            else:
                remark = df4['remarks'][ind].upper()
            string_description = shapes.String(2 * inch, 0, remark, fontName="Helvetica",
                                               fontSize=8)
            column_row.add(string_description)

            # $F{CRDR}.equals("CR")?"CR":" "
            if df4['crdr'][ind] == 'CR':
                addon = " CR"
            else:
                addon = " "
            string_transaction_amount = shapes.String(6.5 * inch, 0, str(df4['amount'][ind]) + ' ' + addon,
                                                      fontName="Helvetica",
                                                      fontSize=8)
            totaladj_reportone += df4['amount'][ind]
            column_row.add(string_transaction_amount)
            elements.append(column_row)

        # row 3
        if loop > 0:
            cardno = card_number_masking(str(df2['cardnumber'].values[0]))
            detail_row3 = shapes.Drawing(row_width, 15)
            string_txn_date = shapes.String(2 * inch, 0, cardno, fontName="Helvetica-Bold", fontSize=8)
            detail_row3.add(string_txn_date)

            name_on_card = "--" if df2['nameoncard'].values[0] is None else df2['nameoncard'].values[0]
            string_description = shapes.String(3.0 * inch, 0, name_on_card, fontName="Helvetica-Bold",
                                               fontSize=8)
            detail_row3.add(string_description)

            string_transaction_amount = shapes.String(4.5 * inch, 0, 'SUB TOTAL', fontName="Helvetica",
                                                      fontSize=8)
            detail_row3.add(string_transaction_amount)

            string_debits = shapes.String(5.5 * inch, 0, '-DEBITS', fontName="Helvetica",
                                          fontSize=8)
            detail_row3.add(string_debits)

            string_ttl_debits = shapes.String(6.5 * inch, 0, str(totaladj_reportone), fontName="Helvetica",
                                              fontSize=8)
            detail_row3.add(string_ttl_debits)
            elements.append(detail_row3)


    except Exception as err:
        app.logger.error('Error while generating sub report one  {}'.format(str(err)))
