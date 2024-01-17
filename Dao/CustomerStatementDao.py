import pandas as pd
from app import app

# import dbconnection
from DatabaseConnection import *
from Utils.Configuration import *
from datetime import datetime
import numpy as np


def totalStmtGenerationCount():

    global status, count

    try:

        query = ''' SELECT COUNT(ACCOUNTNO) AS COUNT FROM BILLINGSTATEMENT WHERE STATEMENTGENERATEDSTATUS IN (0,2)  '''

        df_count = pd.read_sql(query, con=conEngine())

        count = df_count["count"].values[0]

        return count

    except Exception as err:
        app.logger.error('Error in Customer Statement Count Method {}'.format(str(err)))


def getStatementIdsForStatementFileCreation(start, end):

    global status, df2

    try:

        query = ''' SELECT
    b.STATEMENTID  AS stmtid
FROM
    (
        SELECT
            ROWNUM rn,
            a.*
        FROM
            (
                SELECT DISTINCT
                    cac.accountno,
                    bls.*,
                    cac.isprimary,
                    (
                        SELECT
                            cardcategorycode
                        FROM
                            card
                        WHERE
                            cardnumber = cac.cardnumber
                    )                          AS cardcategorycode,
                    (
                        SELECT
                            messageonstmt
                        FROM
                            allocationmessage,
                            triggercards
                        WHERE
                                triggercards.cardno = bs.cardno
                            AND triggercards.lasttriggerpoint = allocationmessage.triggerpoint
                            AND allocationmessage.triggerpoint IN ( 'O3SD', 'O2SD', 'O4SD' )
                            AND allocationmessage.status = 'ACT'
                    )                          AS triggermsg,
                    bs.statementid             AS stmtid,
                    bs.creditlimit,
                    bs.statementgeneratedstatus,
                    nvl(bs.purchases, 0.00)    AS purchases,
                    nvl(bs.dradjustment, 0.00) AS dradjustment,
                    nvl(bs.cradjustment, 0.00) AS cradjustment,
                    nvl(bs.cashadvance, 0.00)  AS cashadvance,
                    nvl(bs.interest, 0.00)     AS interest,
                    nvl(bs.charges, 0.00)      AS charges,
                    bs.starteodid,
                    bs.endeodid,
                    ca.statementsentoption,
                    (
                        CASE
                            WHEN bs.cardcategorycode IN ( 'M', 'A', 'CO' ) THEN
                                (
                                    SELECT
                                        mobileno
                                    FROM
                                        cardmaincustomerdetail
                                    WHERE
                                        customerid = cac.customerid
                                )
                            WHEN bs.cardcategorycode = 'F' THEN
                                (
                                    SELECT
                                        mobileno
                                    FROM
                                        cardfdcustomerdetail
                                    WHERE
                                        customerid = cac.customerid
                                )
                            WHEN bs.cardcategorycode = 'E' THEN
                                (
                                    SELECT
                                        contactnumbersmobile
                                    FROM
                                        cardestcustomerdetails
                                    WHERE
                                        customerid = cac.customerid
                                )
                        END
                    )                          AS mobileno
                FROM
                    billingstatement            bs,
                    billinglaststatementsummary bls,
                    cardaccount                 ca,
                    cardaccountcustomer         cac
                WHERE
                    bs.statementgeneratedstatus IN (0,2)
                    AND bs.cardno = bls.cardno
                    AND bs.accountno = ca.accountno
                    AND cac.cardnumber = bs.cardno
                ORDER BY
                    cac.accountno ASC
            ) a
    ) b WHERE B.RN BETWEEN ''' + str(start) + ''' AND ''' + str(end)

        df2 = pd.read_sql(query, con=conEngine())

        return df2

    except Exception as err:
        app.logger.error('Error in Get Statement Ids for file Creation Method {}'.format(str(err)))

def getdataFromMainQuery(statementid):

    global df
    try:
        query = '''SELECT b.* FROM ( SELECT ROWNUM rn, a.* FROM ( SELECT DISTINCT cac.accountno, bls.*, cac.isprimary, ( SELECT cardcategorycode FROM card WHERE cardnumber = cac.cardnumber ) AS cardcategorycode, ( SELECT messageonstmt FROM allocationmessage, triggercards WHERE triggercards.cardno = bs.cardno AND triggercards.lasttriggerpoint = allocationmessage.triggerpoint AND allocationmessage.triggerpoint IN ( 'O3SD', 'O2SD', 'O4SD' ) AND allocationmessage.status = 'ACT' ) AS triggermsg, bs.statementid AS stmtid, bs.creditlimit, bs.statementgeneratedstatus, nvl(bs.purchases, 0.00) AS purchases, nvl(bs.dradjustment, 0.00) AS dradjustment, nvl(bs.cradjustment, 0.00) AS cradjustment, nvl(bs.cashadvance, 0.00)  AS cashadvance, nvl(bs.interest, 0.00) AS interest, nvl(bs.charges, 0.00) AS charges, bs.starteodid, bs.endeodid, ca.statementsentoption, ( CASE WHEN bs.cardcategorycode IN ( 'M', 'A', 'CO' ) THEN ( SELECT mobileno FROM cardmaincustomerdetail WHERE customerid = cac.customerid ) WHEN bs.cardcategorycode = 'F' THEN ( SELECT mobileno FROM cardfdcustomerdetail WHERE customerid = cac.customerid ) WHEN bs.cardcategorycode = 'E' THEN ( SELECT contactnumbersmobile FROM cardestcustomerdetails WHERE customerid = cac.customerid ) END ) AS mobileno FROM billingstatement bs, billinglaststatementsummary bls, cardaccount ca, cardaccountcustomer cac WHERE bs.statementgeneratedstatus in(0,2) AND bs.cardno = bls.cardno AND bs.accountno = ca.accountno AND cac.cardnumber = bs.cardno ORDER BY cac.accountno ASC ) a ) b WHERE b.stmtid = :statementid'''

        df = pd.read_sql(query, con=conEngine(), params={"statementid": (format(str(statementid)))})

    except Exception as err:
        app.logger.error('Error while getting data from main query {}'.format(str(err)))

    return df


def getBillingAddress(cardcategorycode, cardno):
    global name, address1, address2, address3
    # app.logger.info(cardno)
    try:
        if cardcategorycode == CARD_CATEGORY_MAIN or cardcategorycode == CARD_CATEGORY_CO_BRANDED or cardcategorycode == CARD_CATEGORY_AFFINITY:
            query = '''select TITLE,NAMEWITHINITIAL,BILLINGADDRESS1,BILLINGADDRESS2, BILLINGADDRESS3 from CARDMAINCUSTOMERDETAIL CMC,CARDACCOUNTCUSTOMER CAC where CAC.CUSTOMERID = CMC.CUSTOMERID and CAC.CARDNUMBER = :cardno '''
        elif cardcategorycode == CARD_CATEGORY_ESTABLISHMENT:
            query = '''select NAMEOFTHECOMPANY,BUSINESSADDRESS1,BUSINESSADDRESS2, BUSINESSADDRESS3 from CARDESTCUSTOMERDETAILS CEC,CARDACCOUNTCUSTOMER CAC where CAC.CUSTOMERID = CEC.CUSTOMERID and CAC.CARDNUMBER = :cardno'''
        elif cardcategorycode == CARD_CATEGORY_FD:
            query = '''select TITLE,NAMEWITHINITIAL,BILLINGADDRESS1,BILLINGADDRESS2, BILLINGADDRESS3 from CARDFDCUSTOMERDETAIL CFC,CARDACCOUNTCUSTOMER CAC where CAC.CUSTOMERID = CFC.CUSTOMERID and CAC.CARDNUMBER = :cardno'''

        df = pd.read_sql(query, con=conEngine(), params={"cardno": cardno})
        if cardcategorycode == 'E':
            name = df["nameofthecompany"].values[0]
            address1 = df["businessaddress1"].values[0]
            address2 = df["businessaddress2"].values[0]
            address3 = df["businessaddress3"].values[0]
        else:
            name = df["title"].values[0] + ' ' + df["namewithinitial"].values[0]
            address1 = df["billingaddress1"].values[0]
            address2 = df["billingaddress2"].values[0]
            address3 = df["billingaddress3"].values[0]

    except Exception as err:
        app.logger.error('Error while getting data from billing address query {}'.format(str(err)))
    return name, address1, address2, address3


def getDatafromSecondQuery(accountNo, startEodID, endEodID):

    try:
        query = ''' SELECT CAC.CARDNUMBER,
          NVL(ET.TRANSACTIONAMOUNT,'')     AS TRANSACTIONAMOUNT,
          NVL(ET.SETTLEMENTDATE,'')        AS SETTLEMENTDATE,
          NVL(ET.TRANSACTIONDATE,'')       AS TRANSACTIONDATE,
          NVL(ET.TRANSACTIONTYPE,'')       AS TRANSACTIONTYPE,
          NVL(ET.TRANSACTIONDESCRIPTION,'')AS TRANSACTIONDESCRIPTION,
          CA.CARDCATEGORYCODE,
          CA.CARDSTATUS,
          CA.NAMEONCARD,
          NVL(
          (SELECT SUM(FEEAMOUNT)
          FROM EODCARDFEE
          WHERE STATUS      ='EDON'
          AND FEETYPE       =:feeCashAdType
          AND CARDNUMBER    = cac.cardnumber
          AND TRANSACTIONID = et.TRANSACTIONID
          ),0.00) AS cashAdvanceFee,
          et.eodid,
          et.crdr,
          CAC.ACCOUNTNO,
          AA.CASHBACKAMOUNT,
          AA.AVLCBAMOUNT,
          AA.OPENNINGCASHBACK,
          AA.PREVEODID,
          AA.ACCOUNTNUMBER,
          AA.REDEEMABLECASHBACK,
          AA.CBACCOUNTNO,
          AA.CBACCOUNTNAME,
          AA.EODID,
          AA.REDEEMTOTALCB,
          AA.EXPIRETOTALCB,
          AA.ADJCBAMOUNT,
          (AA.CASHBACKAMOUNT-AA.ADJCBAMOUNT)                  AS CASHBACKAMOUNTWITHOUTADJ,
          (                 -AA.EXPIRETOTALCB+AA.ADJCBAMOUNT) AS CBEXPAMOUNTWITHADJ
        FROM CARDACCOUNTCUSTOMER CAC
        FULL OUTER JOIN EODTRANSACTION ET
        ON ET.ACCOUNTNO        = CAC.ACCOUNTNO
        AND et.cardnumber      =cac.cardnumber
        AND et.EODID           >:startEodID
        AND et.EODID          <=:endEodID
        AND ET.ADJUSTMENTSTATUS='NO'
        RIGHT JOIN CARD CA
        ON cac.cardnumber =ca.cardnumber
        LEFT JOIN
          (SELECT A.*,
            (SELECT NVL(SUM(AMOUNT),0) AS REDEEMTOTALCB
            FROM CASHBACKEXPREDEEM CER
            WHERE CER.STATUS      = 0
            AND CER.ACCOUNTNUMBER = A.ACCOUNTNUMBER
            AND CER.EODID         > A.PREVEODID
            AND CER.EODID        <= A.EODID
            ) AS REDEEMTOTALCB,
            (SELECT NVL(SUM(AMOUNT),0) AS REDEEMTOTALCB
            FROM CASHBACKEXPREDEEM CER
            WHERE CER.STATUS     <> 0
            AND CER.ACCOUNTNUMBER = A.ACCOUNTNUMBER
            AND CER.EODID         > A.PREVEODID
            AND CER.EODID        <= A.EODID
            ) AS EXPIRETOTALCB
          FROM
            (SELECT CB.CASHBACKAMOUNT,
              (
              CASE
                WHEN CB.ISFLAG = 0
                THEN CB.TOTALCBAMOUNT
                WHEN CB.ISFLAG = 1
                THEN 0
                WHEN CB.ISFLAG = 2
                THEN 0
                WHEN CB.ISFLAG = 3
                THEN 0
              END) AS AVLCBAMOUNT,
              (
              CASE
                WHEN CB.ISFLAG = 0
                THEN NVL(CC.TOTALCBAMOUNT,0)
                WHEN CB.ISFLAG = 1
                THEN 0
                WHEN CB.ISFLAG = 2
                THEN CB.TOTALCBAMOUNT
                WHEN CB.ISFLAG = 3
                THEN 0
              END) AS OPENNINGCASHBACK,
              (
              CASE
                WHEN CB.ISFLAG = 0
                THEN NVL(CC.EODID,0)
                WHEN CB.ISFLAG = 1
                THEN 0
                WHEN CB.ISFLAG = 2
                THEN CB.DEAPREVEODID
                WHEN CB.ISFLAG = 3
                THEN CB.EODID
              END) AS PREVEODID,
              CB.ACCOUNTNUMBER,
              (
              CASE
                WHEN CB.ISFLAG = 0
                THEN AA.REDEEMABLECASHBACK
                WHEN CB.ISFLAG = 1
                THEN 0
                WHEN CB.ISFLAG = 2
                THEN 0
                WHEN CB.ISFLAG = 3
                THEN 0
              END) AS REDEEMABLECASHBACK,
              BB.CBACCOUNTNO,
              BB.CBACCOUNTNAME,
              CB.EODID,
              CB.ADJCBAMOUNT
            FROM
              (SELECT CB.ACCOUNTNUMBER,
                CB.TOTALCBAMOUNT,
                CB.CASHBACKAMOUNT,
                CB.EODID,
                0                 AS DEAPREVEODID,
                0                 AS ISFLAG,
                CB.ADJUSTEDAMOUNT AS ADJCBAMOUNT
              FROM CASHBACK CB
              WHERE CB.ISEXPIRED   = 0
              AND CB.EODID         = :endEodID
              AND CB.ACCOUNTNUMBER = :accountNo
              UNION ALL
              SELECT CA.ACCOUNTNO,
                0 AS TOTALCBAMOUNT,
                0 AS CASHBACKAMOUNT,
                0 AS EODID,
                0 AS DEAPREVEODID,
                1 AS ISFLAG,
                0 AS ADJCBAMOUNT
              FROM CARDACCOUNT CA
              WHERE CA.CASHBACKPROFILECODE IS NOT NULL
              AND CASHBACKSTARTDATE        IS NULL
              AND CA.ACCOUNTNO              = :accountNo
              UNION ALL
              SELECT A.*,
                CASE
                  WHEN EODID - DEAPREVEODID >19000
                  THEN 3
                  ELSE 2
                END AS ISFLAG,
                0   AS ADJCBAMOUNT
              FROM
                (SELECT CA.ACCOUNTNO,
                  CB.TOTALCBAMOUNT AS TOTALCBAMOUNT,
                  0                AS CASHBACKAMOUNT,
                  :endEodID     AS EODID,
                  CB.EODID         AS DEAPREVEODID
                FROM CARDACCOUNT CA
                INNER JOIN
                  (SELECT B.ACCOUNTNUMBER,
                    B.EODID,
                    B.TOTALCBAMOUNT
                  FROM
                    (SELECT A.ACCOUNTNUMBER,
                      A.EODID,
                      A.TOTALCBAMOUNT,
                      ROWNUM AS RN
                    FROM
                      (SELECT CBB.ACCOUNTNUMBER,
                        CBB.EODID,
                        CBB.TOTALCBAMOUNT
                      FROM CASHBACK CBB
                      WHERE CBB.ACCOUNTNUMBER = :accountNo
                      ORDER BY CBB.EODID DESC
                      ) A
                    ) B
                  WHERE B.RN=1
                  ) CB
                ON CA.ACCOUNTNO             = CB.ACCOUNTNUMBER
                WHERE CA.STATUS             = 'DEA'
                AND CA.ACCOUNTNO            = :accountNo
                AND CA.CASHBACKPROFILECODE IS NOT NULL
                ) A
              ) CB
            LEFT JOIN
              (SELECT AB.ACCOUNTNO,
                CASE
                  WHEN AB.AVLCASHBACKAMOUNT < AB.MINACCUMULATEDTOCLAIM
                  THEN 0
                  WHEN (AB.REDEEMABLEAMOUNT   + AB.REDEEMEDAMOUNT) > AB.MAXCASHBACKPERYEAR
                  THEN (AB.MAXCASHBACKPERYEAR - AB.REDEEMEDAMOUNT)
                  ELSE AB.REDEEMABLEAMOUNT
                END AS REDEEMABLECASHBACK
              FROM
                (SELECT CA.ACCOUNTNO,
                  (FLOOR(NVL(CA.AVLCASHBACKAMOUNT,0)/NVL(CBP.REDEEMRATIO ,0))*NVL(CBP.REDEEMRATIO ,0) ) AS REDEEMABLEAMOUNT,
                  NVL(AA.REDEEMEDAMOUNT,0)                                                              AS REDEEMEDAMOUNT,
                  NVL(CBP.MAXCASHBACKPERYEAR,0)                                                         AS MAXCASHBACKPERYEAR,
                  NVL(CBP.MINACCUMULATEDTOCLAIM,0)                                                      AS MINACCUMULATEDTOCLAIM,
                  NVL(CA.AVLCASHBACKAMOUNT,0)                                                           AS AVLCASHBACKAMOUNT
                FROM CARDACCOUNT CA
                INNER JOIN CASHBACKPROFILE CBP
                ON CA.CASHBACKPROFILECODE = CBP.PROFILECODE
                LEFT JOIN
                  (SELECT CER.ACCOUNTNUMBER,
                    SUM(NVL(CER.AMOUNT,0)) AS REDEEMEDAMOUNT
                  FROM CASHBACKEXPREDEEM CER
                  INNER JOIN CARDACCOUNT CAC
                  ON CER.ACCOUNTNUMBER    = CAC.ACCOUNTNO
                  WHERE CER.ACCOUNTNUMBER = :accountNo
                  AND CER.STATUS          = 0
                  AND TRUNC(CER.EODDATE) >= TRUNC(CAC.CASHBACKSTARTDATE)
                  GROUP BY CER.ACCOUNTNUMBER
                  ) AA ON CA.ACCOUNTNO = AA.ACCOUNTNUMBER
                WHERE CA.ACCOUNTNO     = :accountNo
                ) AB
              WHERE AB.ACCOUNTNO       = :accountNo
              ) AA ON CB.ACCOUNTNUMBER = AA.ACCOUNTNO
            LEFT JOIN
              (SELECT CA.ACCOUNTNO,
                CA.AVLCASHBACKAMOUNT,
                CA.CASHBACKSTARTDATE,
                CA.CBDEBITACCOUNTNAME AS CBACCOUNTNAME,
                CA.CBDEBITACCOUNTNO   AS CBACCOUNTNO
              FROM CARDACCOUNT CA
              WHERE CA.ACCOUNTNO = :accountNo
              ) BB
            ON CB.ACCOUNTNUMBER = BB.ACCOUNTNO
            LEFT JOIN
              (SELECT C.ACCOUNTNUMBER,
                C.EODID,
                C.TOTALCBAMOUNT
              FROM
                (SELECT B.ACCOUNTNUMBER,
                  B.EODID,
                  B.TOTALCBAMOUNT
                FROM
                  (SELECT A.ACCOUNTNUMBER,
                    A.EODID,
                    A.TOTALCBAMOUNT,
                    ROWNUM AS RN
                  FROM
                    (SELECT CBB.ACCOUNTNUMBER,
                      CBB.EODID,
                      CBB.TOTALCBAMOUNT
                    FROM CASHBACK CBB
                    WHERE CBB.ACCOUNTNUMBER = :accountNo
                    ORDER BY CBB.EODID DESC
                    ) A
                  ) B
                WHERE B.RN=2
                ) C
              ) CC
            ON CB.ACCOUNTNUMBER    = CC.ACCOUNTNUMBER
            WHERE CB.ACCOUNTNUMBER = :accountNo
            ) A
          ) AA ON CAC.ACCOUNTNO = AA.ACCOUNTNUMBER
        WHERE CAC.ACCOUNTNO     =:accountNo
        ORDER BY
          CASE
            WHEN CARDCATEGORYCODE = 'M'
            OR CARDCATEGORYCODE   = 'E'
            OR CARDCATEGORYCODE   = 'F'
            OR CARDCATEGORYCODE   = 'A'
            OR CARDCATEGORYCODE   = 'CO'
            THEN 1
            WHEN CARDCATEGORYCODE = 'S'
            OR CARDCATEGORYCODE   = 'C'
            OR CARDCATEGORYCODE   = 'FS'
            OR CARDCATEGORYCODE   = 'AS'
            OR CARDCATEGORYCODE   = 'COS'
            THEN 2
            ELSE 3
          END,
          CAC.CARDNUMBER,
          SETTLEMENTDATE,
          TRANSACTIONDATE '''

        df = pd.read_sql(query, con=conEngine(),
                         params={"accountNo": int(accountNo), "startEodID": int(startEodID), "endEodID": int(endEodID),
                                 "feeCashAdType": CASH_ADVANCE_FEE})


    except Exception as err:
        app.logger.error('Error while getting data from second query {}'.format(str(err)))
    return df


def getDataForSubReportTwo(cardno):
    global df
    try:
        query = '''SELECT to_date(cf.effectdate,'DD-MM-YYYY')AS effectdate,
  CASE
    WHEN CF.STATUS = 'BCCP'
    THEN NULL
    WHEN CF.feetype=:feeCashAdType
    THEN NULL
    ELSE cf.feeamount
  END AS FEEAMOUNT ,
  f.description ,
  CF.CARDNUMBER,
  cf.status,
  EI.CARDNO,
  ei.forwardinterest AS INTERREST
FROM eodcardfee CF
LEFT JOIN fee f
ON (cf.feetype = f.feecode)
FULL OUTER JOIN EOMINTEREST EI
ON EI.CARDNO          = CF.CARDNUMBER
WHERE (EI.CARDNO          = :cardno) or (cf.cardnumber=:cardno)
ORDER BY to_date(effectdate) ASC'''

        df = pd.read_sql(query, con=conEngine(), params={"cardno": int(cardno), "feeCashAdType": CASH_ADVANCE_FEE})

    except Exception as err:
        app.logger.error('Error while getting data from second query {}'.format(str(err)))
    return df

def get_data_for_subreport_two(cardno):
    try:

        query = '''SELECT * FROM(SELECT TO_DATE(CF.EFFECTDATE,'DD-MM-YYYY')AS EFFECTDATE,
  CASE
    WHEN CF.STATUS = 'BCCP'
    THEN NULL
    WHEN CF.FEETYPE=:feeCashAdType
    THEN NULL
    ELSE CF.FEEAMOUNT
  END AS FEEAMOUNT,
  F.DESCRIPTION ,
  CF.CARDNUMBER,
  CF.STATUS
FROM EODCARDFEE CF
LEFT JOIN FEE F
ON (CF.FEETYPE = F.FEECODE)
WHERE (CF.CARDNUMBER=:cardno)
ORDER BY (EFFECTDATE) ASC)T1 FULL OUTER JOIN (SELECT
    EI.CARDNO,
    EI.FORWARDINTEREST AS INTERREST
FROM
    EOMINTEREST EI
WHERE
    (EI.CARDNO = :cardno)
)T2 ON T1.CARDNUMBER=T2.CARDNO '''

        df = pd.read_sql(query, con=conEngine(), params={"cardno": int(cardno), "feeCashAdType": CASH_ADVANCE_FEE})

        return df

    except Exception as err:
        app.logger.error('Error while getting data from sub report two {}'.format(str(err)))

def get_data_for_subreport_one(cardno, statementEndDate):

    statementEndDate = statementEndDate

    statement_date = datetime.strptime(statementEndDate, '%Y-%m-%d')

    try:

        query = '''select crdr,amount,ADJUSTDATE,remarks,UNIQUEID from ADJUSTMENT where UNIQUEID = :cardno  and EODSTATUS = 'EDON' and TRUNC(adjustdate) <=TRUNC(:statementenddate) order by adjustdate'''
        df = pd.read_sql(query, con=conEngine(), params={"cardno": int(cardno), "statementenddate": statement_date})
        return df

    except Exception as err:
        app.logger.error('Error while getting data from sub report one {}'.format(str(err)))


def updateStatus(statementid):

    global status
    status = 1

    try:
        con = conn()
        cursor = con.cursor()

        sql = ''' UPDATE BILLINGSTATEMENT SET STATEMENTGENERATEDSTATUS = :status WHERE STATEMENTID = :statementid '''

        values = (status, statementid)

        cursor.execute(sql, values)

        con.commit()
        cursor.close()
        con.close()

    except Exception as err:
        app.logger.error('Error in update Billing Status {}'.format(str(err)))


def UpdateEodCardFeeTableBillingDone(accountNo, statementEndDate):
    # Convert the string to a datetime object
    date_obj = datetime.strptime(statementEndDate, "%Y-%m-%d")

    # Format the datetime object as '11-JUL-22'
    formatted_date = date_obj.strftime("%d-%b-%y")

    try:
        con = conn()
        cursor = con.cursor()

        sql = ''' UPDATE EODCARDFEE SET STATUS='BCCP' WHERE ACCOUNTNO =:accountNo and STATUS = 'EDON' and effectdate <=:formatted_date '''

        values = (accountNo, formatted_date)

        cursor.execute(sql, values)

        con.commit()
        cursor.close()
        con.close()

    except Exception as err:
        app.logger.error('Error in Update Eod Card Fee Table Billing Done {}'.format(str(err)))

def UpdateAdjustmentTableBillingDone(accountNo, statementId, statementEndDate):
    try:
        con = conn()
        cursor = con.cursor()

        date_obj = datetime.strptime(statementEndDate, "%Y-%m-%d")
        formatted_date = date_obj.strftime("%d-%b-%y")

        sql = '''
            UPDATE ADJUSTMENT
            SET STATEMENTID = :statementId, EODSTATUS = 'BCCP'
            WHERE UNIQUEID IN (SELECT cardnumber FROM cardaccountcustomer WHERE accountno = :accountNo)
                AND EODSTATUS = 'EDON'
                AND TRUNC(adjustdate) <= TRUNC(TO_DATE(:formatted_date, 'DD-MON-YY'))
        '''

        values = {
            'accountNo': accountNo,
            'statementId': statementId,
            'formatted_date': formatted_date,
        }

        cursor.execute(sql, values)
        con.commit()

    except Exception as err:
        app.logger.error('Error in Update Adjustment Table Billing Done {}'.format(str(err)))
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()


def InsertIntoDownloadTable(cardNo, statementId, accountNo):
    statement = statementId + ".pdf"

    try:
        con = conn()
        cursor = con.cursor()

        sql = '''
            INSERT INTO DOWNLOADFILE (
                FIETYPE, FILENAME, LETTERTYPE, STATUS, GENERATEDUSER,
                STATEMENTMONTH, STATEMENTYEAR, LASTUPDATEDTIME, CREATEDTIME,
                LASTUPDATEDUSER, CARDTYPE, CARDPRODUCT, FILEID, ACCNUMBER,
                APPLICATIONID, SUBFOLDER
            ) VALUES (
                'STATEMENT', :statement, ' ', 'YES', 'EOD',
                '', '', SYSDATE, SYSDATE, 'EOD', '', '', :statement, :accountNo,
                'EDON', ''
            )
        '''

        values = {
            'statement': statement,
            'accountNo': accountNo,
        }

        cursor.execute(sql, values)
        con.commit()

    except Exception as err:
        app.logger.error('Error in Insert Into Download Table: {}'.format(str(err)))
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()

def updateErrorFileStatus(statementid):

    global status
    status = 2

    try:
        con = conn()
        cursor = con.cursor()

        sql = ''' UPDATE BILLINGSTATEMENT SET STATEMENTGENERATEDSTATUS = :status WHERE STATEMENTID = :statementid '''

        values = (status, statementid)

        cursor.execute(sql, values)

        con.commit()
        cursor.close()
        con.close()

    except Exception as err:
        app.logger.error('Error in update Error Billing Status {}'.format(str(err)))

