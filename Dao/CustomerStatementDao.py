import pandas as pd
from app import app

# import dbconnection
from DatabaseConnection import *
from Utils.Configuration import *


def totalStmtGenerationCount(startingEodStatus):

    global status, count

    if startingEodStatus == 'INIT':
        status = 0
    elif startingEodStatus == 'ERROR':
        status = 2
    else:
        app.logger.error('Error in Customer Statement controller {}')

    try:
        query = ''' SELECT COUNT(ACCOUNTNO) AS COUNT FROM BILLINGSTATEMENT WHERE STATEMENTGENERATEDSTATUS = :status  '''

        df_count = pd.read_sql(query, con=conEngine(), params={"status": status})

        count = df_count["count"].values[0]

        print(f'The count of records is: {count}')

        return count

    except Exception as err:
        app.logger.error('Error in Customer Statement controller {}'.format(str(err)))


def getStatementIdsForStatementFileCreation(startEodStatus, start, end):

    global status, df2

    if startEodStatus == 'INIT':
        status = 0
    elif startEodStatus == 'ERROR':
        status = 2
    else:
        app.logger.error('Error in Customer Statement controller {}')

    try:

        query = ''' SELECT B.* 
                        FROM 
                        (SELECT ROWNUM RN, 
                            A.* 
                          FROM 
                            (SELECT DISTINCT CAC.ACCOUNTNO, 
                              BLS.*, 
                              CAC.ISPRIMARY, 
                              (SELECT CARDCATEGORYCODE FROM card WHERE cardnumber =CAC.CARDNUMBER 
                              ) AS CARDCATEGORYCODE, 
                              (SELECT MESSAGEONSTMT 
                              FROM ALLOCATIONMESSAGE , 
                                TRIGGERCARDS 
                              WHERE TRIGGERCARDS.CARDNO           = BS.CARDNO 
                              AND TRIGGERCARDS.LASTTRIGGERPOINT   = ALLOCATIONMESSAGE.TRIGGERPOINT 
                              AND ALLOCATIONMESSAGE.TRIGGERPOINT IN('O3SD','O2SD','O4SD') 
                              AND ALLOCATIONMESSAGE.STATUS        ='ACT' 
                              )              AS TRIGGERMSG, 
                              BS.STATEMENTID AS stmtid, 
                              bs.creditlimit, 
                              BS.STATEMENTGENERATEDSTATUS, 
                              NVL(bs.PURCHASES,0.00)    AS PURCHASES, 
                              NVL(bs.DRADJUSTMENT,0.00) AS DRADJUSTMENT, 
                              NVL(bs.CRADJUSTMENT,0.00) AS CRADJUSTMENT, 
                              NVL(bs.CASHADVANCE,0.00)  AS CASHADVANCE, 
                              NVL(bs.INTEREST,0.00)     AS INTEREST, 
                              NVL(bs.CHARGES,0.00)      AS CHARGES, 
                              BS.STARTEODID, 
                              BS.ENDEODID, 
                              CA.STATEMENTSENTOPTION, 
                              ( 
                              CASE 
                                WHEN BS.CARDCATEGORYCODE IN ('M', 'A', 'CO') 
                                THEN 
                                  (SELECT mobileno 
                                  FROM cardmaincustomerdetail 
                                  WHERE customerid = cac.customerid 
                                  ) 
                                WHEN BS.CARDCATEGORYCODE = 'F' 
                                THEN 
                                  (SELECT mobileno FROM cardfdcustomerdetail WHERE customerid = cac.customerid 
                                  ) 
                                WHEN BS.CARDCATEGORYCODE = 'E' 
                               THEN 
                                 (SELECT contactnumbersmobile 
                                  FROM cardestcustomerdetails 
                                  WHERE customerid = cac.customerid 
                                 ) 
                              END) AS MOBILENO 
                            FROM BILLINGSTATEMENT BS, 
                              BILLINGLASTSTATEMENTSUMMARY BLS, 
                              CARDACCOUNT CA, 
                              CARDACCOUNTCUSTOMER CAC 
                            WHERE BS.STATEMENTGENERATEDSTATUS = :status
                            AND BS.CARDNO                     =BLS.CARDNO 
                            AND BS.ACCOUNTNO                  = CA.ACCOUNTNO 
                           AND CAC.CARDNUMBER                =BS.CARDNO 
                            ORDER BY CAC.ACCOUNTNO ASC 
                            ) A 
                          ) B 
                        WHERE B.RN BETWEEN ''' + str(start) + ''' AND ''' + str(end)

        df2 = pd.read_sql(query, con=conEngine(), params={"status": status})

        return df2

    except Exception as err:
        app.logger.error('Error in Statement Generation controller {}'.format(str(err)))

def getdataFromMainQuery(statementid):

    global df
    try:
        query = '''SELECT b.* FROM ( SELECT ROWNUM rn, a.* FROM ( SELECT DISTINCT cac.accountno, bls.*, cac.isprimary, ( SELECT cardcategorycode FROM card WHERE cardnumber = cac.cardnumber )                          AS cardcategorycode, ( SELECT messageonstmt FROM allocationmessage, triggercards WHERE triggercards.cardno = bs.cardno AND triggercards.lasttriggerpoint = allocationmessage.triggerpoint AND allocationmessage.triggerpoint IN ( 'O3SD', 'O2SD', 'O4SD' ) AND allocationmessage.status = 'ACT' ) AS triggermsg, bs.statementid AS stmtid, bs.creditlimit, bs.statementgeneratedstatus, nvl(bs.purchases, 0.00)    AS purchases, nvl(bs.dradjustment, 0.00) AS dradjustment, nvl(bs.cradjustment, 0.00) AS cradjustment, nvl(bs.cashadvance, 0.00)  AS cashadvance, nvl(bs.interest, 0.00) AS interest, nvl(bs.charges, 0.00) AS charges, bs.starteodid, bs.endeodid, ca.statementsentoption, ( CASE WHEN bs.cardcategorycode IN ( 'M', 'A', 'CO' ) THEN ( SELECT mobileno FROM cardmaincustomerdetail WHERE customerid = cac.customerid ) WHEN bs.cardcategorycode = 'F' THEN ( SELECT mobileno FROM cardfdcustomerdetail WHERE customerid = cac.customerid ) WHEN bs.cardcategorycode = 'E' THEN ( SELECT contactnumbersmobile FROM cardestcustomerdetails WHERE customerid = cac.customerid ) END ) AS mobileno FROM billingstatement bs, billinglaststatementsummary bls, cardaccount ca, cardaccountcustomer cac WHERE bs.statementgeneratedstatus = 0 AND bs.cardno = bls.cardno AND bs.accountno = ca.accountno AND cac.cardnumber = bs.cardno ORDER BY cac.accountno ASC ) a ) b WHERE b.stmtid = :statementid'''

        df = pd.read_sql(query, con=conEngine(), params={"statementid": (format(str(statementid)))})

    except Exception as err:
        app.logger.error('Error while getting data from main query {}'.format(str(err)))

    return df


def getBillingAddress(cardcategorycode, cardno):
    global name, address1, address2, address3
    app.logger.info(cardno)
    try:
        if cardcategorycode == CARD_CATEGORY_MAIN or cardcategorycode == CARD_CATEGORY_CO_BRANDED or cardcategorycode == CARD_CATEGORY_AFFINITY:
            query = '''select TITLE,NAMEWITHINITIAL,BILLINGADDRESS1,BILLINGADDRESS2, BILLINGADDRESS3 from CARDMAINCUSTOMERDETAIL   CMC,CARDACCOUNTCUSTOMER CAC where CAC.CUSTOMERID = CMC.CUSTOMERID and CAC.CARDNUMBER = :cardno '''
        elif cardcategorycode == CARD_CATEGORY_ESTABLISHMENT:
            query = '''select NAMEOFTHECOMPANY,BUSINESSADDRESS1,BUSINESSADDRESS2, BUSINESSADDRESS3 from CARDMAINCUSTOMERDETAIL   CEC,CARDACCOUNTCUSTOMER CAC where CAC.CUSTOMERID = CEC.CUSTOMERID and CAC.CARDNUMBER = :cardno'''
        elif cardcategorycode == CARD_CATEGORY_FD:
            query = '''select TITLE,NAMEWITHINITIAL,BILLINGADDRESS1,BILLINGADDRESS2, BILLINGADDRESS3 from CARDMAINCUSTOMERDETAIL   CFC,CARDACCOUNTCUSTOMER CAC where CAC.CUSTOMERID = CFC.CUSTOMERID and CAC.CARDNUMBER = :cardno'''

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
        query = '''SELECT CAC.CARDNUMBER,
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
  TRANSACTIONDATE'''
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
        return df

    except Exception as err:
        app.logger.error('Error while getting data from sub report two {}'.format(str(err)))

def get_data_for_subreport_one(cardno):
    try:
        query = '''select crdr,amount,ADJUSTDATE,remarks,UNIQUEID from ADJUSTMENT where UNIQUEID = :cardno  and EODSTATUS = 'EDON' order by adjustdate'''
        df = pd.read_sql(query, con=conEngine(), params={"cardno": int(cardno)})
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

        return "1"

    except Exception as err:
        app.logger.error('Error in customer controller {}'.format(str(err)))


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
        app.logger.error('Error in customer controller {}'.format(str(err)))

