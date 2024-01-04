from app import app
import cx_Oracle
from sqlalchemy import create_engine


# def conEngine():
#     try:
#         engine = create_engine('oracle+cx_oracle://DFCCBACKENDMVISAORTEST5:DFCCBACKENDMVISAORTEST5@172.30.35.11:1552/tstcmsbk')
#         return engine
#     except Exception as e:
#         app.logger.error('Error while Db connecting {}'.format(str(e)))
#
#
#
# def conn():
#     try:
#         db_connection = cx_Oracle.connect('DFCCBACKENDMVISAORTEST5/DFCCBACKENDMVISAORTEST5@172.30.35.11:1552/tstcmsbk')
#         return db_connection
#     except Exception as err:
#         app.logger.error('Error while connecting ', err)


def conEngine():
    try:
        engine = create_engine('oracle+cx_oracle://DFCCBACKENDMVISAORTEST5:DFCCBACKENDMVISAORTEST5@172.30.35.11:1552/tstcmsbk')
        return engine
    except Exception as e:
        app.logger.error('Error while Db connecting {}'.format(str(e)))



def conn():
    try:
        db_connection = cx_Oracle.connect('DFCCBACKENDMVISAORTEST5/DFCCBACKENDMVISAORTEST5@172.30.35.11:1552/tstcmsbk')
        return db_connection
    except Exception as err:
        app.logger.error('Error while connecting ', err)





