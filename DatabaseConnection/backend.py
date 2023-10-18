from app import app
import cx_Oracle
from sqlalchemy import create_engine


def conEngine():
    try:
        engine = create_engine('oracle+cx_oracle://CMSPRODUCTBACKENDDEV:password@124.43.16.185:50170/cmsbkdb')
        return engine
    except Exception as e:
        app.logger.error('Error while Db connecting {}'.format(str(e)))



def conn():
    try:
        db_connection = cx_Oracle.connect('CMSPRODUCTBACKENDDEV/password@124.43.16.185:50170/cmsbkdb')
        return db_connection
    except Exception as err:
        app.logger.error('Error while connecting ', err)





