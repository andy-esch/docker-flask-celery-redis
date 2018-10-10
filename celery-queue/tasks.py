import os
import time
from celery import Celery
import pyodbc
from requests import Session

CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379'),
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379')

celery = Celery('tasks', broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)

session = Session()
session.verify = False


@celery.task(name='tasks.add')
def add(x, y):
    time.sleep(5)
    return x + y

@celery.task(name='tasks.hive2carto')
def hive2carto(hivedb, hivetable, carto_username, carto_api_key):
    """Send data from a hive database to a carto table"""
    try:
        pyodbc.autocommit = True
        conn = pyodbc.connect("DSN=Hive", autocommit=True)
        sql = "SELECT * FROM {db}.{table}".format(db=hivedb, table=hivetable)
        host = "https://{}.carto.com/".format(carto_username)

        context = cartoframes.CartoContext(
                baseurl=host,
                api_key=carto_api_key
                session=session,
                verbose=True
            )

        df = pandas.read_sql(sql, conn)
        context.write(
                df,
                hivetable,
                type_guessing=True,
                overwrite=True,
                verbose=True
            )
        return {
            'status': 'success',
            'table': "{host}/dataset/{tablename}".format(
                    host=host.strip('/'),
                    username=carto_username,
                    tablename=hivetable
                ),
             'code': 200
        }
    except pyodbc.Error as e:
        logging.error("Encountered pydobc error: \n{}".format(e))
        return {
            'page': "error-odbc.html",
            'code': 303,
            'reason': str(e)
        }
    except pandas.io.sql.DatabaseError as e:
        logging.error("Encountered invalid database or table: \n{}".format(e))
        return {
            'page': "error-invalid-database.html",
            'code': 303,
            'reason': str(e)
        }
    except carto.exceptions.CartoException as e:
        logging.info("Encountered Carto Error: \n{}".format(e))
        return {
            'page': "error-authentication.html",
            'code': 303,
            'reason': str(e)
        }
    except (RuntimeError, TypeError, NameError) as e:
        logging.error("Encountered error: \n{}".format(e))
        return {
            'page': None,
            'code': 303,
            'reason': str(e)
        }
