import logging
import os
import time
from celery import Celery
import pyodbc
from requests import Session
import configparser
from ccsq_environment import get_config_file_location
import datetime
from subprocess import call
import carto
import cartoframes

import pandas


CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379'),
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379')

celery = Celery('tasks', broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)

session = Session()
session.verify = False

# Configuration
cp= configparser.RawConfigParser( )
cp.read( get_config_file_location() )
loglocation = cp.get('Logging','Location')


# Initialize logging
now = datetime.datetime.now()
logFileName = loglocation + "caads_geo_sync" + now.strftime("%Y-%m-%d-%H-%M-%S") + ".log"
logging.basicConfig(filename=logFileName,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.INFO)
logger = logging.getLogger(__name__)


@celery.task(name='tasks.add')
def add(x, y):
    time.sleep(5)
    return x + y

@celery.task(name='tasks.hive2carto')
def hive2carto(hivedb, hivetable, carto_username, carto_api_key):
    """Send data from a hive database to a carto table"""
    if (not has_kerberos_ticket()):
        init_kerberos_ticket()

    try:
        pyodbc.autocommit = True
        conn = pyodbc.connect("DSN=Hive", autocommit=True)
        sql = "SELECT * FROM {db}.{table}".format(db=hivedb, table=hivetable)
        host = 'https://10.137.161.198/user/{}/'.format(carto_username)
        logging.info("DJJ DEBUG host = " + host + " : user " + carto_username + 
                     " key = " + carto_api_key)
        context = cartoframes.CartoContext(
                base_url=host,
                api_key=carto_api_key,
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

def has_kerberos_ticket():
    return True if call(['klist', '-s']) == 0 else False

def init_kerberos_ticket():
    kinit = '/bin/kinit'
    kinit_args = [ kinit, 'devcartosvc@QNET.QUALNET.ORG', '-kt', '/opt/caads_geo_sync/carto_new.keytab' ]
    call(kinit_args)

