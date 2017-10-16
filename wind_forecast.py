import datetime
import logging
from ftplib import FTP
from pykafka import producer
import settings
import helpers
from custom_logstash import LogstashFormatter, LogstashHandler
from apscheduler.schedulers.blocking import BlockingScheduler
import time
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = LogstashHandler('localhost', 4512, ssl=False)
formatter = LogstashFormatter()
handler.setFormatter(formatter)
logger.addHandler(handler)
scheduler = BlockingScheduler()


def run():
    """
        This is the main function to call other functions for completion of process.
    """
    logging.info('Wind Forecast Process started')
    #file_date = str(datetime.date.today())
    file_date = str( datetime.date.today() - datetime.timedelta(1))
    ftp = FTP(settings.FTP_HOST, settings.FTP_USER, settings.FTP_PASS)
    logging.info('Wind Forecast Process: FTP Connection Established')
    ftp.cwd(settings.FTP_DIRECTORY)
    files_prefix_country = settings.FILES_PREFIX
    for file_prefix_country in files_prefix_country:
        file_prefix = file_prefix_country.split(':')[0]
        file_name = file_prefix + file_date + ".CSV"
        country = file_prefix_country.split(':')[1]
        status = 1
        while status == 1:
            if file_name in ftp.nlst():
                logging.info('Wind Forecast Process:' + ' ' + file_name + ' ' + 'is available in ftp server')
                helpers.download_from_ftp(ftp, file_name)
                formatted_json = helpers.convert_csv_to_json(file_name,country)
                helpers.produce_msg_to_kafka(settings.BOOTSTRAP_SERVER, settings.KAFKA_TOPIC, formatted_json)
                helpers.cleanup(file_name)
                status = 0
            else:
                logging.info('Wind Forecast Process:' + ' ' + file_name + ' ' + 'is not available in ftp server.. will check again')
                time.sleep(settings.SLEEPER_TIME)
                files_prefix_country.append(file_prefix)
                status = 0
    ftp.close()
    logging.info('Wind Forecast Process: FTP Connection Closed')
    logging.info('Wind Forecast Process finished')

if __name__=='__main__':
    try:
#        scheduler.add_job(run, trigger='cron', hour='0,6,10,11,12,13', minute='20',max_instances=4)
#        scheduler.start()
        run()
        sys.exit(0)
#   Capture the generic exception for all other exceptions
    except Exception as err:
        logging.error('Wind Forecast Process is caught with error', exc_info=True)
#        scheduler.shutdown()
        sys.exit(1)
