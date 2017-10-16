import hashlib
from pykafka import KafkaClient
import pandas as pd
import os
import logging
import json
import datetime


def download_from_ftp(ftp, file_name):
    """
    Download the given file from the given ftp.
    :param file_name: The name of the file to be downloaded
    :type file_name: String
    :param ftp: The ftp connection to be used to download the file
    :type ftp: ftp connection object
    """
    logging.info('Wind Forecast Process:' + ' ' + 'Downloading' + ' ' + file_name + ' ' + 'file from ftp server')
    with open(file_name, "wb") as f:
        ftp.retrbinary('RETR %s' % file_name, f.write)
    logging.info('Wind Forecast Process:' + ' ' + 'Finished downloading' + ' ' + file_name + ' ' + 'file from ftp server')



def convert_csv_to_json(file_name,country):
    """
        Convert csv file records to josn format.
        :param file_name: The name of the file to be downloaded
        :type file_name: String
    """
    logging.info('Wind Forecast Process:' + ' ' + 'Converting' + ' ' + file_name + ' ' + 'file to json')
    data = pd.read_csv(file_name, skiprows=[0, 1], sep='|', index_col=False, names=['id', 'forecastdate', 'valuedate', 'value'])
    data['valuedate'] = pd.to_datetime(data.valuedate, format='%d.%m.%Y %H:%M:%S').dt.strftime("%Y-%m-%d %H:%M:%S")
    data['forecastdate'] = pd.to_datetime(data.forecastdate, format='%d.%m.%Y %H:%M:%S').dt.strftime("%Y-%m-%d %H:%M:%S")
    data['country'] = country
    utc_datetime = datetime.datetime.utcnow()
    processed_time = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")
    data['processed_time'] = processed_time
    json_records = data.to_json(orient='records', date_unit='s')
    logging.info('Wind Forecast Process:' + ' ' + 'Finished converting' + ' ' + file_name + ' ' + 'file to json')
    print(json_records)
    return json_records


def produce_msg_to_kafka(bootstrap_server, topic, formatted_json):
    """
    Produce the input message to the given kafka topic
    :param formatted_json: JSON array containing the messages
    :type formatted_json: JSON String
    :param bootstrap_server: The location of the kafka bootstrap server
    :type bootstrap_server: String
    :param topic: The topic to which the message is produced
    :type topic: String
    """
    logging.info('Wind Forecast Process:' + ' ' + 'Producing message to Kafka')
    # Setup the kafka producer
    client = KafkaClient(bootstrap_server)
    topic = client.topics[topic.encode()]
    producer = topic.get_producer(sync=True)
    records = json.loads(formatted_json)
    for record in records:
        hash_object = hashlib.md5(json.dumps(record).encode()).hexdigest()
        record.update({'uid': hash_object})
        producer.produce(json.dumps(record).encode())
    logging.info('Wind Forecast Process:' + ' ' + 'Finished producing message to Kafka')
    producer.stop()


def cleanup(file_name):
    """
    Cleanup after the conversion process
    :param file_name:
    :type file_name:
    """
    logging.info('Wind Forecast Process:' + ' ' + 'Removing leftover files')
    os.remove(file_name)
    logging.info('Wind Forecast Process:' + ' ' + 'Finished removing leftover files')