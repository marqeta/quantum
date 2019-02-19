import yaml
import csv
import os
import boto3
import json
import logging
import redis
from quantum.async_task import AsyncTask
from quantum.agg_engine import AggEngine
from quantum.qkeys import QKeys

class QuantumEngine():

    def __init__(self, config_file_path, hook=None):
        self.hook = hook
        self.config_file_path = config_file_path
        self.streaming = False

        with open(self.config_file_path) as stream:
            self.config = yaml.load(stream)

        for aggname in self.config:
            cfg = self.config[aggname]
            if 'redis' in cfg:
                redis_cfg = cfg['redis']
                redis_host = redis_cfg['host']
            else:
                redis_cfg = {}
                redis_host = 'localhost'
            redis_port =  6379
            if 'port' in redis_cfg:
                redis_port = redis_cfg['port']
            redis_db = 0
            if 'db' in redis_cfg:
                redis_db = redis_cfg['db']
            self.cache = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
            self.name = aggname

        self.qkeys = QKeys(self.cache)

        stream.close()

    def run(self):
        for aggname in self.config:
            cfg = self.config[aggname]
            self.agg_engine = AggEngine(aggname, cfg, self.cache)
            self.process(cfg)
            break # Only process one aggname

    def process(self, cfg):
        data_source = cfg['data_source']
        if data_source['type'] == 'csv':
            self.process_csv(cfg, data_source)
        elif data_source['type'] == 'stream':
            if data_source['stream_type'] == 'sqs':
                self.process_sqs_stream(cfg, data_source['queue'])
        else:
            raise ValueError('Unsupported data_source: ' + data_source)

    def process_csv(self, cfg, data_source):
        path = data_source['path']
        try:
            csv_file = open(path)
        except:
            ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
            path = ROOT_DIR + '/../tests/' + path
            try:
                csv_file = open(path)
            except:
                raise ValueError('Unable to find: ' + data_source['path'])

        input_file = csv.DictReader(csv_file)
        for row in input_file:
            self.perform_agg(row, cfg)
        csv_file.close()

    def process_sqs_stream(self, cfg, queue_url):
        async_task = AsyncTask()
        async_task.create_task(self.do_process_sqs_stream, cfg, queue_url)

    def do_process_sqs_stream(self, cfg, queue_url):
        #print('in do_process_sqs_stream')
        self.streaming = True
        client = boto3.client('sqs')
        while True:
            if not self.streaming:
                break
            response = client.receive_message(QueueUrl=queue_url)
            if 'Messages' in response:
                message = response['Messages'][0]
                message_body = message['Body']
                print('processing: ' + message_body)
                try:
                    fact_data = json.loads(message_body)
                    self.perform_agg(fact_data, cfg)
                except:
                    logging.info('Failed to parse to JSON: ' + message_body)
                receipt_handle = message['ReceiptHandle']
                client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    def stop_streaming(self):
        self.streaming = False

    def perform_agg(self, fact_data, cfg):
        data_type = cfg['data_type']
        dimensions = cfg['dimensions']
        measures = cfg['measures']
        datetime_field_name = cfg['datetime_field_name']
        datetime_field_format = cfg['datetime_field_format']
        fact_data = self.agg_engine.add_time_dimensions(fact_data, datetime_field_name, datetime_field_format)

        if self.hook is not None:
            self.hook.before_agg(fact_data, self.cache)

        t_dimensions = cfg['time']
        while len(t_dimensions) > 0:
            self.agg_engine.perform_agg(data_type, fact_data, dimensions, t_dimensions, measures)
            t_dimensions = t_dimensions[:-1]
        self.agg_engine.perform_agg(data_type, fact_data, dimensions, [], measures)

        if self.hook is not None:
            self.hook.after_agg(fact_data, self.cache)

    #
    #  Q KEYS
    #
    def get_agg_keys(self, pattern):
        return self.qkeys.get(pattern)

    def get_agg_value_by_key(self, key):
        value = self.qkeys.get_value(key)
        return value

    def get_agg_values_by_key(self, key, time_units, direction='-'):
        values =self.qkeys.get_values(key, time_units, direction)
        return values

    def get_agg_values(self, filter, time_units, direction='-'):
        key = self.generate_key_from_filter(filter)
        #print ('getting key: ' + key)
        return self.get_agg_values_by_key(key, time_units, direction)

    def generate_key_from_filter(self, filter):
        dimensions = self.config[self.name]['dimensions']
        key = '/qtname:' + self.name + '/dt:' + self.config[self.name]['data_type']
        for dimension in dimensions:
            if dimension in filter:
                key += '/' + dimension + ':' + filter[dimension]
        if 'y' in filter:
            key += '/y:' + filter['y']
        if 'm' in filter:
            key += '/m:' + filter['m']
        if 'w' in filter:
            key += '/w:' + filter['w']
        if 'd' in filter:
            key += '/d:' + filter['d']
        if 'h' in filter:
            key += '/h:' + filter['h']
        if 'mn' in filter:
            key += '/mn:' + filter['mn']
        if 's' in filter:
            key += '/s:' + filter['s']
        return key

