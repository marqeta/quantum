import unittest
import redis
import os
import time
import boto3
import json
from quantum.quantum_engine import QuantumEngine

class QuantumEngineStreamTest(unittest.TestCase):
    def setUp(self):
        redis_host = 'localhost'
        redis_port = 6379
        self.redis_cache = redis.StrictRedis(host=redis_host, port=redis_port, db=10)
        self.redis_cache.flushdb()

        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.quantum_engine = QuantumEngine(ROOT_DIR + '/aggstream.yml')

    def tearDown(self):
        pass

    def testRun1(self):
        self.quantum_engine.run()

        fact_data = [{
            'date': '2018-04-11T22:41:33Z',
            'cust_id': 'C1',
            'prod_id': 'P1',
            'total_price': 30.0,
            'qty': 10
        },
        {
            'date': '2018-04-12T22:41:33Z',
            'cust_id': 'C1',
            'prod_id': 'P2',
            'total_price': 40.0,
            'qty': 5
        }]

        client = boto3.client('sqs')
        for fd in fact_data:
            response = client.send_message(
                QueueUrl='https://us-east-2.queue.amazonaws.com/762683880968/divaqueue',
                MessageBody=json.dumps(fd)
            )

        time.sleep(3)

        keys = self.quantum_engine.get_agg_keys('*')
        for key in keys:
            print(key)

        self.assertTrue(keys[0] == '/qtname:aggstream/dt:transaction/cust_id:C1')
        self.assertTrue(keys[1] == '/qtname:aggstream/dt:transaction/cust_id:C1/y:2018')
        self.assertTrue(keys[2] == '/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04')
        self.assertTrue(keys[3] == '/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04/d:11')
        self.assertTrue(keys[4] == '/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04/d:11/h:22')
        self.assertTrue(keys[5] == '/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04/d:11/h:22/mn:41')
        self.assertTrue(keys[6] == '/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04/d:12')
        self.assertTrue(keys[7] == '/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04/d:12/h:22')
        self.assertTrue(keys[8] == '/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04/d:12/h:22/mn:41')

        value = self.quantum_engine.get_agg_value_by_key('/qtname:aggstream/dt:transaction/cust_id:C1')
        self.assertTrue(value['sum_total_price'] == 70.0)
        self.assertTrue(value['avg_total_price'] == 35.0)
        self.assertTrue(value['sum_qty'] == 15.0)
        self.assertTrue(value['avg_qty'] == 7.50)
        self.assertTrue(value['count'] == 2.0)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:aggstream/dt:transaction/cust_id:C1/y:2018')
        self.assertTrue(value['sum_total_price'] == 70.0)
        self.assertTrue(value['avg_total_price'] == 35.0)
        self.assertTrue(value['sum_qty'] == 15.0)
        self.assertTrue(value['avg_qty'] == 7.50)
        self.assertTrue(value['count'] == 2.0)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04')
        self.assertTrue(value['sum_total_price'] == 70.0)
        self.assertTrue(value['avg_total_price'] == 35.0)
        self.assertTrue(value['sum_qty'] == 15.0)
        self.assertTrue(value['avg_qty'] == 7.50)
        self.assertTrue(value['count'] == 2.0)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04/d:11')
        self.assertTrue(value['sum_total_price'] == 30.0)
        self.assertTrue(value['avg_total_price'] == 30.0)
        self.assertTrue(value['sum_qty'] == 10.0)
        self.assertTrue(value['avg_qty'] == 10.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04/d:11/h:22')
        self.assertTrue(value['sum_total_price'] == 30.0)
        self.assertTrue(value['avg_total_price'] == 30.0)
        self.assertTrue(value['sum_qty'] == 10.0)
        self.assertTrue(value['avg_qty'] == 10.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04/d:11/h:22/mn:41')
        self.assertTrue(value['sum_total_price'] == 30.0)
        self.assertTrue(value['avg_total_price'] == 30.0)
        self.assertTrue(value['sum_qty'] == 10.0)
        self.assertTrue(value['avg_qty'] == 10.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04/d:12')
        self.assertTrue(value['sum_total_price'] == 40.0)
        self.assertTrue(value['avg_total_price'] == 40.0)
        self.assertTrue(value['sum_qty'] == 5.0)
        self.assertTrue(value['avg_qty'] == 5.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04/d:12/h:22')
        self.assertTrue(value['sum_total_price'] == 40.0)
        self.assertTrue(value['avg_total_price'] == 40.0)
        self.assertTrue(value['sum_qty'] == 5.0)
        self.assertTrue(value['avg_qty'] == 5.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:aggstream/dt:transaction/cust_id:C1/y:2018/m:04/d:12/h:22/mn:41')
        self.assertTrue(value['sum_total_price'] == 40.0)
        self.assertTrue(value['avg_total_price'] == 40.0)
        self.assertTrue(value['sum_qty'] == 5.0)
        self.assertTrue(value['avg_qty'] == 5.0)
        self.assertTrue(value['count'] == 1.0)


if __name__ == '__main__':
    unittest.main()