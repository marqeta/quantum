import unittest

import redis

from quantum.agg_engine import AggEngine
from quantum.qkeys import QKeys

class AggEngineTest(unittest.TestCase):
    def setUp(self):
        redis_host = 'localhost'
        redis_port = 6379
        self.redis_cache = redis.Redis(host=redis_host, port=redis_port, db=10)
        self.redis_cache.flushdb()
        self.agg_engine = AggEngine('test', {'dimensions' : ['cust_id','prod_id'], 'data_type': 'trans'}, self.redis_cache)
        self.qkeys = QKeys(self.redis_cache)

    def tearDown(self):
        pass

    def testAggEngine_1(self):
        fact_data_type = 'trans'
        fact_data = {
            'date': '2018-04-11T22:41:33Z',
            'cust_id': 'C1',
            'prod_id': 'P1',
            'total_price': 30.0,
            'qty': 10
        }
        fact_data = self.agg_engine.add_time_dimensions(fact_data, 'date', 'YYYY-mm-ddTHH:MM:SSZ')
        self.perform_agg(fact_data_type, fact_data)

        keys = self.agg_engine.get_agg_keys('*')
        self.assertTrue(keys[0] == '/qtname:test/dt:trans/cust_id:C1')
        self.assertTrue(keys[1] == '/qtname:test/dt:trans/cust_id:C1/y:2018')
        self.assertTrue(keys[2] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04')
        self.assertTrue(keys[3] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11')
        self.assertTrue(keys[4] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11/h:22')
        self.assertTrue(keys[5] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11/h:22/mn:41')

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1')
        self.assertTrue(value['sum_total_price'] == 30.0)
        self.assertTrue(value['avg_total_price'] == 30.0)
        self.assertTrue(value['sum_qty'] == 10.0)
        self.assertTrue(value['avg_qty'] == 10.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018')
        self.assertTrue(value['sum_total_price'] == 30.0)
        self.assertTrue(value['avg_total_price'] == 30.0)
        self.assertTrue(value['sum_qty'] == 10.0)
        self.assertTrue(value['avg_qty'] == 10.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04')
        self.assertTrue(value['sum_total_price'] == 30.0)
        self.assertTrue(value['avg_total_price'] == 30.0)
        self.assertTrue(value['sum_qty'] == 10.0)
        self.assertTrue(value['avg_qty'] == 10.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11')
        self.assertTrue(value['sum_total_price'] == 30.0)
        self.assertTrue(value['avg_total_price'] == 30.0)
        self.assertTrue(value['sum_qty'] == 10.0)
        self.assertTrue(value['avg_qty'] == 10.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11/h:22')
        self.assertTrue(value['sum_total_price'] == 30.0)
        self.assertTrue(value['avg_total_price'] == 30.0)
        self.assertTrue(value['sum_qty'] == 10.0)
        self.assertTrue(value['avg_qty'] == 10.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11/h:22/mn:41')
        self.assertTrue(value['sum_total_price'] == 30.0)
        self.assertTrue(value['avg_total_price'] == 30.0)
        self.assertTrue(value['sum_qty'] == 10.0)
        self.assertTrue(value['avg_qty'] == 10.0)
        self.assertTrue(value['count'] == 1.0)

        fact_data_2 = {
            'date': '2018-04-12T22:41:33Z',
            'cust_id': 'C1',
            'prod_id': 'P2',
            'total_price': 40.0,
            'qty': 5
        }
        fact_data_2 = self.agg_engine.add_time_dimensions(fact_data_2, 'date', 'YYYY-mm-ddTHH:MM:SSZ')
        self.perform_agg(fact_data_type, fact_data_2)
        keys = self.agg_engine.get_agg_keys('*')
        self.assertTrue(keys[0] == '/qtname:test/dt:trans/cust_id:C1')
        self.assertTrue(keys[1] == '/qtname:test/dt:trans/cust_id:C1/y:2018')
        self.assertTrue(keys[2] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04')
        self.assertTrue(keys[3] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11')
        self.assertTrue(keys[4] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11/h:22')
        self.assertTrue(keys[5] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11/h:22/mn:41')
        self.assertTrue(keys[6] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12')
        self.assertTrue(keys[7] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12/h:22')
        self.assertTrue(keys[8] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12/h:22/mn:41')

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1')
        self.assertTrue(value['sum_total_price'] == 70.0)
        self.assertTrue(value['avg_total_price'] == 35.0)
        self.assertTrue(value['sum_qty'] == 15.0)
        self.assertTrue(value['avg_qty'] == 7.50)
        self.assertTrue(value['count'] == 2.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018')
        self.assertTrue(value['sum_total_price'] == 70.0)
        self.assertTrue(value['avg_total_price'] == 35.0)
        self.assertTrue(value['sum_qty'] == 15.0)
        self.assertTrue(value['avg_qty'] == 7.50)
        self.assertTrue(value['count'] == 2.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04')
        self.assertTrue(value['sum_total_price'] == 70.0)
        self.assertTrue(value['avg_total_price'] == 35.0)
        self.assertTrue(value['sum_qty'] == 15.0)
        self.assertTrue(value['avg_qty'] == 7.50)
        self.assertTrue(value['count'] == 2.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11')
        self.assertTrue(value['sum_total_price'] == 30.0)
        self.assertTrue(value['avg_total_price'] == 30.0)
        self.assertTrue(value['sum_qty'] == 10.0)
        self.assertTrue(value['avg_qty'] == 10.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11/h:22')
        self.assertTrue(value['sum_total_price'] == 30.0)
        self.assertTrue(value['avg_total_price'] == 30.0)
        self.assertTrue(value['sum_qty'] == 10.0)
        self.assertTrue(value['avg_qty'] == 10.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11/h:22/mn:41')
        self.assertTrue(value['sum_total_price'] == 30.0)
        self.assertTrue(value['avg_total_price'] == 30.0)
        self.assertTrue(value['sum_qty'] == 10.0)
        self.assertTrue(value['avg_qty'] == 10.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12')
        self.assertTrue(value['sum_total_price'] == 40.0)
        self.assertTrue(value['avg_total_price'] == 40.0)
        self.assertTrue(value['sum_qty'] == 5.0)
        self.assertTrue(value['avg_qty'] == 5.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12/h:22')
        self.assertTrue(value['sum_total_price'] == 40.0)
        self.assertTrue(value['avg_total_price'] == 40.0)
        self.assertTrue(value['sum_qty'] == 5.0)
        self.assertTrue(value['avg_qty'] == 5.0)
        self.assertTrue(value['count'] == 1.0)

        value = self.agg_engine.get_agg_value_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12/h:22/mn:41')
        self.assertTrue(value['sum_total_price'] == 40.0)
        self.assertTrue(value['avg_total_price'] == 40.0)
        self.assertTrue(value['sum_qty'] == 5.0)
        self.assertTrue(value['avg_qty'] == 5.0)
        self.assertTrue(value['count'] == 1.0)

    def testAggEngine_2(self):
        fact_data_type = 'trans'
        fact_data = {
            'date': '2018-04-11T22:41:33Z',
            'cust_id': 'C1',
            'prod_id': 'P1',
            'total_price': 30.0,
            'qty': 10
        }
        fact_data = self.agg_engine.add_time_dimensions(fact_data, 'date', 'YYYY-mm-ddTHH:MM:SSZ')
        self.perform_agg(fact_data_type, fact_data)

        fact_data_2 = {
            'date': '2018-04-12T22:41:33Z',
            'cust_id': 'C1',
            'prod_id': 'P2',
            'total_price': 40.0,
            'qty': 5
        }
        fact_data_2 = self.agg_engine.add_time_dimensions(fact_data_2, 'date', 'YYYY-mm-ddTHH:MM:SSZ')
        self.perform_agg(fact_data_type, fact_data_2)

        keys = self.agg_engine.get_agg_keys('*')
        self.assertTrue(keys[0] == '/qtname:test/dt:trans/cust_id:C1')
        self.assertTrue(keys[1] == '/qtname:test/dt:trans/cust_id:C1/y:2018')
        self.assertTrue(keys[2] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04')
        self.assertTrue(keys[3] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11')
        self.assertTrue(keys[4] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11/h:22')
        self.assertTrue(keys[5] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11/h:22/mn:41')
        self.assertTrue(keys[6] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12')
        self.assertTrue(keys[7] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12/h:22')
        self.assertTrue(keys[8] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12/h:22/mn:41')

        values = self.agg_engine.get_agg_values_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12', 0)
        self.assertTrue(len(values) == 1)
        value = values[0]
        self.assertTrue(value['key'] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12')

        values = self.agg_engine.get_agg_values_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12', 1)
        self.assertTrue(len(values) == 2)
        value = values[0]
        self.assertTrue(value['key'] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11')
        value = values[1]
        self.assertTrue(value['key'] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12')

    def testAggEngine_3(self):
        fact_data_type = 'trans'
        fact_data = {
            'date': '2018-04-11T22:41:33Z',
            'cust_id': 'C1',
            'prod_id': 'P1',
            'total_price': 30.0,
            'qty': 10
        }
        fact_data = self.agg_engine.add_time_dimensions(fact_data, 'date', 'YYYY-mm-ddTHH:MM:SSZ')
        self.perform_agg(fact_data_type, fact_data)

        fact_data_2 = {
            'date': '2018-04-12T22:41:33Z',
            'cust_id': 'C1',
            'prod_id': 'P2',
            'total_price': 40.0,
            'qty': 5
        }
        fact_data_2 = self.agg_engine.add_time_dimensions(fact_data_2, 'date', 'YYYY-mm-ddTHH:MM:SSZ')
        self.perform_agg(fact_data_type, fact_data_2)

        keys = self.agg_engine.get_agg_keys('*')
        self.assertTrue(keys[0] == '/qtname:test/dt:trans/cust_id:C1')
        self.assertTrue(keys[1] == '/qtname:test/dt:trans/cust_id:C1/y:2018')
        self.assertTrue(keys[2] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04')
        self.assertTrue(keys[3] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11')
        self.assertTrue(keys[4] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11/h:22')
        self.assertTrue(keys[5] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11/h:22/mn:41')
        self.assertTrue(keys[6] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12')
        self.assertTrue(keys[7] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12/h:22')
        self.assertTrue(keys[8] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12/h:22/mn:41')

        values = self.agg_engine.get_agg_values_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12', 0)
        self.assertTrue(len(values) == 1)
        value = values[0]
        self.assertTrue(value['key'] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12')

        values = self.agg_engine.get_agg_values_by_key('/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12', 1)
        self.assertTrue(len(values) == 2)
        value = values[0]
        self.assertTrue(value['key'] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:11')
        value = values[1]
        self.assertTrue(value['key'] == '/qtname:test/dt:trans/cust_id:C1/y:2018/m:04/d:12')

    def perform_agg(self, fact_data_type, fact_data):
        self.agg_engine.perform_agg(fact_data_type, fact_data, ['cust_id'], ['year', 'month', 'day', 'hour', 'min'],
                                    ['total_price', 'qty'])
        self.agg_engine.perform_agg(fact_data_type, fact_data, ['cust_id'], ['year', 'month', 'day', 'hour'],
                                    ['total_price', 'qty'])
        self.agg_engine.perform_agg(fact_data_type, fact_data, ['cust_id'], ['year', 'month', 'day'],
                                    ['total_price', 'qty'])
        self.agg_engine.perform_agg(fact_data_type, fact_data, ['cust_id'], ['year', 'month'],
                                    ['total_price', 'qty'])
        self.agg_engine.perform_agg(fact_data_type, fact_data, ['cust_id'], ['year'],
                                    ['total_price', 'qty'])
        self.agg_engine.perform_agg(fact_data_type, fact_data, ['cust_id'], [],
                                    ['total_price', 'qty'])

if __name__ == '__main__':
    unittest.main()