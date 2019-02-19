import unittest
import redis
import os
from quantum.quantum_engine import QuantumEngine

class QuantumEngineTest(unittest.TestCase):
    def setUp(self):
        redis_host = 'localhost'
        redis_port = 6379
        self.redis_cache = redis.Redis(host=redis_host, port=redis_port, db=10)
        self.redis_cache.flushdb()

        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.quantum_engine = QuantumEngine(ROOT_DIR + '/myagg.yml')

    def tearDown(self):
        pass

    def testRun1(self):
        self.quantum_engine.run()

        keys = self.quantum_engine.get_agg_keys('*')
        self.assertTrue(len(keys) == 52)

        for key in keys:
            print(key)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:myagg/dt:transaction/cust_id:C1')
        self.assertTrue(value['count'] == 2.0)
        self.assertTrue(value['sum_total_price'] == 55.0)
        self.assertTrue(value['avg_total_price'] == 27.5)
        self.assertTrue(value['sum_qty'] == 5.0)
        self.assertTrue(value['avg_qty'] == 2.5)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:myagg/dt:transaction/cust_id:C1/y:2018')
        self.assertTrue(value['count'] == 2.0)
        self.assertTrue(value['sum_total_price'] == 55.0)
        self.assertTrue(value['avg_total_price'] == 27.5)
        self.assertTrue(value['sum_qty'] == 5.0)
        self.assertTrue(value['avg_qty'] == 2.5)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:myagg/dt:transaction/cust_id:C1/y:2018/m:04')
        self.assertTrue(value['count'] == 2.0)
        self.assertTrue(value['sum_total_price'] == 55.0)
        self.assertTrue(value['avg_total_price'] == 27.5)
        self.assertTrue(value['sum_qty'] == 5.0)
        self.assertTrue(value['avg_qty'] == 2.5)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:myagg/dt:transaction/cust_id:C1/y:2018/m:04/d:11')
        self.assertTrue(value['count'] == 1.0)
        self.assertTrue(value['sum_total_price'] == 10.0)
        self.assertTrue(value['avg_total_price'] == 10.0)
        self.assertTrue(value['sum_qty'] == 2.0)
        self.assertTrue(value['avg_qty'] == 2.0)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:myagg/dt:transaction/cust_id:C1/y:2018/m:04/d:11/h:22')
        self.assertTrue(value['count'] == 1.0)
        self.assertTrue(value['sum_total_price'] == 10.0)
        self.assertTrue(value['avg_total_price'] == 10.0)
        self.assertTrue(value['sum_qty'] == 2.0)
        self.assertTrue(value['avg_qty'] == 2.0)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:myagg/dt:transaction/cust_id:C1/y:2018/m:04/d:11/h:22/mn:41')
        self.assertTrue(value['count'] == 1.0)
        self.assertTrue(value['sum_total_price'] == 10.0)
        self.assertTrue(value['avg_total_price'] == 10.0)
        self.assertTrue(value['sum_qty'] == 2.0)
        self.assertTrue(value['avg_qty'] == 2.0)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:myagg/dt:transaction/cust_id:C1/y:2018/m:04/d:12/h:03')
        self.assertTrue(value['count'] == 1.0)
        self.assertTrue(value['sum_total_price'] == 45.0)
        self.assertTrue(value['avg_total_price'] == 45.0)
        self.assertTrue(value['sum_qty'] == 3.0)
        self.assertTrue(value['avg_qty'] == 3.0)

        value = self.quantum_engine.get_agg_value_by_key('/qtname:myagg/dt:transaction/cust_id:C1/y:2018/m:04/d:12/h:03/mn:12')
        self.assertTrue(value['count'] == 1.0)
        self.assertTrue(value['sum_total_price'] == 45.0)
        self.assertTrue(value['avg_total_price'] == 45.0)
        self.assertTrue(value['sum_qty'] == 3.0)
        self.assertTrue(value['avg_qty'] == 3.0)

if __name__ == '__main__':
    unittest.main()