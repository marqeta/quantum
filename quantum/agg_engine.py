import datetime
import logging
import pickle
from quantum.qkeys import QKeys
from redis.exceptions import WatchError

class AggEngine():

    def __init__(self, name, config, cache):
        self.name = name
        self.cache = cache
        self.pipe = self.cache.pipeline()
        self.config = config
        self.qkeys = QKeys(self.cache)

    def perform_agg(self, fact_data_type, fact_data, nt_dimensions, t_dimensions, measures):

        while True:
            key = self.generate_agg_key(fact_data_type, fact_data, nt_dimensions, t_dimensions)
            try:
                self.pipe.watch(key)
                if not self.pipe.exists(key):
                    new_v = {}
                    for measure in measures:
                        sum_measure_key = 'sum_' + measure
                        avg_measure_key = 'avg_' + measure
                        new_v[sum_measure_key] = float(fact_data[measure])
                        new_v[avg_measure_key] = float(fact_data[measure])
                    new_v['count'] = 1
                    self.pipe.set(key, pickle.dumps(new_v))
                else:
                    v = pickle.loads(self.cache.get(key))
                    new_v = {}
                    new_count_value = int(v['count']) + 1
                    for measure in measures:
                        sum_measure_key = 'sum_' + measure
                        avg_measure_key = 'avg_' + measure
                        new_sum_value = float(v[sum_measure_key]) + float(fact_data[measure])
                        new_avg_value = new_sum_value / new_count_value
                        new_v[sum_measure_key] = float(new_sum_value)
                        new_v[avg_measure_key] = float(new_avg_value)

                    new_v['count'] = new_count_value
                    self.pipe.set(key, pickle.dumps(new_v))

                self.pipe.execute()
                return (key, new_v)
            except WatchError:
                #logging.info('retry')
                continue  #retr  y
            except Exception as e:
                logging.error (str(e))
                break
            finally:
                self.pipe.reset()

    def generate_agg_key(self, fact_data_type,  fact_data, nt_dimensions, t_dimensions):
        dimensions = []
        for nt in nt_dimensions:
            prefix = nt
            dimensions.append(prefix+':'+fact_data[nt])

        for t in t_dimensions:
            if t == 'sec':
                prefix = 's'
            elif t == 'min':
                prefix = 'mn'
            elif t == 'hour':
                prefix = 'h'
            elif t == 'day':
                prefix = 'd'
            elif t == 'day_of_week':
                prefix = 'wd'
            elif t == 'week':
                prefix = 'w'
            elif t == 'month':
                prefix = 'm'
            elif t == 'year':
                prefix = 'y'
            else:
                raise ValueError('Invalid time dimension')

            dimensions.append(prefix+':'+str(fact_data[t]))
            
        key = '/qtname:' + self.name + '/dt:' + fact_data_type
        for d in dimensions:
            key += '/' + d
        return key

    def get_week_number(self, year, month, day):
        return datetime.date(int(year), int(month), int(day)).strftime('%V')

    def get_day_of_week(self, year, month, day):
        return datetime.date(int(year), int(month), int(day)).weekday()

    def add_time_dimensions(self, fact_data, datetime_field_name, date_format):
        # TODO use date_format
        tdate = fact_data[datetime_field_name]
        fact_data['year']  = tdate[0:4]
        fact_data['month'] = tdate[5:7]
        fact_data['day']   = tdate[8:10]
        fact_data['hour']  = tdate[11:13]
        fact_data['min']   = tdate[14:16]
        fact_data['sec']   = tdate[17:19]
        fact_data['week']  = self.get_week_number(fact_data['year'], fact_data['month'], fact_data['day'])
        fact_data['day_of_week'] = self.get_day_of_week(fact_data['year'], fact_data['month'], fact_data['day'])
        return fact_data

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

