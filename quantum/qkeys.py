import datetime
import arrow
import pickle

class QKeys():

    def __init__(self, cache):
        self.cache = cache

    def get_dimension_value(self, key_pattern, dimension_token):
        len_token = len(dimension_token)
        dimension_value = None
        if dimension_token in key_pattern:
            idx = key_pattern.index(dimension_token)
            s = key_pattern[idx + len_token:]
            if '/' in s:
                idx = s.index('/')
                dimension_value = s[:idx]
            else:
                dimension_value = s
        return dimension_value

    #
    # Returns a list of keys matching pattern in alphabetically sorted order
    # e.g. get '*'
    #
    def get(self, key_pattern):
        key_list = self.cache.keys(key_pattern)
        key_list_2 = []
        for k in key_list:
            key_list_2.append(k.decode('utf-8'))

        sorted_key_list = sorted(key_list_2)
        return sorted_key_list

    def get_value_from_cache(self, key):
        return pickle.loads(self.cache.get(key))

    def get_value(self, key):
        if not self.cache.exists(key):
            return None
        else:
            return self.get_value_from_cache(key)

    # direction can be one of '-', '+', '-+', '+-'
    # example:  base_key:     /dt:auths/p:Square/y:2018/m:12/d:14/h:2
    #           time_units:   2
    #           direction:    +=
    def get_values(self, base_key, time_units, direction='-'):
        results = []

        t_dimensions = self.get_t_dimensions(base_key)
        if len(t_dimensions) == 0:
            results.append(self.get_value_from_cache(base_key))
            return results

        nt_dimensions = self.get_nt_dimensions(base_key)

        if direction == '-' or direction == '-+' or direction == '+-':
            results.extend(
                self.do_get_values(self.cache, base_key, '-', direction, nt_dimensions, t_dimensions, time_units))

        if direction == '+' or direction == '-+' or direction == '+-':
            results.extend(
                self.do_get_values(self.cache, base_key, '+', direction, nt_dimensions, t_dimensions, time_units))

        return results

    def do_get_values(self, redis_cache, base_key, current_dir, original_dir, nt_dimensions, t_dimensions, time_units):
        results = []
        include_current = self.get_include_current(current_dir, original_dir)
        granularity = self.get_time_granularity(t_dimensions)
        (current_date, target_date) = self.get_current_and_target_dates(current_dir, time_units, granularity,
                                                                        t_dimensions)
        if current_dir == '-':
            if include_current:
                while target_date <= current_date:
                    self.process_key(redis_cache, nt_dimensions, t_dimensions, target_date, results)
                    target_date = self.shift_date(target_date, granularity, 1)
            else:
                while target_date < current_date:
                    self.process_key(redis_cache, nt_dimensions, t_dimensions, target_date, results)
                    target_date = self.shift_date(target_date, granularity, 1)
        else:
            if not include_current:
                current_date = self.shift_date(current_date, granularity, 1)
            while current_date <= target_date:
                self.process_key(redis_cache, nt_dimensions, t_dimensions, current_date, results)
                current_date = self.shift_date(current_date, granularity, 1)

        return results

    def process_key(self, redis_cache, nt_dimensions, t_dimensions, some_date, results):
        some_key = self.get_key(nt_dimensions, t_dimensions, some_date)
        if redis_cache.exists(some_key):
            some_value = self.get_value_from_cache(some_key)
            results.append({'key': some_key, 'value': some_value})

    def get_time_granularity(self, t_dimensions):
        granularity = 'y'
        for td in t_dimensions:
            toks = td.split(':')
            prefix = toks[0]
            granularity = prefix
        return granularity

    def get_current_and_target_dates(self, current_dir, time_units, granularity, t_dimensions):
        td_values = {}
        for td in t_dimensions:
            toks = td.split(':')
            prefix = toks[0]
            value = toks[1]
            td_values[prefix] = int(value)

        some_date = None
        target_date = None

        if granularity == 'mn':
            if 'wd' not in td_values:
                some_date = arrow.get(td_values['y'], td_values['m'], td_values['d'], hour=td_values['h'],
                                      minute=td_values['mn'])
            else:
                dt_str = str(td_values['y']) + '-' + str(td_values['w']) + '-' + str(td_values['wd'] + 1) + '-' + str(
                    td_values['h']) + '-' + str(td_values['mn'])
                dt = datetime.datetime.strptime(dt_str, '%Y-%W-%w-%H-%M')
                some_date = arrow.get(dt)
            if current_dir == '-':
                target_date = some_date.shift(minutes=-time_units)
            else:
                target_date = some_date.shift(minutes=time_units)

        elif granularity == 'h':
            if 'wd' not in td_values:
                some_date = arrow.get(td_values['y'], td_values['m'], td_values['d'], hour=td_values['h'])
            else:
                dt_str = str(td_values['y']) + '-' + str(td_values['w']) + '-' + str(td_values['wd'] + 1) + '-' + str(
                    td_values['h'])
                dt = datetime.datetime.strptime(dt_str, '%Y-%W-%w-%H')
                some_date = arrow.get(dt)
            if current_dir == '-':
                target_date = some_date.shift(hours=-time_units)
            else:
                target_date = some_date.shift(hours=time_units)

        elif granularity == 'd':
            some_date = arrow.get(td_values['y'], td_values['m'], td_values['d'])
            if current_dir == '-':
                target_date = some_date.shift(days=-time_units)
            else:
                target_date = some_date.shift(days=time_units)

        elif granularity == 'wd':
            dt_str = str(td_values['y']) + '-' + str(td_values['w']) + '-' + str(td_values['wd'] + 1)
            dt = datetime.datetime.strptime(dt_str, '%Y-%W-%w')
            some_date = arrow.get(dt)
            if current_dir == '-':
                target_date = some_date.shift(days=-time_units)
            else:
                target_date = some_date.shift(days=time_units)

        elif granularity == 'm':
            some_date = arrow.get(td_values['y'], td_values['m'], 1)
            if current_dir == '-':
                target_date = some_date.shift(months=-time_units)
            else:
                target_date = some_date.shift(months=time_units)

        else:
            some_date = arrow.get(td_values['y'], 1, 1)
            if current_dir == '-':
                target_date = some_date.shift(years=-time_units)
            else:
                target_date = some_date.shift(years=time_units)

        return (some_date, target_date)

    def get_include_current(self, current_dir, original_dir):
        if current_dir == '-':
            include_current = True
        else:
            if original_dir == '+-' or original_dir == '-+':
                include_current = False
            else:
                include_current = True
        return include_current

    def get_t_dimensions(self, key):
        t_dimensions = []
        if '/y:' in key:
            year_str = self.get_dimension_value(key, '/y:')
            t_dimensions.append('y:' + year_str)
        if '/m:' in key:
            month_str = self.get_dimension_value(key, '/m:')
            t_dimensions.append('m:' + month_str)
        if '/d:' in key:
            day_str = self.get_dimension_value(key, '/d:')
            t_dimensions.append('d:' + day_str)
        if '/w:' in key:
            week_str = self.get_dimension_value(key, '/w:')
            t_dimensions.append('w:' + week_str)
        if '/wd:' in key:
            day_of_week_str = self.get_dimension_value(key, '/wd:')
            t_dimensions.append('wd:' + day_of_week_str)
        if '/h:' in key:
            hour_str = self.get_dimension_value(key, '/h:')
            t_dimensions.append('h:' + hour_str)
        if '/mn:' in key:
            minute_str = self.get_dimension_value(key, '/mn:')
            t_dimensions.append('mn:' + minute_str)
        if '/s:' in key:
            second_str = self.get_dimension_value(key, '/s:')
            t_dimensions.append('s:' + second_str)
        return t_dimensions

    def get_nt_dimensions(self, key):
        nt_dimensions = []
        toks = key.split('/')
        for tok in toks:
            if tok == '':
                continue
            dv = tok.split(':')
            v1 = dv[0]
            if v1 in ['y', 'm', 'd', 'w', 'wd', 'h', 'mn', 's']:
                continue
            v2 = dv[1]
            nt_dimensions.append(v1 + ':' + v2)
        return nt_dimensions

    def get_key(self, nt_dimensions, t_dimensions, t_date):
        target_key = ''
        for ntd in nt_dimensions:
            target_key = target_key + '/' + ntd
        for td in t_dimensions:
            toks = td.split(':')
            prefix = toks[0]
            if prefix == 'y':
                t_year = '{0:02d}'.format(t_date.year)
                target_key = target_key + '/y:' + t_year
            elif prefix == 'm':
                t_month = '{0:02d}'.format(t_date.month)
                target_key = target_key + '/m:' + t_month
            elif prefix == 'w':
                t_week = '{0:02d}'.format(t_date.week)
                target_key = target_key + '/w:' + t_week
            elif prefix == 'wd':
                t_day_of_week = '{0:02d}'.format(t_date.weekday())
                target_key = target_key + '/wd:' + t_day_of_week
            elif prefix == 'd':
                t_day = '{0:02d}'.format(t_date.day)
                target_key = target_key + '/d:' + t_day
            elif prefix == 'h':
                t_hour = '{0:02d}'.format(t_date.hour)
                target_key = target_key + '/h:' + t_hour
            elif prefix == 'mn':
                t_minute = '{0:02d}'.format(t_date.minute)
                target_key = target_key + '/mn:' + t_minute

        return target_key

    def shift_date(self, some_date, granularity, time_units):
        new_date = some_date
        if granularity == 'y':
            new_date = some_date.shift(years=time_units)
        elif granularity == 'm':
            new_date = some_date.shift(months=time_units)
        elif granularity == 'w':
            new_date = some_date.shift(weeks=time_units)
        elif granularity == 'd':
            new_date = some_date.shift(days=time_units)
        elif granularity == 'wd':
            new_date = some_date.shift(days=time_units)
        elif granularity == 'h':
            new_date = some_date.shift(hours=time_units)
        elif granularity == 'mn':
            new_date = some_date.shift(minutes=time_units)
        elif granularity == 's':
            new_date = some_date.shift(sceconds=time_units)
        return new_date
