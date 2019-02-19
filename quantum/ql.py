import json
import re
from cmd import Cmd
from quantum.quantum_engine import QuantumEngine

class QL(Cmd):

    prompt = 'Q: '

    def __init__(self, config_file_path):
        super().__init__()
        self.quantum_engine = QuantumEngine(config_file_path)

    def do_EOF(self, line):
        print ('')
        return True  # Handles clean exit

    def do_get(self, line):
        filters = line.strip()
        toks = filters.split(';')
        query = {}
        time_units = 0
        plus_minus = ''
        for tok in toks:
            if tok is None or tok == '':
                continue
            tok = tok.strip()
            try:
                tt = tok.split('=')
                if len(tt) == 2:
                    lt = tt[0]
                    rt = tt[1]
                    # Normalize time units with leading 0
                    if lt in ['m', 'w', 'wd', 'd', 'h', 'mn', 's']:
                        if len(rt) == 1:
                            rt = '0' + rt
                    query[lt] = rt
                elif len(tt) == 1:
                    tu = tt[0]
                    tu = tu.strip()
                    matches = re.findall('^[0-9]*', tu)
                    if len(matches) == 1:
                        time_units_str = matches[0]
                        time_units=int(time_units_str)
                        if '+' in tu:
                            plus_minus += '+'
                        if '-' in tu:
                            plus_minus += '-'
                    else:
                        print("Syntax error")
                        return

            except:
                print("Syntax error")
                return

        if len(query) == 0:
            print ("Syntax error")
        else:
            if plus_minus == '':
                plus_minus = '-'
            try:
               #print (query, time_units, plus_minus)
               value = self.quantum_engine.get_agg_values(query, time_units, plus_minus)
               pretty_value = json.dumps(value, indent=4)
               print (pretty_value)
            except:
               print("Syntax error")
               return
