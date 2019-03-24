import sys
from quantum.quantum_engine import QuantumEngine

def run_quantum():
    numargs = len(sys.argv)
    if numargs < 2:
        print('Usage: quantum <config_file_path>')
    else:
        override_file_path = None
        config_file_path = sys.argv[1]
        if numargs == 3:
            override_file_path = sys.argv[2]
        QuantumEngine(config_file_path, override_file_path).run()
