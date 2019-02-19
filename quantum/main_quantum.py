import sys
from quantum.quantum_engine import QuantumEngine

def run_quantum():
    numargs = len(sys.argv)
    if numargs != 2:
        print('Usage: quantum <config_file_path>')
    else:
        config_file_path = sys.argv[1]
        QuantumEngine(config_file_path).run()