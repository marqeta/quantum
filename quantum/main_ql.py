import sys
from quantum.ql import QL

def run_ql():
    numargs = len(sys.argv)
    if numargs != 2:
        print('Usage: ql <config_file_path>')
    else:
        config_file_path = sys.argv[1]
        QL(config_file_path).cmdloop()