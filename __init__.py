import sys
import os

current_file_dir = os.path.dirname(os.path.abspath(__file__))

if current_file_dir not in sys.path:
    sys.path.append(current_file_dir)