import os
import sys

# Get the directory of the current file
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the directory to sys.path if it is not already present
if current_dir not in sys.path:
    sys.path.append(current_dir)
