from setuptools import setup
from Cython.Build import cythonize
import os

current_file_path = os.path.abspath(__file__)
directory_path = os.path.dirname(current_file_path)
print(directory_path)
setup(
    ext_modules = cythonize(f"{directory_path}/utils.pyx")
)
