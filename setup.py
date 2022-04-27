import os
from setuptools import setup, find_packages

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

dependencies = ["pandas", "numpy", "jupyter", "pyyaml", "matplotlib", "scikit-learn", "seaborn", "mpld3"]

setup(
    name = "dataCleaning",
    version = "0.0.1",
    author = "Robert Rochlin",
    author_email = "rrochling@yahoo.com",
    install_requires = dependencies,
    description = "Project for organizing and analyzing data from UW Seattle Aerospec team\
        particle sensors",
    url = "https://github.com/rrochlin/DataProcessingMaster",
    packages=find_packages(),
    long_description=read('README.md'),
)