from distutils.core import setup
from setuptools import setup
import glob

setup(name='emr_boto',
      version='1.0',
      description='emr boto examples',
      author='Paul Tremblay',
      author_email='paulhtremblay@gmail.com',
      url='https://www.python.org/sigs/distutils-sig/',
      packages=['boto_emr'],
      scripts = glob.glob("scripts/*"),

     )
