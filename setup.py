from setuptools import find_packages, setup

with open("./README.md", "r") as f:
    long_description = f.read()

setup(
    name='bike-trips',
    version='0.1.0',
    description='download and preprocess popular bike sharing trip data',
    packages=find_packages(),
    author='P. Samuel Njiki',
    license='',
    long_description=long_description,
    long_description_content_type="text/markdown")
