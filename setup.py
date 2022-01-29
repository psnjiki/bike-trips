from setuptools import find_packages, setup

with open("./README.md", "r") as f:
    long_description = f.read()

setup(
    name='bike-trips',
    version='1.0.0',
    description='download and preprocess popular bike sharing trips data',
    packages=find_packages(),
    author='P. Samuel Njiki',
    license='MIT License',
    long_description=long_description,
    long_description_content_type="text/markdown")
