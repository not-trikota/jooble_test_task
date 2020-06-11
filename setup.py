from setuptools import setup
from setuptools import find_packages

with open('requirements.txt') as f:
    requirements = f.read().split('\n')

setup(
    name="jooble_transformer",
    version="0.1",
    author="Gregory Berzin",
    author_email="gregoryberzin@gmail.com",
    packages=find_packages(),
    install_requires=requirements,
)
