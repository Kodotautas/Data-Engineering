from setuptools import setup, find_packages

setup(
    name='my_package',
    version='0.0.1',
    packages=find_packages(),
    install_requires=['src', 'pandas', 'xml.etree.ElementTree', 'logging', 're'],
    package_data={'src': ['*.py']}
)