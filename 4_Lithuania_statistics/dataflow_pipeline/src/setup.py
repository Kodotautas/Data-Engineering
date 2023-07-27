from setuptools import setup, find_packages

# List all your package dependencies here
REQUIRED_PACKAGES = [
    'requests',
]

setup(
    name='my_dataflow_pipeline',
    version='1.0',
    packages=find_packages(),
    install_requires=REQUIRED_PACKAGES,
    include_package_data=True,
    description='My Dataflow Pipeline',
)

# run: python3 setup.py sdist
