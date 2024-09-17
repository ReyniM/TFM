from setuptools import setup, find_packages

setup(
    name='data_pipeline',
    version='1.0',
    packages=find_packages(),
    description='Data engine for my tfm project.',
    author='Reyni Manuel Calderon Cots',
    author_email='reynim16@gmail.com',
    install_requires=[
        'googlemaps==4.10.0',
        'numpy==1.26.4',
        'pandas==2.2.2',
        'PyYAML==6.0.1',
        'requests==2.32.3',
    ],
)
