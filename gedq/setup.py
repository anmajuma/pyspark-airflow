from setuptools import setup , find_packages

setup( 
    name='gedq', 
    version='0.1', 
    description='Python package for dataquality check', 
    author='John Doe', 
    author_email='jdoe@example.com', 
    packages=find_packages(), 
    install_requires=[ 
        'great-expectations'
    ], 
) 