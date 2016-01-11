from setuptools import setup

setup(
    name='pyhs2',
    version='0.6.0',
    author='Brad Ruderman',
    author_email='bradruderman@gmail.com',
    packages=['pyhs2', 'pyhs2/twitter', 'pyhs2/TCLIService'],
    url='https://github.com/BradRuderman/pyhs2',
    license='LICENSE.txt',
    description='Python Hive Server 2 Client Driver',
    long_description=open('README.md').read(),
    install_requires=[
        "pure-sasl>=0.1.7",
        "thrift",
    ],
    test_suite='pyhs2.test',
    tests_require=["mock"]

)
