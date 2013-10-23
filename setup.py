from distutils.core import setup

setup(
    name='pyhs2',
    version='0.1.0',
    author='Brad Ruderman',
    author_email='bradruderman@gmail.com',
    packages=['pyhs2'],
    url='http://pypi.python.org/pypi/pyhs2/',
    license='LICENSE.txt',
    description='Python Hive Server 2 Client Driver',
    long_description=open('README.txt').read(),
    install_requires=[
        "sasl",
        "thrift",
    ],
)
