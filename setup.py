from distutils.core import setup

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError, RuntimeError):
    long_description = open('README.md').read()

setup(
    name='pysyncobj',
    packages=['pysyncobj'],
    version='0.1.4',
    description='A library for replicating your python class between multiple servers, based on raft protocol',
    long_description=long_description,
    author='Filipp Ozinov',
    author_email='fippo@mail.ru',
    url='https://github.com/bakwc/PySyncObj',
    download_url='https://github.com/bakwc/PySyncObj/tarball/0.1.4',
    keywords=['network', 'replication', 'raft', 'synchronization'],
    classifiers=[
        'Topic :: System :: Networking',
        'Topic :: System :: Distributed Computing',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 2 :: Only',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
        'License :: OSI Approved :: MIT License',
    ],
)
