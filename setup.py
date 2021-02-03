from setuptools import setup
from pysyncobj.version import VERSION

description='A library for replicating your python class between multiple servers, based on raft protocol'
try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError, RuntimeError):
    long_description = description

setup(
    name='pysyncobj',
    packages=['pysyncobj'],
    version=VERSION,
    description=description,
    long_description=long_description,
    author='Filipp Ozinov',
    author_email='fippo@mail.ru',
    license='MIT',
    url='https://github.com/bakwc/PySyncObj',
    download_url='https://github.com/bakwc/PySyncObj/tarball/' + VERSION,
    keywords=['network', 'replication', 'raft', 'synchronization'],
    classifiers=[
        'Topic :: System :: Networking',
        'Topic :: System :: Distributed Computing',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
        'License :: OSI Approved :: MIT License',
    ],
    entry_points={
        'console_scripts': [
            'syncobj_admin=pysyncobj.syncobj_admin:main',
        ],
    },
)
