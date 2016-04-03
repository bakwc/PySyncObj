from distutils.core import setup
setup(
  name = 'pysyncobj',
  packages = ['pysyncobj'],
  version = '0.1',
  description = 'A library for replicating your python class between multiple servers, '
				'based on raft protocol',
  author = 'Filipp Ozinov',
  author_email = 'fippo@mail.ru',
  url = 'https://github.com/bakwc/PySyncObj',
  download_url = 'https://github.com/bakwc/PySyncObj/tarball/0.1',
  keywords = ['network', 'replication', 'raft', 'synchronization'],
  classifiers = [
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
