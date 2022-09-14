#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='islandoraq',
      version='0.3.4',
      packages= find_packages(),
      install_requires=[
          'celery==5.2.2 ; python_version >= "3.7"',
          'celery==3.1.22 ; python_version == "2.7"',
          'pymongo==3.12.3; python_version > "2.7"',
          'pymongo==3.2.1; python_version == "2.7"',
          'requests==2.24.0',
      ],
      include_package_data=True,
)
