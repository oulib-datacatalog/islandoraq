#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='islandoraq',
      version='0.2.6.76',
      packages= find_packages(),
      install_requires=[
          'celery==3.1.22',
          'pymongo==3.2.1',
          'requests==2.20.0',
          'pycurl==7.43.0',
      ],
      include_package_data=True,
)
