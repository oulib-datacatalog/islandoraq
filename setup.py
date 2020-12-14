#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='islandoraq',
      version='0.3.2',
      packages= find_packages(),
      install_requires=[
          'celery',
          'pymongo',
          'requests==2.24.0',
      ],
      include_package_data=True,
)
