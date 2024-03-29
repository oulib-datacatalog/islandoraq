#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='islandoraq',
      version='0.3.6',
      packages= find_packages(),
      install_requires=[
          'celery==5.2.7 ; python_version >= "3.7"',
          'celery==3.1.22 ; python_version == "2.7"',
          'pymongo==4.3.3; python_version >= "3.7"',
          'pymongo==3.2.1; python_version == "2.7"',
          'requests==2.28.2; python_version >= "3.7"',
          'requests==2.24.0; python_version == "2.7"',
      ],
      include_package_data=True,
)
