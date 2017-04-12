from celery.task import task
from os import chown
from os import chmod
from os import environ
from subprocess import check_call, CalledProcessError
from shutil import rmtree
from tempfile import mkdtemp
import logging
import grp

from celeryconfig import ISLANDORA_DRUPAL_ROOT

logging.basicConfig(level=logging.INFO)


@task()
def ingest_recipe(recipe_urls, collection='islandora:bookCollection'):
    """
    Ingest recipe json file into Islandora repository.
    
    This kickstarts the Islandora local process to import a book collection.
    
    args:
      recipe_urls: List of URLs pointing to json formatted recipe files
      collection: Name of Islandora collection to ingest to. Default is: islandora:bookCollection  
    """
    
    for recipe_url in recipe_urls:
        tmpdir = mkdtemp(prefix="recipeloader_")
        chmod(tmpdir, 0o775)
        chown(tmpdir, -1, grp.getgrnam("apache").gr_gid)
        try:
            check_call(['drush', '-u', '1', 'oubib',
                        '--recipe_uri={0}'.format(recipe_url),
                        '--parent_collection={0}'.format(collection),
                        '--tmp_dir={0}'.format(tmpdir),
                        '--root={0}'.format(ISLANDORA_DRUPAL_ROOT)
                        ],
                       shell=True
                       )
            pass
        except CalledProcessError as err:
            logging.error(err)
            logging.error(environ)
            return({"ERROR": "Ingest command failed"})
        finally:
            rmtree(tmpdir)

        return("SUCCESS")  # TODO: return islandora url for ingested book


# added to asssist with testing connectivity
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result
