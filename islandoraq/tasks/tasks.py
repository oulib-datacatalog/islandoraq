from celery.task import task
from os import chown
from os import chmod
from os import environ, pathsep
from subprocess import check_call, CalledProcessError
from shutil import rmtree
from tempfile import mkdtemp
import logging
import grp

from celeryconfig import ISLANDORA_DRUPAL_ROOT

logging.basicConfig(level=logging.DEBUG)

needed_paths = ["/opt/php/bin", "/opt/d7/bin"]


@task()
def ingest_recipe(recipe_urls, collection='islandora:bookCollection'):
    """
    Ingest recipe json file into Islandora repository.
    
    This kickstarts the Islandora local process to import a book collection.
    
    args:
      recipe_urls: List of URLs pointing to json formatted recipe files
      collection: Name of Islandora collection to ingest to. Default is: islandora:bookCollection  
    """
    logging.debug("ingest recipe args: {0}, {1}".format(recipe_urls, collection))
    logging.debug("Environment: {0}".format(environ))
    for recipe_url in recipe_urls:
        logging.debug("ingesting: {0}".format(recipe_url))
        tmpdir = mkdtemp(prefix="recipeloader_")
        logging.debug("created working dir: {0}".format(tmpdir))
        chmod(tmpdir, 0o775)
        chown(tmpdir, -1, grp.getgrnam("apache").gr_gid)
        try:
            check_call(['drush', '-u', '1', 'oubib',
                        '--recipe_uri={0}'.format(recipe_url),
                        '--parent_collection={0}'.format(collection),
                        '--tmp_dir={0}'.format(tmpdir),
                        '--root={0}'.format(ISLANDORA_DRUPAL_ROOT)
                        ],
                       shell=True,
                       env={"PATH": pathsep.join(needed_paths) + pathsep + environ["PATH"]}
                       )
        except CalledProcessError as err:
            logging.error(err)
            logging.error(environ)
            return({"ERROR": "Ingest command failed"})
        finally:
            #rmtree(tmpdir)
            logging.debug("removed working dir")
            pass

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
