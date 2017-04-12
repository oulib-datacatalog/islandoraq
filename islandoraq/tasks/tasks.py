from celery.task import task
from os import chown
from os import chmod
from os import environ, pathsep
from subprocess import check_call, check_output, CalledProcessError
from shutil import rmtree
from tempfile import mkdtemp
import logging
import grp

from celeryconfig import ISLANDORA_DRUPAL_ROOT

logging.basicConfig(level=logging.INFO)

needed_paths = ["/opt/php/bin", "/opt/d7/bin"]
environ["PATH"] = pathsep.join(needed_paths) + pathsep + environ["PATH"]

@task()
def ingest_recipe(recipe_urls, collection='islandora:bookCollection'):
    """
    Ingest recipe json file into Islandora repository.
    
    This kickstarts the Islandora local process to import a book collection.
    
    args:
      recipe_urls: Comma seperated string of URLs pointing to json formatted recipe files
      collection: Name of Islandora collection to ingest to. Default is: islandora:bookCollection  
    """
    logging.error("ingest recipe args: {0}, {1}".format(recipe_urls, collection)) # debug
    logging.error("Environment: {0}".format(environ)) # debug
    logging.error("root path: {0}".format(ISLANDORA_DRUPAL_ROOT)) # debug
    for recipe_url in recipe_urls.split(","):
        logging.error("ingesting: {0}".format(recipe_url.strip())) # debug
        tmpdir = mkdtemp(prefix="recipeloader_")
        logging.error("created working dir: {0}".format(tmpdir)) # debug
        chmod(tmpdir, 0o775)
        chown(tmpdir, -1, grp.getgrnam("apache").gr_gid)
        try:
            #check_call([
            drush_response = check_output([
                'drush', '-u', '1', 'oubib',
                '--recipe_uri={0}'.format(recipe_url.strip()),
                '--parent_collection={0}'.format(collection),
                '--tmp_dir={0}'.format(tmpdir),
                '--root={0}'.format(ISLANDORA_DRUPAL_ROOT),
                ],
                    shell=True
                )
            logging.error(drush_response)
        except CalledProcessError as err:
            logging.error(err)
            logging.error(environ)
            logging.error(drush_response)
            return({"ERROR": "Ingest command failed"})
        finally:
            #rmtree(tmpdir)
            logging.error("removed working dir") # debug
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
