from celery.task import task
from os import chown
from os import chmod
from os import environ, pathsep
from subprocess import check_call, check_output, CalledProcessError
from shutil import rmtree
from tempfile import mkdtemp
import logging
import grp
import requests

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
    logging.debug("ingest recipe args: {0}, {1}".format(recipe_urls, collection))
    logging.debug("Environment: {0}".format(environ))
    logging.debug("root path: {0}".format(ISLANDORA_DRUPAL_ROOT))
    fail = 0
    success = 0
    for recipe_url in recipe_urls.split(","):
        logging.debug("ingesting: {0}".format(recipe_url.strip()))
        tmpdir = mkdtemp(prefix="recipeloader_")
        logging.debug("created working dir: {0}".format(tmpdir))
        chmod(tmpdir, 0o775)
        chown(tmpdir, -1, grp.getgrnam("apache").gr_gid)
        try:
            if requests.get(recipe_url).status_code == 200:
                drush_response = check_output("drush -u 1 oubib --recipe_uri={0} --parent_collection={1} --tmp_dir={2} --root={3}".format(
                    recipe_url.strip(), collection, tmpdir, ISLANDORA_DRUPAL_ROOT
                    ),
                        shell=True
                    )
                logging.debug(drush_response)
                success += 1
            else:
                logging.error("Issue getting recipe at: {0}".format(recipe_url))
                fail += 1
        except CalledProcessError as err:
            fail += 1
            logging.error(err)
            logging.error(environ)
            logging.error(drush_response)
        finally:
            rmtree(tmpdir)
            logging.debug("removed working dir")
            
    return({"Successful": success, "Failures": fail})


# added to asssist with testing connectivity
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result
