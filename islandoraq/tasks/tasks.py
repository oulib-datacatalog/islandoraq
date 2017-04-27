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

logging.basicConfig(level=logging.INFO)

ISLANDORA_DRUPAL_ROOT = environ.get("ISLANDORA_DRUPAL_ROOT")
ingest_template = "drush -u 1 oubib --recipe_uri={0} --parent_collection={1} --tmp_dir={2} --root={3}"


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
    if not ISLANDORA_DRUPAL_ROOT:
        raise Exception("Drupal path config not set. Contact your administrator")

    recipe_urls = [recipe_urls] if not isinstance(recipe_urls, list) else recipe_urls
    
    fail = [] 
    success = []
    for recipe_url in recipe_urls:
        logging.debug("ingesting: {0}".format(recipe_url.strip()))
        testresp = requests.head(recipe_url, allow_redirects=True)
        if testresp.status_code == requests.codes.ok:
            tmpdir = mkdtemp(prefix="recipeloader_")
            logging.debug("created working dir: {0}".format(tmpdir))
            chmod(tmpdir, 0o775)
            chown(tmpdir, -1, grp.getgrnam("apache").gr_gid)
            try:
                drush_response = None
                #-----------------
                drush_response = check_output(
                    ingest_template.format(recipe_url.strip(), collection, tmpdir, ISLANDORA_DRUPAL_ROOT),
                    shell=True
                )
                #-----------------
                #drush_response = check_output([
                #    "drush",
                #    "-u",
                #    "1",
                #    "oubib",
                #    "--recipe_uri={0}".format(recipe_url.strip()),
                #    "--parent_collection={0}".format(collection),
                #    "--tmp_dir={0}".format(tmpdir),
                #    "--root={0}".format(ISLANDORA_DRUPAL_ROOT)
                #    ])
                #-----------------
                #drush_response = check_output("/opt/php/bin/drush", shell=True)
                #-----------------
                logging.debug(drush_response)
                success.append(recipe_url)
            except CalledProcessError as err:
                fail.append([recipe_url, "Drush status {0}".format(err.returncode)])
                logging.error(drush_response)
                logging.error(err)
                logging.error(environ)
            finally:
                rmtree(tmpdir)
                logging.debug("removed working dir")
        else:
            logging.error("Issue getting recipe at: {0}".format(recipe_url))
            fail.append([recipe_url, "Server status {0}".format(testresp.status_code)])
            
    return ({"Successful": success, "Failures": fail})


@task
def clear_drush_cache():
    check_call(["drush", "cache-clear", "drush"])
    return True


# added to asssist with testing connectivity
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result
