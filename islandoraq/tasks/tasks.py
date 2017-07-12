from celery.task import task
from os import chown
from os import chmod
from os import environ, pathsep
from subprocess import check_call, check_output, CalledProcessError
from shutil import rmtree
from tempfile import mkdtemp
from json import loads
import logging
import grp
import requests
import pycurl

from celeryconfig import ISLANDORA_DRUPAL_ROOT, ISLANDORA_FQDN, PATH

logging.basicConfig(level=logging.INFO)

ingest_template = "drush -u 1 oubib --recipe_uri={0} --parent_collection={1} --pid_namespace={2} --tmp_dir={3} --root={4}"
crud_template = "drush -u 1 iim --pid={0}:{1} --operation={2} --root={3}"

environ["PATH"] = PATH + pathsep + environ["PATH"]


@task()
def ingest_recipe(recipe_urls, collection='oku:hos', pid_namespace=None):
    """
    Ingest recipe json file into Islandora repository.
    
    This kickstarts the Islandora local process to import a book collection.
    
    args:
      recipe_urls: List of URLs pointing to json formatted recipe files
      collection: Name of Islandora collection to ingest to. Default is: oku:hos 
      pid_namespace: Namespace to ingest recipe. Default is first half of collection name
    """
    logging.debug("ingest recipe args: {0}, {1}, {2}".format(recipe_urls, collection, pid_namespace))
    logging.debug("Environment: {0}".format(environ))
    
    #ISLANDORA_DRUPAL_ROOT = environ.get("ISLANDORA_DRUPAL_ROOT")
    if not ISLANDORA_DRUPAL_ROOT:
        logging.error("Missing ISLANDORA_DRUPAL_ROOT")
        logging.error(environ)
        raise Exception("Drupal path config not set. Contact your administrator")

    if not pid_namespace:
        pid_namespace = collection.split(":")[0]

    logging.debug("Drupal root path: {0}".format(ISLANDORA_DRUPAL_ROOT))

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
                    ingest_template.format(recipe_url.strip(), collection, pid_namespace, tmpdir, ISLANDORA_DRUPAL_ROOT),
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


@task()
def ingest_status(recipe_url):
    """
    Polls the server to check that objects defined in the recipe_url exist on the server.
    
    args:
      recipe_url: URL string pointing to a json formatted recipe file
    """
 
    if not ISLANDORA_FQDN:
       logging.error("Missing ISLANDORA_FQDN")
       logging.error(environ)
       raise Exception("Missing Islandora FQDN. Contact your administrator")
    
    uuid_url = "https://{0}/uuid/{1}"
    resolve = "{0}:443:127.0.0.1".format(ISLANDORA_FQDN)
    
    # Get UUIDs from recipe file
    try:
        recipe_text = requests.get(recipe_url).text
    except requests.RequestException:
        raise Exception("Bad recipe url")
    recipe_data = loads(recipe_text)
    book_uuid = recipe_data['recipe']['uuid']
    page_uuids = [page['uuid'] for page in recipe_data['recipe']['pages']]

    # Setup curl connection to resolve ISLANDORA_FQDN to 127.0.0.1
    repo = pycurl.Curl()
    repo.setopt(repo.RESOLVE, [resolve])
    repo.setopt(repo.SSL_VERIFYPEER, 0)
    repo.setopt(repo.WRITEFUNCTION, lambda x: None)

    # Check that book is loaded
    repo.setopt(repo.URL, uuid_url.format(ISLANDORA_FQDN, book_uuid))
    repo.perform()
    book_status = repo.getinfo(repo.RESPONSE_CODE)
    if book_status != 200:
        return {"book": book_uuid, "page_status": None, "successful_load": False, 
                "error": "Book not loaded. Received status {0}".format(book_status)}

    # Check individual pages exist
    status = {}
    for uuid in page_uuids:
        page_url = uuid_url.format(ISLANDORA_FQDN, uuid)
        repo.setopt(repo.URL, page_url)
        repo.perform()
        status[uuid] = repo.getinfo(repo.RESPONSE_CODE)
    
    successful_load = all([value == 200 for value in status.values()])
    return {"book": book_uuid, "page_status": status, "successful_load": successful_load}


@task()
def ingest_and_verify(recipe_url, collection='oku:hos', pid_namespace=None):
    """
    Ingest a recipe into Islandora and then verify if it was loaded succeccfully.
    
    args:
      recipe_url: URL string pointing to a json formatted recipe file
      collection: Name of Islandora collection to ingest to. Default is: oku:hos 
      pid_namespace: Namespace to ingest recipe. Default is first half of collection name
    """
    ingest = ingest_recipe.s(recipe_url, collection, pid_namespace)
    verify = ingest_status.si(recipe_url)  # immutable signature to prevent result of ingest being appended
    chain = (ingest | verify)
    result = chain()
    return "Kicked off tasks to ingest recipe and verify ingest"


def _item_manipulator(pid, namespace, operation):
    """ Internal function to call the islandora_item_manipulator (iim) drush script """
    operations = ['read', 'delete']
    if operation not in operations:
        raise Exception("operation must be one of {0}".format(operations))
    drush_response = None
    logging.info("operation: {0}, namespace: {1}, pid: {2}".format(operation, namespace, pid))
    try:
        drush_response = check_output(
            crud_template.format(namespace, pid, operation, ISLANDORA_DRUPAL_ROOT),
            shell=True
        )
        logging.debug(drush_response)
    except CalledProcessError as err:
        logging.error(drush_response)
        logging.error(err)
        logging.error(environ)
        #return {"Error": "Could not perform operation"}
        return {"Error": [drush_response, err.returncode, environ]}
    return drush_response


@task()
def read_item(pid, namespace):
    """
    Read details of an object in Islandora
    
    args:
      pid - The unique identifier of the object (PID / UUID)
      namespace - The collection namespace the object exists in
    """
    return _item_manipulator(pid, namespace, 'read')


@task()
def delete_item(pid, namespace):
    """
    Delete an object from Islandora
    
    args:
      pid - The unique identifier of the object (PID / UUID)
      namespace - The collection namespace the object exists in
    """
    _item_manipulator(pid, namespace, 'delete')
    return True


@task()
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
