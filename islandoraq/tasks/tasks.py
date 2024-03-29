from celery import Celery
from os import chown
from os import chmod
from os import environ, pathsep
from os.path import join
from subprocess import check_call, check_output, CalledProcessError, STDOUT
from shutil import rmtree
from tempfile import mkdtemp
from json import loads, dumps
import datetime
import logging
import grp
import requests
from requests.exceptions import ConnectionError

logging.basicConfig(level=logging.INFO)

try:
    from urlparse import urlparse
except:
    from urllib.parse import urlparse

try:
    import celeryconfig
except ImportError:
    logging.error('Failed to import celeryconfig!')
    celeryconfig = None

try:
    from celeryconfig import ISLANDORA_DRUPAL_ROOT, ISLANDORA_FQDN, PATH, CYBERCOMMONS_TOKEN
except ImportError:
    logging.error("Failed to import environment variables from celeryconfig!")
    ISLANDORA_DRUPAL_ROOT = ""
    ISLANDORA_FQDN = ""
    PATH = ""
    CYBERCOMMONS_TOKEN = ""

app = Celery()
app.config_from_object(celeryconfig)

ingest_template = "drush -u 1 oubib --recipe_uri={0} --parent_collection={1} --pid_namespace={2} --tmp_dir={3} --root={4}"
crud_template = "drush -u 1 iim --pid={0}:{1} --operation={2} --root={3}"

base_url = "https://cc.lib.ou.edu"
api_url = "{0}/api".format(base_url)
catalog_url = "{0}/catalog/data/catalog/digital_objects/.json".format(api_url)
search_url = "{0}?query={{\"filter\": {{\"bag\": \"{1}\"}}}}"

environ["PATH"] = PATH + pathsep + environ["PATH"]


def is_uri(item):
    """ check if item looks like a uri returning True or False """
    if type(item) != str:
        return False
    else:
        return urlparse(item).netloc != ''


def is_recipe(item):
    """ check if item looks like a valid recipe file returning True or False """
    if type(item) == dict and item.get("recipe"):
        return True
    elif type(item) in [str, bytes]:
        try:
            return loads(item).get("recipe") is not None
        except:
            return False
    else:
        return False


def searchcatalog(bag):
    resp = requests.get(search_url.format(catalog_url, bag))
    catalogitems = loads(resp.text)
    if catalogitems['count']:
        return catalogitems['results'][0]
    return {}


@app.task(bind=True)
def updatecatalog(self, bag, paramstring, collection, ingested=True):
    """
    Update Bag in Data Catalog with repository ingest status

    args:
      bag (string); Name of bag to update data catalog entry
      paramstring (string);  Parameter settings of derivative (e.x. "jpeg_040_antialias")
      collection (string); collection name with namespace (e.x. oku:hos)
      ingested (boolean); Indicates the bags ingest status - default is true
    """

    """
    Example derivative record structure:
    {"application": 
      {"islandora":
        {"derivative": "jpeg_040_antialias",
         "collection": "oku:hos",
         "ingested": True,
         "datetime": <timestamp of derivative>,
        }
      }
    }
    """
    catalogitem = searchcatalog(bag)
    if not catalogitem.get("bag"):
        return False  # this bag does not have a catalog entry
    if "application" not in catalogitem:
        catalogitem["application"] = {}
    if "islandora" not in catalogitem["application"]:
        catalogitem["application"]["islandora"] = {}
    catalogitem["application"]["islandora"]["derivative"] = paramstring
    catalogitem["application"]["islandora"]["collection"] = collection
    catalogitem["application"]["islandora"]["ingested"] = ingested
    catalogitem["application"]["islandora"]["datetime"] = datetime.datetime.utcnow().isoformat()
    
    headers = {"Content-Type": "application/json", "Authorization": "Token {0}".format(CYBERCOMMONS_TOKEN)}
    try:
        req = requests.post(catalog_url, data=dumps(catalogitem), headers=headers)
        req.raise_for_status()
    except Exception as e:  # TODO: use specific exceptions to catch
        self.retry(countdown=60, max_retries=4)
    return True


@app.task()
def ingest_recipe(recipes, collection='oku:hos', pid_namespace=None):
    """
    Ingest recipe json into Islandora repository.
    
    This kickstarts the Islandora local process to import a book collection.
    
    args:
      recipes: List of URLs pointing to JSON recipe objects or list of JSON recipe objects
      collection: Name of Islandora collection to ingest to. Default is: oku:hos 
      pid_namespace: Namespace to ingest recipe. Default is first half of collection name
    """
    logging.debug("ingest recipe args: {0}, {1}, {2}".format(recipes, collection, pid_namespace))
    logging.debug("Environment: {0}".format(environ))
    
    #ISLANDORA_DRUPAL_ROOT = environ.get("ISLANDORA_DRUPAL_ROOT")
    if not ISLANDORA_DRUPAL_ROOT:
        logging.error("Missing ISLANDORA_DRUPAL_ROOT")
        logging.error(environ)
        raise Exception("Drupal path config not set. Contact your administrator")

    if not pid_namespace:
        pid_namespace = collection.split(":")[0]

    logging.debug("Drupal root path: {0}".format(ISLANDORA_DRUPAL_ROOT))

    recipes = [recipes] if not isinstance(recipes, list) else recipes
    
    fail = [] 
    success = []
    for recipe in recipes:
        logging.debug("ingesting: {0}".format(recipe))
        if is_uri(recipe):
            recipe_uri = recipe
            testresp = requests.get(recipe_uri, allow_redirects=True)
            if testresp.status_code != requests.codes.ok:
                logging.error("Issue getting recipe at: {0}".format(recipe_uri))
                fail.append([recipe_uri, "Server status {0}".format(testresp.status_code)])
                continue
            if not is_recipe(testresp.json()):
                logging.error("Invalid recipe at: {0}".format(recipe_uri))
                fail.append([recipe_uri, "Invalid recipe: {0}".format(recipe_uri)])
                continue
            recipe = testresp.json()
        tmpdir = mkdtemp(prefix="recipeloader_")
        logging.debug("created working dir: {0}".format(tmpdir))
        chmod(tmpdir, 0o775)
        chown(tmpdir, -1, grp.getgrnam("apache").gr_gid)
        try:
            if not is_uri(recipe):
                if not is_recipe(recipe):
                    raise Exception("Not a valid recipe object")
                recipe_uri = join(tmpdir, "cc_recipe.json")
                with open(recipe_uri, "w") as f:
                    f.write(dumps(recipe))
            drush_response = None
            drush_response = check_output(
                ingest_template.format(recipe_uri.strip(), collection, pid_namespace, tmpdir, ISLANDORA_DRUPAL_ROOT),
                stderr=STDOUT,  # include stderr in output
                shell=True
            )
            logging.debug(drush_response)
            success.append(recipe)
        except CalledProcessError as err:
            fail.append([recipe, "Drush status {0}".format(err.returncode)])
            logging.error(drush_response)
            logging.error(err)
            logging.error(environ)
        except Exception as err:
            fail.append([recipe, err])
            logging.error(err)
            logging.error(recipe)
        finally:
            rmtree(tmpdir)
            logging.debug("removed working dir")
    return ({"Successful": success, "Failures": fail})


@app.task()
def verify_solr_up():
    """
    Check that the solr application is running returning True or False
    """
    try:
        return requests.get("http://localhost:8080/solr").ok
    except ConnectionError as err:
        logging.error("Error verifying solr is running")
        logging.error(err)
        return False


@app.task()
def object_exists(uuid, namespace, method="solr"):
    """
    Uses local drush script to check that object exists
    args:
      uuid: uuid/pid of object
      namespace: indicate which namespace to use
      method: indicate which system to use to check existance: solr (default) or drush
    """
    if method == "drush":
        return check_output(crud_template.format(namespace, uuid, 'read', ISLANDORA_DRUPAL_ROOT), shell=True) != ""
    elif method == "solr":
        resp = requests.get('http://localhost:8080/solr/select?q=PID:"{0}:{1}"&fl=numFound&wt=json'.format(namespace, uuid))
        data = loads(resp.text)
        if data['response']['numFound'] >= 1:
            return True
        return False


@app.task()
def ingest_status(recipe_url, namespace=None):
    """
    Polls the server to check that objects defined in the recipe_url exist on the server.
    
    args:
      recipe_url: URL string pointing to a json formatted recipe file
      namespace: String indicating which collection to use
    """
   
    # Get UUIDs from recipe file
    try:
        recipe_text = requests.get(recipe_url).text
    except requests.RequestException:
        raise Exception("Bad recipe url")
    recipe_data = loads(recipe_text)
    book_uuid = recipe_data['recipe']['uuid']
    page_uuids = [page['uuid'] for page in recipe_data['recipe']['pages']]

    if not object_exists(book_uuid, namespace):
        return {"book": book_uuid, "page_status": None, "successful_load": False, 
                "error": "Book not loaded. Book's UUID not found: {0}".format(book_uuid)}

    status = {}
    for uuid in page_uuids:
        status[uuid] = object_exists(uuid, namespace)

    successful_load = all([value for value in status.values()])
    
    return {"book": book_uuid, "page_status": status, "successful_load": successful_load}



@app.task()
def ingest_and_verify(recipe_url, collection='oku:hos', pid_namespace=None):
    """
    Ingest a recipe into Islandora and then verify if it was loaded succeccfully.
    
    args:
      recipe_url: URL string pointing to a json formatted recipe file
      collection: Name of Islandora collection to ingest to. Default is: oku:hos 
      pid_namespace: Namespace to ingest recipe. Default is first half of collection name
    """
    if not pid_namespace:
        pid_namespace = collection.split(":")[0]

    # recipe_url example: https://bag.ou.edu/derivative/[bag name]/[paramstring]/[lowercase version of bag name].json
    bag = recipe_url.split("/")[4]
    paramstring = recipe_url.split("/")[5]

    ingest = ingest_recipe.s(recipe_url, collection, pid_namespace)
    verify = ingest_status.si(recipe_url, namespace=pid_namespace)  # immutable signature to prevent result of ingest being appended
    update_catalog = updatecatalog.si(bag, paramstring, collection, ingested=True)  # immutable signature
    chain = (ingest | verify | update_catalog)
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
        logpath = environ.get('CELERY_LOG_FILE')
        with open(logpath, 'r') as f:
            loglast5 = f.readlines()[-5:]
        return {"Error": [drush_response, err.returncode, environ, loglast5]}
    return drush_response


@app.task()
def read_item(pid, namespace):
    """
    Read details of an object in Islandora
    
    args:
      pid - The unique identifier of the object (PID / UUID)
      namespace - The collection namespace the object exists in
    """
    return _item_manipulator(pid, namespace, 'read')


@app.task()
def delete_item(pid, namespace):
    """
    Delete an object from Islandora
    
    args:
      pid - The unique identifier of the object (PID / UUID)
      namespace - The collection namespace the object exists in
    """
    _item_manipulator(pid, namespace, 'delete')
    return True


@app.task()
def clear_drush_cache():
    check_call(["drush", "cache-clear", "drush"])
    return True


# added to asssist with testing connectivity
@app.task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result
