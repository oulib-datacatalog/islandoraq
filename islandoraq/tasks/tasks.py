from celery.task import task
from subprocess import check_call, CalledProcessError
from shutil import rmtree
from tempfile import mkdtemp
import logging

logging.basicConfig(level=logging.INFO)


@task()
def ingest_recipe(recipe_url, collection='islandora:bookCollection'):
    """
    Ingest recipe json file into Islandora repository.
    
    This kickstarts the Islandora local process to import a book collection.
    
    args:
      recipe_url: URL pointing to json formatted recipe file
      collection: Name of Islandora collection to ingest to. Default is: islandora:bookCollection  
    """
    tmpdir = mkdtemp(prefix="recipeloader_")
    try:
        check_call(['drush',
                    '-u', '1',
                    'oubib',
                    '--recipe_uri={0}'.format(recipe_url),
                    '--parent_collection={0}'.format(collection),
                    '--tmp_dir={0}'.format(tmpdir),
                    '--root=/srv/repository/drupal/'])
    except CalledProcessError as err:
        logging.error(err)
        return({"ERROR": err})
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
