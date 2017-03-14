from celery.task import task
from dockertask import docker_task
from subprocess import check_call, CalledProcessError
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
    try:
        check_call(['drush',
                    '-u', '1',
                    'oubib',
                    '--recipe_uri={0}'.format(recipe_url),
                    '--parent_collection={0}'.format(collection),
                    '--tmp_dir=/tmp',
                    '--root=/srv/repository/drupal/'])
    except CalledProcessError as err:
        logging.error(err)
        return({"ERROR": err})

    return("SUCCESS")  # TODO: return islandora url for ingested book

