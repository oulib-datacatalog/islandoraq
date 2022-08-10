import sys
from os.path import exists

from six import PY2

if PY2:
    from os.path import join
    from mock import MagicMock, Mock, patch
else:
    from unittest.mock import MagicMock, Mock, patch

import pytest

from islandoraq.tasks.tasks import verify_solr_up, searchcatalog, updatecatalog, ingest_status, ingest_recipe
from requests.exceptions import HTTPError


mock_celeryconfig = patch("islandoraq.tasks.tasks.celeryconfig").start()
mock_islandora_drupal_root = patch("islandoraq.tasks.tasks.ISLANDORA_DRUPAL_ROOT", return_value="/tmp").start()
mock_islandora_fqdn = patch("islandoraq.tasks.tasks.ISLANDORA_FQDN", return_value="test.somesite").start()
mock_path = patch("islandoraq.tasks.tasks.PATH", return_value="/tmp").start()
mock_cybercommons_token = patch("islandoraq.tasks.tasks.CYBERCOMMONS_TOKEN", return_value="test").start()


@patch('islandoraq.tasks.tasks.requests.get')
def test_searchcatalog_not_found(mock_get):
    not_found = """
    {
        "count": 0, 
        "meta": {
            "page": 1, 
            "page_size": 10, 
            "pages": 0
        }, 
        "next": null, 
        "previous": null, 
        "results": []
    }
    """
    mock_get.return_value = Mock(ok=True)
    mock_get.return_value.text = not_found
    response = searchcatalog("test_bag")
    assert response == None


@patch('islandoraq.tasks.tasks.requests.get')
def test_searchcatalog_found(mock_get):
    found = """
    {
        "count": 1,
        "meta": {
            "page": 1,
            "page_size": 10,
            "pages": 1
        },
        "next": null,
        "previous": null,
        "results": [
        {
            "_id": "testid",
            "application": {
                "islandora": {
                    "datetime": "",
                    "derivative": "jpeg_040_antialias",
                    "ingested": false
                }
            },
            "bag": "Tyler_2019",
            "department": "",
            "derivatives": {
                "jpeg_040_antialias": {
                    "datetime": "",
                    "pages": ["https://bag.ou.edu/derivative/Tyler_2019/jpeg_040_antialias/data/001.jpg"],
                    "recipe": "https://bag.ou.edu/derivative/Tyler_2019/jpeg_040_antialias/tyler_2019.json"
                }
            },
            "locations": {
                "nas": {
                    "error": "",
                    "exists": false,
                    "location": "",
                    "place_holder": true
                },
                "norfile": {
                    "exists": true,
                    "location": "",
                    "valid": true,
                    "validation_date": ""
                },
                "s3": {
                    "bucket": "",
                    "error": [],
                    "exists": true,
                    "manifest": "manifest-md5.txt",
                    "valid": true,
                    "validation_date": "",
                    "verified": ["source/Tyler_2019/data/001.tif"]
                }
            },
            "project": "fake_bag"
        }
        ]
    }
    """
    mock_get.return_value = Mock(ok=True)
    mock_get.return_value.text = found
    response = searchcatalog("Tyler_2019")
    assert response['bag'] == "Tyler_2019"
    assert response['project'] == 'fake_bag'


@patch('islandoraq.tasks.tasks.requests.post')
@patch('islandoraq.tasks.tasks.searchcatalog')
def test_updatecatalog_success(mock_search, mock_post):
    mock_search.return_value = {}
    mock_post.return_value = Mock(ok=True)
    response = updatecatalog(bag="Tyler_2019", paramstring="jpeg_040_antialias", collection="oku:hos")
    assert response == True


@patch('islandoraq.tasks.tasks.requests.post')
@patch('islandoraq.tasks.tasks.searchcatalog')
def test_updatecatalog_fail_not_in_catalog(mock_search, mock_post):
    mock_search.return_value = None
    mock_post.return_value = Mock(ok=True)
    response = updatecatalog(bag="Tyler_2019", paramstring="jpeg_040_antialias", collection="oku:hos")
    assert response == False


@pytest.mark.skip(reason="This is not raising the HTTPError side effect")
@patch('islandoraq.tasks.tasks.requests.post')
@patch('islandoraq.tasks.tasks.searchcatalog')
def test_updatecatalog_fail_server_500(mock_search, mock_post):
    mock_search.return_value = {}
    mock_post.return_value.status_code = 500
    mock_post.raise_for_status = Mock(side_effect=HTTPError("500 Server Error"))
    response = updatecatalog(bag="Tyler_2019", paramstring="jpeg_040_antialias", collection="oku:hos")
    assert response == False


@patch('islandoraq.tasks.tasks.grp.getgrnam')
@patch('islandoraq.tasks.tasks.chown')
@patch('islandoraq.tasks.tasks.mkdtemp')
@patch('islandoraq.tasks.tasks.rmtree')
@patch('islandoraq.tasks.tasks.check_output')
def test_ingest_recipe_with_recipe_object(mock_check_output, mock_rmtree, mock_mkdtemp, mock_chown, mock_getgrnam, tmp_path):
    recipe = {
        "recipe": {
            "uuid": "test", 
            "update": "true", 
            "label": "Test Collection", 
            "import": "collection", 
            "metadata": {
                "mods": "test_collection_mods.xml"
            },
            "members": [
                {
                    "files": {
                        "tn": "recording-tn.jpg", 
                        "obj": "https://test.somesite.test/nonexistent_path/test.mp3"
                    }, 
                    "uuid": "test", 
                    "update": "true", 
                    "label": "test item label", 
                    "import": "audio", 
                    "metadata": {
                        "mods": "test_item_mods.xml"
                    }
                }
            ]
        }
    }
    mock_mkdtemp.return_value = str(tmp_path)
    ingest_recipe(recipe)
    
    if PY2:
        recipe_file = join(str(tmp_path), "cc_recipe.json")
    else:
        recipe_file = tmp_path / "cc_recipe.json"
    
    assert exists(recipe_file)


@patch('islandoraq.tasks.tasks.grp.getgrnam')
@patch('islandoraq.tasks.tasks.chown')
@patch('islandoraq.tasks.tasks.mkdtemp')
@patch('islandoraq.tasks.tasks.rmtree')
@patch('islandoraq.tasks.tasks.check_output')
@patch('islandoraq.tasks.tasks.requests.get')
def test_ingest_recipe_with_url(mock_requests_get, mock_check_output, mock_rmtree, mock_mkdtemp, mock_chown, mock_getgrnam, tmp_path):
    recipe = "https://test.somesite.com/nonexistent_path/test.json"
    
    mock_requests_get.return_value = Mock(status_code=200, content={"recipe": {"uuid": "test"}})

    mock_mkdtemp.return_value = str(tmp_path)
    ingest_recipe(recipe)
    
    if PY2:
        recipe_file = join(str(tmp_path), "cc_recipe.json")
    else:
        recipe_file = tmp_path / "cc_recipe.json"
    
    assert exists(recipe_file)


@patch('islandoraq.tasks.tasks.grp.getgrnam')
@patch('islandoraq.tasks.tasks.chown')
@patch('islandoraq.tasks.tasks.mkdtemp')
@patch('islandoraq.tasks.tasks.rmtree')
@patch('islandoraq.tasks.tasks.check_output')
@patch('islandoraq.tasks.tasks.requests.get')
def test_ingest_recipe_with_url_404(mock_requests_get, mock_check_output, mock_rmtree, mock_mkdtemp, mock_chown, mock_getgrnam, tmp_path):
    recipe = "https://test.somesite.com/nonexistent_path/test.json"
    
    mock_requests_get.return_value = Mock(status_code=404, content={"recipe": {"uuid": "test"}})

    mock_mkdtemp.return_value = str(tmp_path)
    results = ingest_recipe(recipe)
    
    if PY2:
        recipe_file = join(str(tmp_path), "cc_recipe.json")
    else:
        recipe_file = tmp_path / "cc_recipe.json"
    
    assert results == {'Failures': [[recipe, 'Server status 404']], 'Successful': []}


@patch('islandoraq.tasks.tasks.requests.get')
def test_verify_solr_up(mock_get):
    mock_get.return_value.ok=True
    response = verify_solr_up()
    assert response == True

    mock_get.return_value.ok=False
    response = verify_solr_up()
    assert response == False


@patch('islandoraq.tasks.tasks.requests.get')
def test_verify_solr_down(mock_get):
    mock_get.return_value.ok = False
    response = verify_solr_up()
    assert response == False


@pytest.mark.skip(reason="not implemented")
def test_ingest_status():
    ingest_status()
    raise Exception("Testing...")