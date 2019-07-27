import sys
from nose.tools import assert_true, assert_false, assert_equal, nottest
try:
    from unittest.mock import MagicMock, Mock, patch
except ImportError:
    from mock import MagicMock, Mock, patch

from islandoraq.tasks.tasks import verify_solr_up, searchcatalog, updatecatalog
from requests.exceptions import HTTPError


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
    assert_false(response)


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
    assert_equal(response['bag'], "Tyler_2019")
    assert_equal(response['project'], 'fake_bag')


@patch('islandoraq.tasks.tasks.requests.post')
@patch('islandoraq.tasks.tasks.searchcatalog')
def test_updatecatalog_success(mock_search, mock_post):
    mock_search.return_value = {}
    mock_post.return_value = Mock(ok=True)
    response = updatecatalog(bag="Tyler_2019", paramstring="jpeg_040_antialias", collection="oku:hos")
    assert_true(response)


@patch('islandoraq.tasks.tasks.requests.post')
@patch('islandoraq.tasks.tasks.searchcatalog')
def test_updatecatalog_fail_not_in_catalog(mock_search, mock_post):
    mock_search.return_value = None
    mock_post.return_value = Mock(ok=True)
    response = updatecatalog(bag="Tyler_2019", paramstring="jpeg_040_antialias", collection="oku:hos")
    assert_false(response)


@nottest  #FIXME: This is not raising the HTTPError side effect
@patch('islandoraq.tasks.tasks.requests.post')
@patch('islandoraq.tasks.tasks.searchcatalog')
def test_updatecatalog_fail_server_500(mock_search, mock_post):
    mock_search.return_value = {}
    mock_post.return_value.status_code = 500
    mock_post.raise_for_status = Mock(side_effect=HTTPError("500 Server Error"))
    response = updatecatalog(bag="Tyler_2019", paramstring="jpeg_040_antialias", collection="oku:hos")
    assert_false(response)


@nottest  # TODO: complete test
@patch('islandoraq.tasks.tasks.requests.head')
def test_ingest_recipe(mock_head):
    mock_head.return_value.status_code = 200


@patch('islandoraq.tasks.tasks.requests.get')
def test_verify_solr_up(mock_get):
    mock_get.return_value.ok=True
    response = verify_solr_up()
    assert_true(response)


@patch('islandoraq.tasks.tasks.requests.get')
def test_verify_solr_down(mock_get):
    mock_get.return_value.ok = False
    response = verify_solr_up()
    assert_false(response)