import sys
import os
import pytest
import mock
from unittest.mock import MagicMock

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.crdcdh.dh_mongodb import DataHubMongoDB


@pytest.fixture
def mongodb_obj():
    return DataHubMongoDB()


@mock.patch("src.crdcdh.dh_mongodb.get_secret", autospec=True)
def test_mongo_connection_str(mock_get_secret, mongodb_obj):
    mock_get_secret.return_value = {
        "mongo_db_user": "test_user",
        "mongo_db_password": "test_password",
        "mongo_db_host": "test_host",
        "mongo_db_port": "test_port",
    }
    connection_str = mongodb_obj._mongo_connection_str()
    assert (
        connection_str
        == "mongodb://test_user:test_password@test_host:test_port/?authMechanism=DEFAULT&authSource=admin"
    )


@mock.patch("src.crdcdh.dh_mongodb.get_secret", autospec=True)
def test_mongo_db_name(mock_get_secret, mongodb_obj):
    mock_get_secret.return_value = {
        "database_name": "test_database",
    }
    db_name = mongodb_obj._mongo_db_name()
    assert db_name == "test_database"


def test_find_study_version_delimiter(mongodb_obj):
    test_str_one = "phs000123.v2.p1|phs000123.v1.p1"
    test_str_two = "phs000123.v2.p1;phs000123.v1.p1"
    delimiter_one = mongodb_obj._find_study_version_delimiter(
        study_version_str=test_str_one
    )
    delimiter_two = mongodb_obj._find_study_version_delimiter(
        study_version_str=test_str_two
    )
    assert "|" == delimiter_one
    assert ";" == delimiter_two


def test_find_latest_version(mongodb_obj):
    test_str_one = "phs000123.v2.p1|phs000123.v1.p1"
    test_str_two = "phs000123.v1.p1"
    latest_version_one = mongodb_obj._find_latest_version(
        study_version_str=test_str_one
    )
    latest_version_two = mongodb_obj._find_latest_version(
        study_version_str=test_str_two
    )
    assert latest_version_one == "2"
    assert latest_version_two == "1"


@mock.patch("src.crdcdh.dh_mongodb.get_secret", autospec=True)
@mock.patch("src.crdcdh.dh_mongodb.MongoClient", autospec=True)
def test_get_dbgap_id(mock_client, mock_get_secret, mongodb_obj):
    mock_get_secret.side_effect = [
        {
            "mongo_db_user": "test_user",
            "mongo_db_password": "test_password",
            "mongo_db_host": "test_host",
            "mongo_db_port": "test_port",
        },
        {
            "database_name": "test_database",
        },
    ]
    magic_mock = MagicMock()
    mock_client().__getitem__.return_value.__getitem__.return_value = magic_mock
    magic_mock.find.return_value = [{"dbGaPID": "phs000123.v3.p2"}]
    # client_object.find.side_effect = [{"dbGaPID":"phs000123.v3.p2"}]
    test_dbgap_id = mongodb_obj.get_dbgap_id(submission_id="test_submission")
    assert test_dbgap_id == "phs000123"


@mock.patch("src.crdcdh.dh_mongodb.get_secret", autospec=True)
@mock.patch("src.crdcdh.dh_mongodb.MongoClient", autospec=True)
def test_get_study_version(mock_client, mock_get_secret, mongodb_obj):
    mock_get_secret.side_effect = [
        {
            "mongo_db_user": "test_user",
            "mongo_db_password": "test_password",
            "mongo_db_host": "test_host",
            "mongo_db_port": "test_port",
        },
        {
            "database_name": "test_database",
        },
    ]
    magic_mock = MagicMock()
    mock_client().__getitem__.return_value.__getitem__.return_value = magic_mock
    magic_mock.find.return_value = [
        {"props": {"study_version": "phs000123.v3.p2|phs000123.v2.p2"}}
    ]
    magic_mock.count_documents.return_value = 1
    study_versions = mongodb_obj.get_study_version(submission_id="test_submission")
    assert study_versions == "3"


@mock.patch("src.crdcdh.dh_mongodb.get_secret", autospec=True)
@mock.patch("src.crdcdh.dh_mongodb.MongoClient", autospec=True)
def test_get_study_participants(mock_client, mock_get_secret, mongodb_obj):
    mock_get_secret.side_effect = [
        {
            "mongo_db_user": "test_user",
            "mongo_db_password": "test_password",
            "mongo_db_host": "test_host",
            "mongo_db_port": "test_port",
        },
        {
            "database_name": "test_database",
        },
    ]
    magic_mock = MagicMock()
    mock_client().__getitem__.return_value.__getitem__.return_value = magic_mock
    magic_mock.find.return_value = [
        {"props": {"participant_id": "ptc_1"}},
        {"props": {"participant_id": "ptc_2"}},
        {"props": {"participant_id": "ptc_3"}},
    ]
    magic_mock.count_documents.return_value = 3
    study_participants = mongodb_obj.get_study_participants(
        submission_id="test_submission"
    )
    assert len(study_participants) == 3
    assert "ptc_2" in study_participants


@mock.patch("src.crdcdh.dh_mongodb.get_secret", autospec=True)
@mock.patch("src.crdcdh.dh_mongodb.MongoClient", autospec=True)
def test_get_study_participants(mock_client, mock_get_secret, mongodb_obj):
    mock_get_secret.side_effect = [
        {
            "mongo_db_user": "test_user",
            "mongo_db_password": "test_password",
            "mongo_db_host": "test_host",
            "mongo_db_port": "test_port",
        },
        {
            "database_name": "test_database",
        },
    ]
    magic_mock = MagicMock()
    mock_client().__getitem__.return_value.__getitem__.return_value = magic_mock
    magic_mock.find.side_effect = [
        [
            {
                "props": {"sample_id": "sample_1"},
                "parents": [{"parentIDValue": "ptc_1_db"}],
            },
            {
                "props": {"sample_id": "sample_2"},
                "parents": [{"parentIDValue": "ptc_2_db"}],
            },
        ],
        [{"props": {"participant_id": "ptc_1"}}],
        [{"props": {"participant_id": "ptc_2"}}],
    ]
    magic_mock.count_documents.return_value = 2
    sample_dict = mongodb_obj.get_study_samples(submission_id="test_submission")
    assert "sample_2" in sample_dict.keys()
    assert sample_dict["sample_1"] == "ptc_1"
