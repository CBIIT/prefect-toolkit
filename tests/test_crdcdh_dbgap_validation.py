import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from workflow.crdcdh.dbgap_validation import (
    find_ptc_not_in_db,
    find_ptc_not_in_dbGaP,
    find_db_ptc_consent_zero,
    find_sample_not_in_db,
    find_sample_not_in_dbgap,
    sample_ptc_check,
)
import pytest


@pytest.mark.parametrize(
    "db_ptc_list, dbgap_ptc_dict, expected",
    [
        (
            ["ptc_1", "ptc_2", "ptc_3"],
            {"ptc_1": 0, "ptc_2": 1},
            "ERROR: Found 1 participant(s)",
        ),
        (
            ["ptc_1", "ptc_2"],
            {"ptc_1": 0, "ptc_2": 1},
            "INFO: All participants in DB found in dbGaP",
        ),
    ],
)
def test_find_ptc_not_in_dbGaP(db_ptc_list, dbgap_ptc_dict, expected):
    """test for find_ptc_not_in_dbgap"""
    test_message = find_ptc_not_in_dbGaP(db_ptc_list=db_ptc_list, dbgap_ptc_dict=dbgap_ptc_dict)
    assert expected in test_message


@pytest.mark.parametrize(
    "db_ptc_list, dbgap_ptc_dict, expected",
    [
        (
            ["ptc_1", "ptc_2"],
            {"ptc_1": 0, "ptc_2": 1, "ptc_3": 1, "ptc_4": 1},
            "WARNING: Found 2 participant(s) in dbGaP (consent non-0) but not in DB",
        ),
        (
            ["ptc_1", "ptc_2", "ptc_3"],
            {"ptc_1": 0, "ptc_2": 1},
            "INFO: ALL participants in dbGaP (consent non-0) were found in DB",
        ),
    ],
)
def test_find_ptc_not_in_db(db_ptc_list, dbgap_ptc_dict, expected):
    """test for ptc_not_in_db"""
    test_message = find_ptc_not_in_db(db_ptc_list=db_ptc_list, dbgap_ptc_dict=dbgap_ptc_dict)
    assert expected in test_message

@pytest.mark.parametrize(
    "db_ptc_list, dbgap_ptc_dict, expected",
    [
        (
            ["ptc_1", "ptc_2", "ptc_3"],
            {"ptc_1": 0, "ptc_2": 1},
            "ERROR: Found 1 participant(s) in DB",
        ),
        (
            ["ptc_1", "ptc_2", "ptc_3"],
            {"ptc_1": 1, "ptc_2": 1},
            "INFO: All participants in DB have consent code non-0",
        ),
        (
            ["ptc_1", "ptc_2", "ptc_3"],
            {"ptc_4": 1, "ptc_5": 1},
            "ERROR: No overlap of participants found",
        ),
    ],
)
def test_find_db_ptc_consent_zero(db_ptc_list, dbgap_ptc_dict, expected):
    """test for find_db_ptc_consent_zero"""
    test_message = find_db_ptc_consent_zero(db_ptc_list=db_ptc_list, dbgap_ptc_dict=dbgap_ptc_dict)
    assert expected in test_message


@pytest.mark.parametrize(
    "db_sample_dict,dbgap_sample_dict,dbgap_ptc_dict,expected",
    [
        (
            {"sample_1": "ptc_1", "sample_2": "ptc_2", "sample_3": "ptc_3"},
            {"sample_2": "ptc_2"},
            {"ptc_1": 1, "ptc_2": 1},
            "WARNING: 1 Sample(s)",
        ),
        (
            {"sample_1": "ptc_1", "sample_2": "ptc_2", "sample_3": "ptc_3"},
            {"sample_2": "ptc_2"},
            {"ptc_1": 1, "ptc_2": 1},
            "ERROR: 1 Sample(s)",
        ),
        (
            {"sample_1": "ptc_1", "sample_2": "ptc_2", "sample_3": "ptc_3"},
            {
                "sample_1": "ptc_1",
                "sample_2": "ptc_2",
                "sample_3": "ptc_3",
                "sample_4": "ptc_4",
            },
            {
                "ptc_1": 1,
                "ptc_2": 2,
                "ptc_3": 3,
                "ptc_4": 4,
            },
            "INFO: Samples in DB passed validation",
        ),
    ],
)
def test_find_sample_not_in_dbgap(
    db_sample_dict, dbgap_sample_dict, dbgap_ptc_dict, expected
):
    """test for find_sample_not_in_dbgap"""
    test_message = find_sample_not_in_dbgap(
        db_sample_dict=db_sample_dict,
        dbgap_sample_dict=dbgap_sample_dict,
        dbgap_ptc_dict=dbgap_ptc_dict,
    )
    assert expected in test_message


@pytest.mark.parametrize(
    "db_sample_dict, dbgap_sample_dict, expected",
    [
        (
            {"sample_2": "ptc_2", "sample_3": "ptc_3"},
            {
                "sample_1": "ptc_1",
                "sample_2": "ptc_2",
                "sample_3": "ptc_3",
                "sample_4": "ptc_4",
            },
            "WARNING: 2 Sample(s) ",
        ),
        (
            {"sample_2": "ptc_2", "sample_3": "ptc_3"},
            {
                "sample_2": "ptc_2",
                "sample_3": "ptc_3",
            },
            "INFO: Samples",
        ),
    ],
)
def test_find_sample_not_in_db(db_sample_dict, dbgap_sample_dict, expected):
    """test for find_sample_not_in_db"""
    test_message = find_sample_not_in_db(
        db_sample_dict=db_sample_dict, dbgap_sample_dict=dbgap_sample_dict
    )
    assert expected in test_message


@pytest.mark.parametrize(
    "db_sample_dict, dbgap_sample_dict,expected",
    [
        (
            {"sample_1": "ptc_1", "sample_2": "ptc_2", "sample_3": "ptc_3"},
            {"sample_1": "ptc_1", "sample_3": "ptc_4"},
            "ERROR: 1 Sample(s)",
        ),
        (
            {"sample_1": "ptc_1", "sample_2": "ptc_2", "sample_3": "ptc_3"},
            {"sample_1": "ptc_1", "sample_3": "ptc_3"},
            "INFO: Samples' participant ids match between DB and dbGaP",
        ),
        (
            {"sample_1": "ptc_1", "sample_2": "ptc_2", "sample_3": "ptc_3"},
            {"sample_4": "ptc_1", "sample_5": "ptc_3"},
            "ERROR: No overlap",
        ),
    ],
)
def test_sample_ptc_check(db_sample_dict, dbgap_sample_dict, expected):
    """test for sample_ptc_check"""
    test_message = sample_ptc_check(
        db_sample_dict=db_sample_dict, dbgap_sample_dict=dbgap_sample_dict
    )
    assert expected in test_message
