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


def test_find_ptc_not_in_dbgap_error():
    """test for find_ptc_not_in_db with error"""
    db_ptc_list = ["ptc_1", "ptc_2", "ptc_3"]
    dbgap_ptc_dict = {"ptc_1": 0, "ptc_2": 1}
    message = find_ptc_not_in_dbGaP(
        db_ptc_list=db_ptc_list, dbgap_ptc_dict=dbgap_ptc_dict
    )
    assert "ERROR: Found 1 participant(s)" in message


def test_find_ptc_not_in_dbgap_pass():
    """test for find_ptc_not_in_db with pass"""
    db_ptc_list = ["ptc_1", "ptc_2"]
    dbgap_ptc_dict = {"ptc_1": 0, "ptc_2": 1}
    message = find_ptc_not_in_dbGaP(
        db_ptc_list=db_ptc_list, dbgap_ptc_dict=dbgap_ptc_dict
    )
    assert "INFO: All participants in DB found in dbGaP" in message


def test_find_ptc_not_in_db_warning():
    """test for find_ptc_not_in_db with found"""
    db_ptc_list = ["ptc_1", "ptc_2"]
    dbgap_ptc_dict = {"ptc_1": 0, "ptc_2": 1, "ptc_3": 1, "ptc_4": 1}
    message = find_ptc_not_in_db(db_ptc_list=db_ptc_list, dbgap_ptc_dict=dbgap_ptc_dict)
    assert (
        "WARNING: Found 2 participant(s) in dbGaP (consent non-0) but not in DB"
        in message
    )


def test_find_ptc_not_in_db_pass():
    """test for find_ptc_not_in_db passed"""
    db_ptc_list = ["ptc_1", "ptc_2", "ptc_3"]
    dbgap_ptc_dict = {"ptc_1": 0, "ptc_2": 1}
    message = find_ptc_not_in_db(db_ptc_list=db_ptc_list, dbgap_ptc_dict=dbgap_ptc_dict)
    assert "INFO: ALL participants in dbGaP (consent non-0) were found in DB" in message


def test_find_db_ptc_consent_zero_found():
    """test for find_db_ptc_consent_zero found"""
    db_ptc_list = ["ptc_1", "ptc_2", "ptc_3"]
    dbgap_ptc_dict = {"ptc_1": 0, "ptc_2": 1}
    message = find_db_ptc_consent_zero(
        db_ptc_list=db_ptc_list, dbgap_ptc_dict=dbgap_ptc_dict
    )
    assert "ERROR: Found 1 participant(s) in DB" in message


def test_find_db_ptc_consent_zero_unfound():
    """test for find_db_ptc_consent_zero unfound"""
    db_ptc_list = ["ptc_1", "ptc_2", "ptc_3"]
    dbgap_ptc_dict = {"ptc_1": 1, "ptc_2": 1}
    message = find_db_ptc_consent_zero(
        db_ptc_list=db_ptc_list, dbgap_ptc_dict=dbgap_ptc_dict
    )
    assert "INFO: All participants in DB have consent code non-0" in message


def test_find_db_ptc_consent_zero_nonoverlap():
    """test for find_db_ptc_consent_zero no overlap found"""
    db_ptc_list = ["ptc_1", "ptc_2", "ptc_3"]
    dbgap_ptc_dict = {"ptc_4": 1, "ptc_5": 1}
    message = find_db_ptc_consent_zero(
        db_ptc_list=db_ptc_list, dbgap_ptc_dict=dbgap_ptc_dict
    )
    assert "ERROR: No overlap of participants found" in message


def test_find_sample_not_in_dbgap_found():
    """test for find_sample_not_in_dbgap found"""
    db_sample_dict = {"sample_1": "ptc_1", "sample_2": "ptc_2", "sample_3": "ptc_3"}
    dbgap_sample_dict = {"sample_2": "ptc_2"}
    dbgap_ptc_dict = {"ptc_1": 1, "ptc_2": 1}
    message = find_sample_not_in_dbgap(
        db_sample_dict=db_sample_dict,
        dbgap_sample_dict=dbgap_sample_dict,
        dbgap_ptc_dict=dbgap_ptc_dict,
    )
    assert "WARNING: 1 Sample(s)" in message
    assert "ERROR: 1 Sample(s)" in message

def test_find_sample_not_in_dbgap_unfound():
    """test for find_sample_not_in_dbgap unfound"""
    db_sample_dict = {"sample_1": "ptc_1", "sample_2": "ptc_2", "sample_3": "ptc_3"}
    dbgap_sample_dict = {
        "sample_1": "ptc_1",
        "sample_2": "ptc_2",
        "sample_3": "ptc_3",
        "sample_4": "ptc_4",
    }
    dbgap_ptc_dict = {
        "ptc_1": 1,
        "ptc_2": 2,
        "ptc_3": 3,
        "ptc_4": 4,
    }
    message = find_sample_not_in_dbgap(
        db_sample_dict=db_sample_dict,
        dbgap_sample_dict=dbgap_sample_dict,
        dbgap_ptc_dict=dbgap_ptc_dict,
    )
    assert "INFO: Samples in DB passed validation" in message

def test_find_sample_not_in_db_found():
    """test for find_sample_not_in_db found"""
    db_sample_dict = {"sample_2": "ptc_2", "sample_3": "ptc_3"}
    dbgap_sample_dict = {
        "sample_1": "ptc_1",
        "sample_2": "ptc_2",
        "sample_3": "ptc_3",
        "sample_4": "ptc_4",
    }
    message = find_sample_not_in_db(db_sample_dict=db_sample_dict, dbgap_sample_dict = dbgap_sample_dict)
    assert "WARNING: 2 Sample(s) " in message

def test_find_sample_not_in_db_unfound():
    """test for find_sample_not_in_db unfound"""
    db_sample_dict = {"sample_2": "ptc_2", "sample_3": "ptc_3"}
    dbgap_sample_dict = {
        "sample_2": "ptc_2",
        "sample_3": "ptc_3",
    }
    message = find_sample_not_in_db(
        db_sample_dict=db_sample_dict, dbgap_sample_dict=dbgap_sample_dict
    )
    assert "INFO: Samples" in message

def test_sample_ptc_check_found():
    """test for sample_ptc_check found"""
    db_sample_dict = {"sample_1": "ptc_1", "sample_2": "ptc_2", "sample_3": "ptc_3"}
    dbgap_sample_dict = {"sample_1": "ptc_1", "sample_3": "ptc_4"}
    message = sample_ptc_check(db_sample_dict=db_sample_dict, dbgap_sample_dict=dbgap_sample_dict)
    assert "ERROR: 1 Sample(s)" in message

def test_sample_ptc_check_unfound():
    """test for sample_ptc_check unfound"""
    db_sample_dict = {"sample_1": "ptc_1", "sample_2": "ptc_2", "sample_3": "ptc_3"}
    dbgap_sample_dict = {"sample_1": "ptc_1", "sample_3": "ptc_3"}
    message = sample_ptc_check(
        db_sample_dict=db_sample_dict, dbgap_sample_dict=dbgap_sample_dict
    )
    assert "INFO: Samples' participant ids match between DB and dbGaP" in message

def test_sample_ptc_check_nonoverlap():
    """test for sample_ptc_check nonoverlap"""
    db_sample_dict = {"sample_1": "ptc_1", "sample_2": "ptc_2", "sample_3": "ptc_3"}
    dbgap_sample_dict = {"sample_4": "ptc_1", "sample_5": "ptc_3"}
    message = sample_ptc_check(
        db_sample_dict=db_sample_dict, dbgap_sample_dict=dbgap_sample_dict
    )
    assert "ERROR: No overlap" in message
