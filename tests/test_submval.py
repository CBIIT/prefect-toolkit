import sys
import os
import pytest
import mock
from pathlib import Path
from unittest.mock import MagicMock

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.commons.submval import SubmVal
from src.commons.datamodel import ReadDataModel


test_file_list = [os.path.join("tests/test_files", i) for i in os.listdir("tests/test_files") if "node" in i]
model_files =  [os.path.join("tests/test_files", i) for i in os.listdir("tests/test_files") if "yml" in i]

@pytest.fixture
def my_datamodel():
    model_file_yml =  [i for i in model_files if i.endswith("model.yml")][0]
    model_prop_yml = [i for i in model_files if i.endswith("props.yml")][0]
    return ReadDataModel(model_file=model_file_yml, prop_file=model_prop_yml)

@pytest.fixture
def my_submval():
    return SubmVal(filepath_list=test_file_list)

def test_section_head():
    """test of static method section_header"""
    test_head = SubmVal.section_header(section_name="test_head")
    assert "#   test_head   #" in test_head

def test_commons_delimiter():
    """test of classmethod commons_delimiter"""
    test_delimiter = SubmVal.commons_delimiter(commons_acronym="icdc")
    assert test_delimiter == ";"

def test_report_header():
    """test of stattic method report_header"""
    test_report_header =  SubmVal.report_header(report_path="test_file.txt", tsv_folder_path="test_folder", model_file="model.yaml", prop_file="model-props.yaml", tag="test_tag")
    assert "Model tag:\n        - test_tag" in test_report_header
    assert (
        "Model property YAML file: \n        - model-props.yaml" in test_report_header
    )

def test_validate_required_properties(my_submval, my_datamodel):
    """test for required property validation"""
    validate_str = my_submval.validate_required_properties(data_model=my_datamodel)
    assert validate_str.count("ERROR") == 2
    assert "5,18,19" in validate_str
    assert "9,10" in validate_str

def test_validate_whitespace_check(my_submval):
    """test for whitespace check validation"""
    validate_str = my_submval.validate_whitespace_issue()
    assert validate_str.count("ERROR") == 1
    assert "12,18,23" in validate_str


def test_terms_and_value_sets_check(my_submval, my_datamodel):
    """test for terms and value sets"""
    validate_str = my_submval.validate_terms_value_sets(data_model=my_datamodel, commons_acronym="ccdi")
    assert "free strings allowed" in validate_str
    assert "unrecognized value" in validate_str
    assert "[test wrong term]" in validate_str
    assert "[worng term test]" in validate_str


def test_validate_numeric_integer(my_submval, my_datamodel):
    """test for integer and numeric properties"""
    validate_str =  my_submval.validate_numeric_integer(data_model=my_datamodel)
    assert validate_str.count("ERROR") == 1
    assert "5" in validate_str

def test_validate_cross_links(my_submval):
    """test for cross links validation"""
    validate_str = my_submval.validate_cross_links()
    print(validate_str)
    assert "test wrong participant" in validate_str
    assert validate_str.count("ERROR") == 3
