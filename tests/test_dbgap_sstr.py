import sys
import os
import pytest
import mock
from unittest.mock import MagicMock

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.commons.dbgap_sstr import SstrHaul


def test_SstrHaul_fail_wrong_accesion():
    """test for SstrHaul init fail due to accession"""
    with pytest.raises(ValueError):
        SstrHaul(phs_accession="failaccession", version_str="1")

def test_SstrHaul_fail_wrong_version():
    """test for SstrHaul init fail due to version str"""
    with  pytest.raises(ValueError):
        SstrHaul(phs_accession="phs000123", version_str="failversion")

@mock.patch("src.commons.dbgap_sstr.requests.get", autospec=True)
def test_SstrHaul_fail_wrong_version_second(mock_requests):
    """test for SstrHaul init fail due to version str"""
    magic_response = MagicMock()
    mock_requests.return_value = magic_response
    magic_response.json.return_value = {"study":{"accver":{"version": 2}}}
    with pytest.raises(ValueError):
        SstrHaul(phs_accession="phs123456", version_str="3")

@mock.patch("src.commons.dbgap_sstr.requests.get", autospec=True)
def test_SstrHaul_get_participant_cnt(mock_requests):
    """test for SstrHaul get_participant_cnt"""
    magic_response = MagicMock()
    mock_requests.return_value = magic_response
    magic_response.json.side_effect = [
        {"study": {"accver": {"version": 2}}},
        {"pagination":{"total": 123}},
    ]
    ptc_count =  SstrHaul(phs_accession="phs000123", version_str="1").get_participant_cnt()
    assert ptc_count == 123

@mock.patch("src.commons.dbgap_sstr.requests.get", autospec=True)
def test_SstrHaul_get_study_participants(mock_requests):
    """test for SstrHaul get_study_participants"""
    magic_response = MagicMock()
    mock_requests.return_value = magic_response
    magic_response.json.side_effect = [
        {"study": {"accver": {"version": 2}}},
        {"pagination": {"total": 30}},  # only requires two extra calls
        {
            "subjects": [
                {"submitted_subject_id": "ptc_1", "consent_code": 1},
                {"submitted_subject_id": "ptc_2", "consent_code": 1},
            ]
        },
        {
            "subjects": [
                {"submitted_subject_id": "ptc_3", "consent_code": 1},
                {"submitted_subject_id": "ptc_4", "consent_code": 1},
                {"submitted_subject_id": "ptc_5", "consent_code": 1},
            ]
        },

    ]
    ptc_dict = SstrHaul(
        phs_accession="phs000123", version_str="1"
    ).get_study_participants()
    assert len(ptc_dict.keys())==5


@mock.patch("src.commons.dbgap_sstr.requests.get", autospec=True)
def test_SstrHaul_get_study_samples(mock_requests):
    """test for SstrHaul get_study_participants"""
    magic_response = MagicMock()
    mock_requests.return_value = magic_response
    magic_response.json.side_effect = [
        {"study": {"accver": {"version": 2}}},
        {"pagination": {"total": 30}},  # only requires two extra calls
        {
            "subjects": [
                {
                    "submitted_subject_id": "ptc_1",
                    "consent_code": 1,
                    "samples": [
                        {"submitted_sample_id": "sample1"},
                        {"submitted_sample_id": "sample2"},
                    ],
                },
                {
                    "submitted_subject_id": "ptc_2",
                    "consent_code": 1,
                    "samples": [
                        {"submitted_sample_id": "sample3"},
                        {"submitted_sample_id": "sample4"},
                    ],
                },
            ]
        },
        {
            "subjects": [
                {
                    "submitted_subject_id": "ptc_3",
                    "consent_code": 1,
                    "samples": [
                        {"submitted_sample_id": "sample5"},
                    ],
                },
            ]
        },
    ]
    sample_dict = SstrHaul(
        phs_accession="phs000123", version_str="1"
    ).get_study_samples()
    assert len(sample_dict.keys()) == 5
    assert "sample5" in sample_dict.keys()
    assert sample_dict["sample4"] == "ptc_2"
