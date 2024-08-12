from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from src.commons.utils import get_time
from src.crdcdh.metadata_validation import DataHubMongoDB
from src.commons.dbgap_sstr import SstrHaul
from typing import TypeVar
import pandas as pd
import json


DataFrame = TypeVar("DataFrame")


@task
def dbgap_validation_md(
    submission_id: str,
    study_accession: str,
    study_version: str,
    participant_count: int,
    sample_count: int,
    validationstr: str,
) -> None:
    """Creates an artifact of metadata validation flow

    Args:
        study_accession (str): study dbGaP accession str
        study_version (str): study version str
        participant_count (int): counts of participants
        sample_count (int): counts of samples
        validationstr (str): validation report str
    """
    if int(study_version) == 0:
        study_version = "Not Found [WARNING: Validation was performed using LATEST version found dbGaP API]"
    else:
        pass
    markdown_report = f"""# CRDCDH Metadata Validation Report - {get_time()}
## Submission Information

- **Submission ID** 
    - {submission_id}

- **dbGaP accession in DB**
    - {study_accession}

- **dbGaP version in DB**
    - {study_version}

- **Participant count in DB**
    - {participant_count}

- **Sample count in DB**
    - {sample_count}

## Validation Report

{validationstr}

"""
    create_markdown_artifact(
        key="crdcdh-metada-dbgap-validation",
        markdown=markdown_report,
        description="CRDCDH metadata validation against dbGaP",
    )


def metadata_validation_str(
    db_participant_list: list,
    db_sample_dict: dict,
    dbgap_participant_dict: dict,
    dbgap_sample_dict: dict,
) -> str:
    """Returns a Dataframe of metadata validation

    Args:
        db_participant_list (list): A list of participant id in crdc datahub MongoDB
        db_sample_list (list): A list of sample id in crdc datahub MongoDB
        dbgap_participant_dict (dict): A dict of participants with participant id as key and consent code as value
        dbgap_sample_dict (dict): A dict of samples with sample id as key and participant id as value

    Returns:
        str: A string of validation summary
    """
    summary_str = ""

    # participants in DB but not in dbGaP
    p_in_db_not_in_dbGaP = [
        i for i in db_participant_list if i not in dbgap_participant_dict.keys()
    ]
    if len(p_in_db_not_in_dbGaP) > 0:
        p_in_db_not_in_dbGaP_str = pd.DataFrame(
            p_in_db_not_in_dbGaP, columns=["Participant ID"]
        ).to_markdown(tablefmt="pipe", index=False)
        error_message = f"ERROR: Found {len(p_in_db_not_in_dbGaP)} participant(s) in DB but not in dbGaP.\n{p_in_db_not_in_dbGaP_str}\n\n"
        summary_str += error_message
    else:
        # participants in DB are all found in dbGaP
        summary_str += f"INFO: All participants in DB found in dbGaP"

    # participants found in DB and dbGaP, but consent code is 0
    p_in_db_in_dbGaP = [
        i for i in db_participant_list if i in dbgap_participant_dict.keys()
    ]
    if len(p_in_db_in_dbGaP) > 0:
        p_consent_zero = [i for i in p_in_db_in_dbGaP if dbgap_participant_dict[i] == 0]
        if len(p_consent_zero) > 0:
            p_consent_zero_str = pd.DataFrame(
                p_consent_zero, columns=["Participant ID"]
            ).to_markdown(tablefmt="pipe", index=False)
            error_message = f"ERROR: Found {len(p_consent_zero)} participant(s) in DB with consent code of 0 in dbGaP.\n{p_consent_zero_str}\n\n"
            summary_str += error_message
        else:
            # participants in DB and dbGaP, and no consent code 0 is found
            summary_str += "INFO: All participants in DB have consent code non-0\n\n"
    else:
        summary_str += "WARNING: No overlap of participants found between DB and dbGaP\n\n"

    # participants found in dbGaP but not in DB
    p_in_dbGaP_not_in_db = [
        i for i in dbgap_participant_dict.keys() if i not in db_participant_list
    ]
    # make sure these participants are not consent 0
    p_in_dbGaP_not_in_db = [
        i for i in p_in_dbGaP_not_in_db if dbgap_participant_dict[i] != 0
    ]
    if len(p_in_dbGaP_not_in_db) > 0:
        p_in_dbGaP_not_in_db_str = pd.DataFrame(
            p_in_dbGaP_not_in_db, columns=["Participant ID"]
        ).to_markdown(tablefmt="pipe", index=False)
        warn_message = f"WARNING: Found {len(p_in_dbGaP_not_in_db)} participant(s) in dbGaP (consent non-0) but in DB.\n{p_in_dbGaP_not_in_db_str}\n\n"
        summary_str += warn_message
    else:
        # all participants in dbGaP found in DB
        summary_str += (
            "INFO: ALL participants in dbGaP (consent non-0) were found in DB\n\n"
        )

    # samples in DB but not in dbGaP
    s_in_db_not_in_dbGaP = [
        i for i in db_sample_dict.keys() if i not in dbgap_sample_dict.keys()
    ]
    if len(s_in_db_not_in_dbGaP) > 0:
        s_in_db_not_in_dbGaP_w_participant = {
            k: db_sample_dict[k] for k in s_in_db_not_in_dbGaP
        }
        # identify if the parent of these samples are found in dbGaP
        s_parent_in_dbgap = []
        s_parent_not_in_dbgap = []
        for key in s_in_db_not_in_dbGaP_w_participant.keys():
            key_parent = s_in_db_not_in_dbGaP_w_participant[key]
            if key_parent in dbgap_participant_dict.keys():
                s_parent_in_dbgap.append({"Sample": key, "Participant": key_parent})
            else:
                s_parent_not_in_dbgap.append({"Sample": key, "Participant": key_parent})

        if len(s_parent_in_dbgap) > 0:
            s_parent_in_dbgap_df_str = pd.DataFrame.from_records(
                s_parent_in_dbgap
            ).to_markdown(tablefmt="pipe", index=False)
            warn_message = f"WARNING: {len(s_parent_in_dbgap)} Samples found in DB but not in dbGaP. However, they belong to participants registered in dbGaP.\n{s_parent_in_dbgap_df_str}\n\n"
            summary_str += warn_message
        else:
            pass

        if len(s_parent_not_in_dbgap) > 0:
            s_parent_not_in_dbgap_df_str = pd.DataFrame.from_records(
                s_parent_not_in_dbgap
            ).to_markdown(tablefmt="pipe", index=False)
            error_message = f"ERROR: {len(s_parent_not_in_dbgap)} Samples found in DB but not in dbGaP. They belong to participants NOT registered in dbGaP.\n{s_parent_not_in_dbgap_df_str}\n\n"
            summary_str += error_message
        else:
            pass
    else:
        # all sample ids in DB found in dbGaP
        summary_str += "INFO: Samples in DB passed validation\n\n"

    # sample in dbGaP not in DB
    s_in_dbGaP_not_in_db = [
        i for i in dbgap_sample_dict.keys() if i not in db_sample_dict.keys()
    ]
    if len(s_in_dbGaP_not_in_db) > 0:
        s_in_dbGaP_not_in_db_dict = [
            {"Sample": k, "Participant": dbgap_sample_dict[k]}
            for k in s_in_dbGaP_not_in_db
        ]
        s_in_dbGaP_not_in_db_dict_str = pd.DataFrame.from_records(
            s_in_dbGaP_not_in_db_dict
        ).to_markdown(tablefmt="pipe", index=False)
        warn_message = f"WARNING: {len(s_in_dbGaP_not_in_db_dict)} Samples found in dbGaP but not found in DB.\n{s_in_dbGaP_not_in_db_dict_str}\n\n"
        summary_str += warn_message
    else:
        # all sample id in dbGaP are found
        summary_str += "INFO: Samples in dbGaP were all found in DB\n\n"

    # sample in both DB and dbGaP, but their parents/participant id don't match
    s_in_dbgap_in_db = [
        i for i in db_sample_dict.keys() if i in dbgap_sample_dict.keys()
    ]
    if len(s_in_dbgap_in_db) > 0:
        # there are sample id overlap between two sources
        parent_mismatch_list = []
        for s in s_in_dbgap_in_db:
            s_db_parent = db_sample_dict[s]
            s_dbgap_parent = dbgap_sample_dict[s]
            if s_db_parent != s_dbgap_parent:
                s_dict = {
                    "Sample": s,
                    "dbGaP_subject_id": s_dbgap_parent,
                    "DB_subject_id": s_db_parent,
                }
                parent_mismatch_list.append(s_dict)
            else:
                pass
        if len(parent_mismatch_list) > 0:
            parent_mismatch_list_df_str = pd.DataFrame.from_records(
                parent_mismatch_list
            ).to_markdown(tablefmt="pipe", index=False)
            error_message = f"ERROR: Samples found associated with different participant ids between DB and dbGaP\n{parent_mismatch_list_df_str}\n\n"
            summary_str += error_message
        else:
            # all samples found in both dbgap and db share the identical subject id
            summary_str += f"INFO: Samples' participant ids match between DB and dbGaP\n\n"
    else:
        summary_str += "WARNING: No overlap of samples found between DB and dbGaP\n\n"
    return summary_str


@flow(
    name="Data Hub metadata validation against dbGaP",
    log_prints=True,
    flow_run_name=f"datahub-metadata-validation-{get_time()}",
)
def validation_against_dbgap(submission_id: str) -> None:
    logger = get_run_logger()

    # create a datahub mongodb
    db_object = DataHubMongoDB()

    # get DB participant
    submission_participants = db_object.get_study_participants(
        submission_id=submission_id
    )
    submission_samples = db_object.get_study_samples(submission_id=submission_id)
    logger.info(
        f"Participants found in submission {submission_id}: {len(submission_participants)}"
    )
    logger.info(
        f"Samples found in submission {submission_id}: {len(submission_samples.keys())}"
    )

    # get dbgap accession and version in DB
    study_accession = db_object.get_dbgap_id(submission_id=submission_id)
    study_version = db_object.get_study_version(submission_id=submission_id)
    logger.info(f"Submission {submission_id} dbGaP accession: {study_accession}")
    logger.info(f"Submission {submission_id} dbGaP version: {study_version}")
    if study_version == None:
        study_version = "0"
    else:
        pass
    # get dbgap participants
    sstrhaul = SstrHaul(phs_accession=study_accession, version_str=study_version)
    study_particpant_dict = sstrhaul.get_study_participants()
    study_sample_dict = sstrhaul.get_study_samples()
    logger.info(
        f"Participants found for study {study_accession} in dbGaP: {len(study_particpant_dict.keys())}"
    )
    logger.info(
        f"Samples found for study {study_accession} in dbGaP: {len(study_sample_dict.keys())}"
    )

    # validation
    validation_str = metadata_validation_str(
        db_participant_list=submission_participants,
        db_sample_dict=submission_samples,
        dbgap_participant_dict=study_particpant_dict,
        dbgap_sample_dict=study_sample_dict,
    )
    # create summary artifact
    dbgap_validation_md(
        submission_id=submission_id,
        study_accession=study_accession,
        study_version=study_version,
        participant_count=len(submission_participants),
        sample_count=len(submission_samples.keys()),
        validationstr=validation_str,
    )
    return None


@flow(name="dbgap validation", log_prints=True)
def dbgap_validation_test() -> None:

    dh_mongo = DataHubMongoDB()

    # connection_str = dh_mongo._mongo_connection_str()

    # db_name = dh_mongo._mongo_db_name()

    dbgap_id = dh_mongo.get_dbgap_id(
        submission_id="eaee9cf0-5d42-43f6-8e1b-8ef3df072884"
    )
    print(
        f"dbGaP accessioin for submission eaee9cf0-5d42-43f6-8e1b-8ef3df072884: {dbgap_id}"
    )  # should expect phs002529

    version_number = dh_mongo.get_study_version(
        submission_id="2a23e8ed-af03-4d8e-9ef7-ebd3af79611f"
    )
    print(
        f"version for submission 2a23e8ed-af03-4d8e-9ef7-ebd3af79611f: {version_number}"
    )

    study_particpants = dh_mongo.get_study_participants(
        submission_id="eaee9cf0-5d42-43f6-8e1b-8ef3df072884"
    )
    print(
        f"study participants for submission eaee9cf0-5d42-43f6-8e1b-8ef3df072884: {*study_particpants,}"
    )

    study_samples = dh_mongo.get_study_samples(
        submission_id="eaee9cf0-5d42-43f6-8e1b-8ef3df072884"
    )
    sample_list = [key + ":" + study_samples[key] for key in study_samples.keys()]
    print(
        f"study samples for submission eaee9cf0-5d42-43f6-8e1b-8ef3df072884: {*sample_list,}"
    )

    return None
