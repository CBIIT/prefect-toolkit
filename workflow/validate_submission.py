from src.commons.submval import SubmVal
from src.commons.datamodel import ReadDataModel, GetDataModel
from src.commons.utils import AwsUtils, get_date, get_time
from prefect import get_run_logger, flow, task
from typing import Literal
import os

DropDownChoices = Literal["ccdi", "icdc", "cds", "c3dc", "ctdc"]


@task(name="Validate File Format", log_prints=True)
def val_format(valid_object: SubmVal):
    validation_str, passed_filelist = valid_object.validate_format()
    return validation_str, passed_filelist


@task(name="Validate Required Properties", log_prints=True)
def val_required(valid_object: SubmVal, datamodel_obj: ReadDataModel, filepath_list: list[str]) -> str:
    validation_str = valid_object.validate_required_properties(data_model=datamodel_obj, filepath_list=filepath_list)
    return validation_str


@task(name="Validate Whitespace", log_prints=True)
def val_whitespace(valid_object: SubmVal, filepath_list: list[str]) -> str:
    validation_str = valid_object.validate_whitespace_issue(filepath_list=filepath_list)
    return validation_str


@task(name="Validate Numeric and Integer Properties", log_prints=True)
def val_numeric(valid_object: SubmVal, datamodel_obj: ReadDataModel, filepath_list: list[str]) -> str:
    validation_str = valid_object.validate_numeric_integer(data_model=datamodel_obj, filepath_list=filepath_list)
    return validation_str


@task(name="Validate Terms and Value Sets", log_prints=True)
def val_terms(
    valid_object: SubmVal, datamodel_obj: ReadDataModel, commons_acronym: str, filepath_list: list[str]
) -> str:
    validation_str = valid_object.validate_terms_value_sets(
        data_model=datamodel_obj, commons_acronym=commons_acronym, filepath_list=filepath_list
    )
    return validation_str


@task(name="Validate Cross Links", log_prints=True)
def val_crosslinks(valid_object: SubmVal, filepath_list: list[str]) -> str:
    validation_str = valid_object.validate_cross_links(filepath_list=filepath_list)
    return validation_str


@task(name="Validate Unique Key ID", log_prints=True)
def val_keyid(valid_object: SubmVal, datamodel_obj: ReadDataModel, filepath_list: list[str]) -> str:
    validation_str = valid_object.validate_unique_key_id(data_model=datamodel_obj, filepath_list=filepath_list)
    return validation_str


@task(name="Extract Model Files", log_prints=True)
def download_model_files(commons_acronym: str, tag: str) -> tuple:
    data_model_yaml, props_yaml = GetDataModel.dl_model_files(
        commons_acronym=commons_acronym, tag=tag
    )
    return data_model_yaml, props_yaml


@flow(name="Writing Validation Report", log_prints=True)
def write_report(
    valid_object: SubmVal,
    datamodel_object: ReadDataModel,
    submission_folder: str,
    output_name: str,
    commons_acronym: str,
    skip_uniq_key: bool,
    tag: str,
) -> None:
    """Prefect flow which writes validatioin report

    Args:
        valid_object (SubmVal): SubmVal object
        datamodel_object (ReadDataModel): ReadDataModel object
        submission_folder (str): Name of the tsv folder
        output_name (str): Validation report output name
        commons_acronym (str): Commons acronym
        tag (str): data model tag
    """
    # write header
    report_header = SubmVal.report_header(
        report_path=output_name,
        tsv_folder_path=submission_folder,
        model_file=datamodel_object.model_file,
        prop_file=datamodel_object.prop_file,
        tag=tag,
    )
    with open(output_name, "a+") as outf:
        outf.write(report_header)

    # validate format
    format_validation, passed_files = val_format(valid_object=valid_object)
    with open(output_name, "a+") as outf:
        outf.write(format_validation)
    print(f"Files that passed format validation: {*passed_files,}")
    print("Submission file format validation finished")

    # validate required property
    rq_prop_validation = val_required(
        valid_object=valid_object, datamodel_obj=datamodel_object, filepath_list=passed_files
    )
    with open(output_name, "a+") as outf:
        outf.write(rq_prop_validation)
    print("Required properties validation finished")

    # validate whitespace
    ws_validation = val_whitespace(valid_object=valid_object, filepath_list=passed_files
                                   )
    with open(output_name, "a+") as outf:
        outf.write(ws_validation)
    print("Whitespace validation finished")

    # validate terms and value sets
    terms_validation = val_terms(
        valid_object=valid_object,
        datamodel_obj=datamodel_object,
        commons_acronym=commons_acronym,
        filepath_list=passed_files
    )
    with open(output_name, "a+") as outf:
        outf.write(terms_validation)
    print("Terms and value sets validation finished")

    # validate numeric and integer properties
    numeric_validation = val_numeric(
        valid_object=valid_object, datamodel_obj=datamodel_object, filepath_list=passed_files
    )
    with open(output_name, "a+") as outf:
        outf.write(numeric_validation)
    print("Numeric and integer properties validation finished")

    # validate cross links
    cl_validation = val_crosslinks(valid_object=valid_object, filepath_list=passed_files)
    with open(output_name, "a+") as outf:
        outf.write(cl_validation)
    print("Crosslink validation finished")

    # validate key id
    if not skip_uniq_key:
        key_validation = val_keyid(
            valid_object=valid_object, datamodel_obj=datamodel_object, filepath_list=passed_files
        )
        with open(output_name, "a+") as outf:
            outf.write(key_validation)
        print("Unique key id validation finished")
    else:
        pass


@flow(name="Validate Submission Files", log_prints=True)
def validate_submission_tsv(
    submission_loc: str,
    commons_name: DropDownChoices,
    val_output_bucket: str,
    runner: str,
    tag: str = "",
    exclude_node_type: list[str] = [],
    skip_uniq_key_val: bool = False,
) -> None:
    """Prefect flow which validates a folder of submission tsv files against data model

    Args:
        submission_loc (str): Bucket location of submission files (tsv). Whitespace is NOT allowed, e.g., s3://bucket-name/folder-path
        commons_name (DropDownChoices): Commons acronym. Acceptable options are: ccdi, icdc, cds, c3dc, ctdc
        val_output_bucket (str): Bucket name of where validation output be uploaded to
        runner (str): Unique runner name without whitespace, e.g., john_smith
        tag (str, optional): Tag name of the data model. Defaults to "" to use master branch.
        exclude_node_type (list[str], optional): List of nodes to exclude. Defaults to [].
        skip_uniq_key_val (bool, optional): If skip unique key property validation. Defaults to False.
    """
    logger = get_run_logger()
    # download submission file folder
    submission_bucket, submission_path = AwsUtils.parse_object_uri(uri=submission_loc)
    submission_folder = AwsUtils.folder_dl(
        bucket=submission_bucket, remote_folder_path=submission_path
    )
    logger.info(
        f"Downloaded submission files from bucket {submission_bucket} folder {submission_path}"
    )

    # download data model files
    model_yaml, props_yaml = download_model_files(commons_acronym=commons_name, tag=tag)
    logger.info(f"Downloaded data files: {model_yaml}, {props_yaml}")

    # validation starts
    file_list = SubmVal.select_tsv_exclude_type(
        folder_path=submission_folder, exclude_type_list=exclude_node_type
    )
    logger.info(f"Submission tsv files found: {*file_list,}")
    valid_obj = SubmVal(filepath_list=file_list)
    model_obj = ReadDataModel(model_file=model_yaml, prop_file=props_yaml)

    output_name = (
        os.path.basename(submission_folder.strip("/"))
        + "_validation_report_"
        + get_date()
        + ".txt"
    )
    logger.info("Starting validation")
    # if tag not provide, main branch data model files were use, change tag value to main branch
    if tag == "":
        tag = "main branch"
    else:
        pass
    write_report(
        valid_object=valid_obj,
        datamodel_object=model_obj,
        submission_folder=submission_folder,
        output_name=output_name,
        commons_acronym=commons_name,
        skip_uniq_key=skip_uniq_key_val,
        tag=tag,
    )
    logger.info("Validation finished!")

    # upload output to AWS bucket
    output_folder = os.path.join(runner, "submission_validation_" + get_time())
    AwsUtils.file_ul(
        bucket=val_output_bucket, output_folder=output_folder, newfile=output_name
    )
    logger.info(
        f"Uploaded output {output_name} to bucket {val_output_bucket} folder path {output_folder}"
    )

    return None


@flow(name="Validate Data Model", log_prints=True)
def validate_data_model(
    commons_name: DropDownChoices, val_output_bucket: str, runner: str, tag: str = ""
) -> None:
    """Prefect flow that generates a table of data model props (tsv) which can be used for
    submssion file validation

    Args:
        commons_name (DropDownChoices): Commons acronym. Acceptable options are: ccdi, icdc, cds, c3dc, ctdc
        val_output_bucket (str): Bucket name of where the output be uploaded to
        runner (str): Unique runner name without whitespace, e.g., john_smith
        tag (str, optional): Tag name of the data model. Defaults to "" to use master branch.
    """
    logger = get_run_logger()
    # download data model files
    model_yaml, props_yaml = download_model_files(commons_acronym=commons_name, tag=tag)
    logger.info(f"Downloaded data files: {model_yaml}, {props_yaml}")

    # output folder name in bucket
    output_folder = os.path.join(runner, "data_model_validation_" + get_time())
    # create model object
    model_obj = ReadDataModel(model_file=model_yaml, prop_file=props_yaml)

    # write prop dict to file and upload to AWS
    if tag == "":
        props_dict_tag = "master"
    else:
        props_dict_tag = tag
    prop_dict_df = model_obj.props_df
    prop_dict_filename = f"{commons_name}_model-{props_dict_tag}_props_table.tsv"
    prop_dict_df.to_csv(prop_dict_filename, sep="\t", index=False)
    AwsUtils.file_ul(
        bucket=val_output_bucket,
        output_folder=output_folder,
        newfile=prop_dict_filename,
    )
    logger.info(
        f"Uploaded {prop_dict_filename} to bucket {val_output_bucket} folder path {output_folder}"
    )
    return None
