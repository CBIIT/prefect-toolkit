from src.commons.submval import SubmVal
from src.commons.datamodel import ReadDataModel, GetDataModel
from src.commons.utils import AwsUtils, get_date, get_time
from prefect import get_run_logger, flow, task
import os


@task(name="Validate Required Properties", log_prints=True)
def val_required(valid_object: SubmVal, datamodel_obj: ReadDataModel) -> str:
    validation_str =  valid_object.validate_required_properties(data_model=datamodel_obj)
    return validation_str


@task(name="Validate Whitespace", log_prints=True)
def val_whitespace(valid_object: SubmVal) -> str:
    validation_str = valid_object.validate_whitespace_issue()
    return validation_str


@task(name="Validate Numeric and Integer Properties", log_prints=True)
def val_numeric(valid_object: SubmVal, datamodel_obj: ReadDataModel) -> str:
    validation_str = valid_object.validate_numeric_integer(data_model=datamodel_obj)
    return validation_str


@task(name="Validate Terms and Value Sets", log_prints=True)
def val_terms(valid_object: SubmVal, datamodel_obj: ReadDataModel) -> str:
    validation_str = valid_object.validate_terms_value_sets(data_model=datamodel_obj)
    return validation_str


@task(name="Validate Cross Links", log_prints=True)
def val_crosslinks(valid_object: SubmVal) -> str:
    validation_str = valid_object.validate_cross_links()
    return validation_str


@task(name="Validate Unique Key ID", log_prints=True)
def val_keyid(valid_object: SubmVal, datamodel_obj: ReadDataModel) -> str:
    validation_str = valid_object.validate_unique_key_id(data_model=datamodel_obj)
    return validation_str


@task(name="Extract Model Files", log_prints=True)
def download_model_files(commons_acronym: str, tag: str) -> tuple:
    data_model_yaml, props_yaml = GetDataModel.dl_model_files(commons_acronym=commons_acronym, tag=tag)
    return data_model_yaml, props_yaml

@flow(name="Writing Validation Report", log_prints=True)
def write_report(valid_object: SubmVal, datamodel_object: ReadDataModel, submission_folder: str, output_name: str) -> None:
    """Prefect flow which writes validatioin report

    Args:
        valid_object (SubmVal): SubmVal object
        datamodel_object (ReadDataModel): ReadDataModel object
        submission_folder (str): Name of the tsv folder
        output_name (str): Validation report output name
    """    
    # write header
    report_header = SubmVal.report_header(
        report_path=output_name,
        tsv_folder_path=submission_folder,
        model_file=datamodel_object.model_file,
        prop_file=datamodel_object.prop_file
    )
    with open(output_name, "a+") as outf:
        outf.write(report_header)

    # validate required property
    rq_prop_validation = val_required(valid_object=valid_object, datamodel_obj=datamodel_object)
    with open(output_name, "a+") as outf:
        outf.write(rq_prop_validation)
    print("Required properties validation finished")

    # validate whitespace
    ws_validation = val_whitespace(valid_object=valid_object)
    with open(output_name, "a+") as outf:
        outf.write(ws_validation)
    print("Whitespace validation finished")

    # validate terms and value sets
    terms_validation = val_terms(valid_object=valid_object, datamodel_obj=datamodel_object)
    with open(output_name, "a+") as outf:
        outf.write(terms_validation)
    print("Terms and value sets validation finished")

    # validate numeric and integer properties
    numeric_validation = val_numeric(valid_object=valid_object, datamodel_obj=datamodel_object)
    with open(output_name, "a+") as outf:
        outf.write(numeric_validation)
    print("Numeric and integer properties validation finished")

    # validate cross links
    cl_validation = val_crosslinks(valid_object=valid_object)
    with open(output_name, "a+") as outf:
        outf.write(cl_validation)
    print("Crosslink validation finished")

    # validate key id
    key_validation = val_keyid(valid_object=valid_object, datamodel_obj=datamodel_object)
    with open(output_name, "a+") as outf:
        outf.write(key_validation)
    print("Unique key id validation finished")


@flow(name="Validate Submission Files", log_prints=True)
def validate_submission_tsv(submission_loc: str, commons_name: str,  val_output_bucket: str, runner: str, tag: str = "",exclude_node_type: list[str] = []) -> None:  
    """Prefect flow which validates a folder of submission tsv files against data model

    Args:
        submission_loc (str): Bucket location of submission files (tsv), e.g., s3://bucket-name/folder-path
        commons_name (str): Commons acronym. Acceptable options are: ccdi, icdc
        val_output_bucket (str): Bucket name of where validation output be uploaded to
        runner (str): Unique runner name without whitespace, e.g., john_smith
        tag (str, optional): Tag name of the data model. Defaults to "" to use master branch. 
        exclude_node_type (list[str], optional): List of nodes to exclude. Defaults to [].
    """
    logger = get_run_logger()
    # download submission file folder
    submission_bucket, submission_path = AwsUtils.parse_object_uri(uri=submission_loc)
    submission_folder = AwsUtils.folder_dl(bucket=submission_bucket, remote_folder_path=submission_path)
    logger.info(f"Downloaded submission files from bucket {submission_bucket} folder {submission_path}")

    # download data model files
    model_yaml, props_yaml = download_model_files(commons_acronym=commons_name, tag=tag)
    logger.info(f"Downloaded data files: {model_yaml}, {props_yaml}")

    # validation starts
    file_list = SubmVal.select_tsv_exclude_type(
        folder_path=submission_folder, exclude_type_list=exclude_node_type
    )
    valid_obj = SubmVal(filepath_list=file_list)
    model_obj = ReadDataModel(model_file = model_yaml, prop_file=props_yaml)

    output_name = os.path.basename(submission_folder.strip("/")) + "_validation_report_" + get_date() + ".txt"
    logger.info("Starting validation")
    write_report(valid_object=valid_obj, datamodel_object=model_obj, submission_folder=submission_folder, output_name=output_name)
    logger.info("Validation finished!")

    # upload output to AWS bucket
    output_folder = os.path.join(runner, "submission_validation_" + get_time())
    AwsUtils.file_ul(bucket=val_output_bucket, output_folder=output_folder, newfile=output_name)
    logger.info(f"Uploaded output {output_name} to bucket {val_output_bucket} folder path {output_folder}")

    # write prop dict to file and upload to db
    prop_dict_df = model_obj.get_prop_dict_df()
    prop_dict_df.to_csv("props_dict_table.tsv", sep="\t", index=False)
    AwsUtils.file_ul(
        bucket=val_output_bucket,
        output_folder=output_folder,
        newfile="props_dict_table.tsv",
    )
    logger.info(
        f"Uploaded props_dict_table.tsv to bucket {val_output_bucket} folder path {output_folder}"
    )

    return None
