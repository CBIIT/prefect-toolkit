import yaml
from pathlib import Path
from typing import Dict
import numpy as np
import pandas as pd
import json
from src.commons.constants import CommonsRepo
from dataclasses import fields
import requests
import tempfile
from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO
import os
from shutil import copy


class GetDataModel(CommonsRepo):
    def __init__(self):
        return None

    @classmethod
    def _get_repo_dict(cls, commons_acronym: str) -> dict:
        """Returns a dictionary of commons repo

        Args:
            commons_acronym (str): An acronym of commons, e.g., ccdi

        Raises:
            ValueError: If acronym is found in CommonsRepo class

        Returns:
            dict: A dictionary of repo url, model file paths
        """
        commons_acronym = commons_acronym.lower()
        available_commons = [field.name for field in fields(cls)]
        if commons_acronym not in available_commons:
            raise ValueError(f"Unknown commons acronym {commons_acronym} provided")
        else:
            pass
        return_dict = cls.__dataclass_fields__[commons_acronym].default_factory()
        return return_dict

    @classmethod
    def get_tags_complete(cls, commons_acronym: str) -> dict:
        """Get tags informtion from github repo using github api

        Args:
            commons_acronym (str): An acronym of commons, e.g., ccdi 

        Returns:
            dict: A dictionary of tags of a repo
        """
        repo_dict = cls._get_repo_dict(commons_acronym=commons_acronym)
        api_link = repo_dict["tags_api"]
        api_re = requests.get(api_link)
        re_list = api_re.json()
        return re_list

    @classmethod
    def get_tags_only(cls, commons_acronym: str) -> list:
        """Get tags name only of a commons data model repo

        Args:
            commons_acronym (str): An acronym of commons, e.g., ccdi

        Returns:
            list: A list of tag names
        """
        tags_list_complete = cls.get_tags_complete(commons_acronym=commons_acronym)
        tags_name = [i["name"] for i in tags_list_complete]
        return tags_name

    @classmethod
    def get_latest_tag(cls, commons_acronym: str) -> str:
        """Get the latest tag available in repo

        Args:
            commons_acronym (str): An acronym of commons, e.g., ccdi

        Returns:
            str: Name of a tag
        """
        tags_list_complete = cls.get_tags_complete(commons_acronym=commons_acronym)
        latest_tag = tags_list_complete[0]["name"]
        return latest_tag

    @classmethod
    def dl_model_files(cls, commons_acronym: str, tag: str) -> tuple:
        """Downloads data model files from a commons data model repo

        Args:
            commons_acronym (str): An acronym of commons, e.g., ccdi

        Returns:
            tuple: data model file names
        """
        tags_name = cls.get_tags_only(commons_acronym=commons_acronym)
        if tag not in tags_name:
            raise ValueError(f"Tag {tag} not found in repo. Available tags in repo: {*tags_name,}")
        else:
            pass
        tags_list_complete =  cls.get_tags_complete(commons_acronym=commons_acronym)
        tag_item = [i for i in tags_list_complete if i["name"] == tag][0]
        model_file_relpath = cls._get_repo_dict(commons_acronym=commons_acronym)["model_yaml"]
        props_file_relpath = cls._get_repo_dict(commons_acronym=commons_acronym)[
            "props_yaml"
        ]
        tag_zipurl = tag_item["zipball_url"]
        http_response = urlopen(tag_zipurl)
        zipfile = ZipFile(BytesIO(http_response.read()))
        # create a temp dir to download the zipfile
        tempdirobj = tempfile.TemporaryDirectory(suffix="_github_dl")
        tempdir = tempdirobj.name
        zipfile.extractall(path=tempdir)
        try:
            copy(
                os.path.join(
                    tempdir, os.listdir(tempdir)[0], model_file_relpath
                ),
                os.path.basename(model_file_relpath),
            )
            copy(
                os.path.join(tempdir, os.listdir(tempdir)[0], props_file_relpath),
                os.path.basename(props_file_relpath),
            )
        except FileNotFoundError as err:
            raise FileNotFoundError(f"File not found: {repr(err)}")
        except Exception as e:
            raise FileNotFoundError(f"Error occurred downloading data model files: {repr(e)}")
        return (os.path.basename(model_file_relpath), os.path.basename(props_file_relpath))


class ReadDataModel:
    """A class reads data model files
    """    
    def __init__(self, model_file: str, prop_file: str) -> None:
        self.model_file = model_file
        self.prop_file = prop_file

    def _read_model(self) -> dict:
        """reads model.yml file into dict"""
        model_dict = yaml.safe_load(Path(self.model_file).read_text())
        return model_dict

    def _read_prop(self) -> dict:
        """reads prop.yml file into dict"""
        prop_dict = yaml.safe_load(Path(self.prop_file).read_text())
        return prop_dict

    def get_model_nodes(self) -> Dict:
        """Returns a dict of node and properties of the node"""
        model_dict = self._read_model()
        nodes_dict = model_dict["Nodes"]
        return nodes_dict

    def _read_each_prop(self, prop_dict: dict) -> tuple:
        """Extracts multiple prop information from a prop blob in a
        single prop dictionary

        Example prop_dict:
        {
            "Desc": "The text for reporting information about ethnicity based on the Office of Management and Budget (OMB) categories.",
            "Term": [
                {
                    "Origin": "caDSR",
                    "Code": "2192217",
                    "Value": "Ethnic Group Category Text",
                }
            ],
            "Type": {
                "value_type": "list",
                "Enum": [
                    "Hispanic or Latino",
                    "Not Allowed to Collect",
                    "Not Hispanic or Latino",
                    "Not Reported",
                    "Unknown",
                ],
            },
            "Req": True,
            "Strict": False,
            "Private": False,
        }
        Special comments for ICDC:
        - Req has three values, "Yes", "No", and "Preferred"
        - "Type" is not a required key in prop_dict, if "Enum" is found
        """
        if "Desc" in prop_dict.keys():
            prop_description = prop_dict["Desc"]
        else:
            prop_description = np.nan

        if "Term" in prop_dict.keys():
            term_list = prop_dict["Term"]
            cde_term = [i for i in term_list if i["Origin"] == "caDSR"]
            if len(cde_term) >= 1:
                prop_CDE = cde_term[0]["Code"]
            else:
                prop_CDE = np.nan
        else:
            prop_CDE = np.nan

        if "Req" in prop_dict.keys():
            prop_required = prop_dict["Req"]
        else:
            prop_required = "No"

        if "Key" in prop_dict.keys():
            prop_key = prop_dict["Key"]
        else:
            prop_key = np.nan

        # if "Type" found a key
        if "Type" in prop_dict.keys():

            if isinstance(prop_dict["Type"], str):
                # this covers string, integar, number in CCDI
                # this covers string, datetime, (certain)number, integer in ICDC
                prop_type = prop_dict["Type"]
            # if prop_dict["Type"] is a list
            elif isinstance(prop_dict["Type"], list):
                # this covers some unfixed icdc properties such as medication, type is a list
                prop_type = "string"
            # if prop_dict["Type"] is a dict
            else:
                if "value_type" not in prop_dict["Type"].keys():
                    # there is only few cases in icdc, such as document_number
                    # This is probably a poorly described property that needs to be fixed later
                    prop_type = "string"
                elif (
                    prop_dict["Type"]["value_type"] == "string"
                    and "Enum" in prop_dict["Type"].keys()
                    and prop_dict["Strict"] == False
                ):
                    # this covers string;enum
                    prop_type = "string;enum"

                elif (
                    prop_dict["Type"]["value_type"] == "string"
                    and "Enum" in prop_dict["Type"].keys()
                    and prop_dict["Strict"] == True
                ):
                    # this covers enum
                    prop_type = "enum"
                elif (
                    prop_dict["Type"]["value_type"] == "list"
                    and "Type" in prop_dict["Type"].keys()
                ):
                    # this covers array[string]
                    prop_type = "array[string]"
                elif (
                    prop_dict["Type"]["value_type"] == "list"
                    and "Enum" in prop_dict["Type"].keys()
                    and "Strict" not in prop_dict.keys()
                ):
                    # this coveres array[enum] in ICDC, only one occurence
                    prop_type = "array[enum]"
                elif (
                    prop_dict["Type"]["value_type"] == "list"
                    and "Enum" in prop_dict["Type"].keys()
                    and prop_dict["Strict"] == True
                ):
                    # this covers array[enum]
                    prop_type = "array[enum]"
                elif (
                    prop_dict["Type"]["value_type"] == "list"
                    and "Enum" in prop_dict["Type"].keys()
                    and prop_dict["Strict"] == False
                ):
                    # this covers array[string;enum]
                    prop_type = "array[string;enum]"

                elif isinstance(prop_dict["Type"]["value_type"], str):
                    # in ICDC this covers number/integer properties that comes with unit
                    prop_type = prop_dict["Type"]["value_type"]
                else:
                    print(json.dumps(prop_dict, indent=4))
                    raise TypeError(
                        "Can not categorize property type. Need to modify GetCCDIModel._read_each_prop method"
                    )
            # populate prop_example in CCDI model
            if isinstance(prop_dict["Type"], dict):
                if "Enum" in prop_dict["Type"].keys():
                    prop_enum_list = prop_dict["Type"]["Enum"]
                else:
                    # prop_example = np.nan
                    prop_enum_list = []

            else:
                # prop_example = np.nan
                prop_enum_list = []

        elif "Enum" in prop_dict.keys():
            # this covers enum in ICDC
            prop_type = "enum"
            prop_enum_list = prop_dict["Enum"]
        return (
            prop_description,
            prop_type,
            prop_enum_list,
            prop_required,
            prop_CDE,
            prop_key,
        )

    def get_prop_dict_df(self):
        """Returns a dataframe"""
        # model_dict contains information of what properties in each node
        model_dict = self.get_model_nodes()
        # prop_dict contains property description, type, type, example value, and if the property is required
        prop_dict = self._read_prop()["PropDefinitions"]

        # create a dictionary to be convereted to df later
        prop_return_df = pd.DataFrame(
            columns=[
                "Property",
                "Description",
                "Node",
                "Type",
                "Example value",
                "Required",
                "Key",
                "CDE",
            ]
        )

        for node in model_dict.keys():
            # print(f"node: {node}")
            node_property_list = model_dict[node]["Props"]
            if node_property_list is None:
                # this is for nodes that have no props under
                pass
            else:
                for property in node_property_list:
                    # print(property)
                    (
                        prop_description,
                        prop_type,
                        prop_example,
                        prop_required,
                        prop_cde,
                        prop_key,
                    ) = self._read_each_prop(prop_dict=prop_dict[property])

                    """
                    if property == node + "_id":
                        prop_key = True
                    elif property == "id":
                        prop_key = False
                    else:
                        prop_key = np.nan
                    """

                    prop_append_line = {
                        "Property": [property],
                        "Description": [prop_description],
                        "Node": [node],
                        "Type": [prop_type],
                        "Example value": [prop_example],
                        "Required": [prop_required],
                        "Key": [prop_key],
                        "CDE": [prop_cde],
                    }
                    prop_return_df = pd.concat(
                        [prop_return_df, pd.DataFrame(prop_append_line)],
                        ignore_index=True,
                    )
        return prop_return_df
