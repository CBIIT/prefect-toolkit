import numpy as np
import pandas as pd
from src.commons.literals import CommonsRepo
from dataclasses import fields
import requests
import tempfile
from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO
import os
from shutil import copy
from bento_mdf import MDF
import bento_meta
from bento_meta.model import Model
from typing import TypeVar

DataFrame = TypeVar("DataFrame")


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
        if tag != "":
            tags_name = cls.get_tags_only(commons_acronym=commons_acronym)
            if tag not in tags_name:
                raise ValueError(
                    f"Tag {tag} not found in repo. Available tags in repo: {*tags_name,}"
                )
            else:
                pass
            tags_list_complete = cls.get_tags_complete(commons_acronym=commons_acronym)
            tag_item = [i for i in tags_list_complete if i["name"] == tag][0]

            zipurl = tag_item["zipball_url"]
        else:
            repo_dict = cls._get_repo_dict(commons_acronym=commons_acronym)
            zipurl = repo_dict["master_zipball"]
        model_file_relpath = cls._get_repo_dict(commons_acronym=commons_acronym)[
            "model_yaml"
        ]
        props_file_relpath = cls._get_repo_dict(commons_acronym=commons_acronym)[
            "props_yaml"
        ]
        http_response = urlopen(zipurl)
        zipfile = ZipFile(BytesIO(http_response.read()))
        # create a temp dir to download the zipfile
        tempdirobj = tempfile.TemporaryDirectory(suffix="_github_dl")
        tempdir = tempdirobj.name
        zipfile.extractall(path=tempdir)
        try:
            copy(
                os.path.join(tempdir, os.listdir(tempdir)[0], model_file_relpath),
                os.path.basename(model_file_relpath),
            )
            copy(
                os.path.join(tempdir, os.listdir(tempdir)[0], props_file_relpath),
                os.path.basename(props_file_relpath),
            )
        except FileNotFoundError as err:
            raise FileNotFoundError(f"File not found: {repr(err)}")
        except Exception as e:
            raise FileNotFoundError(
                f"Error occurred downloading data model files: {repr(e)}"
            )
        return (
            os.path.basename(model_file_relpath),
            os.path.basename(props_file_relpath),
        )


class ReadDataModel:
    """A class reads data model files"""

    def __init__(self, model_file: str, prop_file: str) -> None:
        self.model_file = model_file
        self.prop_file = prop_file
        self.model = self._get_model()

    def _get_model(self) -> Model:
        """Returns bento_meta.meta Model object from model and props file
        Returns:
            Model: bento_meta.meta Model object
        """        
        model_mdf = MDF(self.model_file, self.prop_file, handle="data_model")
        model =  model_mdf.model
        return model

    def get_nodes_list(self) -> list:
        """Returns a dict of node and properties of the node"""
        nodes = [x for x in self.model.nodes]
        return nodes

    def get_node_props_list(self, node_name: str) -> list:
        """Return a list of prop list of a given node

        Args:
            node_name (str): node name in data model

        Returns:
            list: a list of prop names
        """
        node_props = [x for x in self.model.nodes[node_name].props]
        return node_props        

    def _get_prop_cde_code(self, prop_obj) -> str:
        """Returns CDE code of a prop"""
        # set default value for prop_cde_code
        prop_cde_code = np.nan
        # test if the prop has bento_meta Concept obj
        if isinstance(prop_obj.concept, bento_meta.objects.Concept):
            props_term_list = prop_obj.concept.terms
            for i in props_term_list.keys():
                if i[1] == "caDSR":
                    prop_cde_code = props_term_list[i].origin_id
                else:
                    pass
        else:
            pass
        return prop_cde_code

    def _read_each_prop(self, node_name: str, prop_name: str) -> tuple:
        """Extract prop information of a given prop name in a given node

        Args:
            node_name (str): node name
            prop_name (str): prop name

        Returns:
            tuple: a tuple of prop features/info

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
                "item_type": [
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
        """
        prop_obj = self.model.nodes[node_name].props[prop_name]
        # get_attr_dict of a prop
        prop_attr_dict = prop_obj.get_attr_dict()
        # get description
        if "desc" in prop_attr_dict.keys():
            prop_description = prop_attr_dict["desc"]
        else:
            prop_description = ""

        # get if key
        # in CCDI, every prop has a req key, which is not the case in other projects
        if prop_obj.is_key:
            prop_if_key = prop_obj.is_key
        elif prop_name == "id":
            prop_if_key = False
        else:
            prop_if_key = np.nan

        # if required
        prop_required = prop_obj.is_required
        if isinstance(prop_required, bool):
            if prop_required == False:
                prop_required = np.nan
            else:
                pass
        else:
            if prop_required == "Yes":
                prop_required = True
            else:
                prop_required = np.nan

        # get cde code
        prop_CDE = self._get_prop_cde_code(prop_obj=prop_obj)

        # extract enum list if there is a list
        if isinstance(prop_obj.values, list):
            prop_enum_list = prop_obj.values
        else:
            prop_enum_list = []

        prop_value_domain = prop_attr_dict["value_domain"]
        # prop has value_set as value_domain
        if prop_value_domain == "value_set":
            if prop_obj.is_strict:
                prop_type = "enum"
            else:
                prop_type = "string;enum"
        # prop is a list type
        elif prop_value_domain == "list":
            prop_item_type = prop_attr_dict["item_domain"]
            if prop_item_type == "value_set":
                if prop_obj.is_strict:
                    prop_type = "array[enum]"
                else:
                    prop_type = "array[string;enum]"
            else:
                prop_type = f"array[{prop_item_type}]"
        else:
            prop_type = prop_value_domain
        return (
            prop_description,
            prop_type,
            prop_enum_list,
            prop_required,
            prop_CDE,
            prop_if_key,
        )

    def get_prop_dict_df(self) -> DataFrame:
        """Returns a dataframe of node property information of entire data model

        Returns:
            DataFrame: a dataframe
        """        
        node_list = self.get_nodes_list()

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

        for node in node_list:
            # print(f"node: {node}")
            node_property_list = self.get_node_props_list(node_name=node)
            if node_property_list is None:
                # this is for nodes that have no props under
                pass
            else:
                for property in node_property_list:
                    # print(property)
                    (
                        prop_description,
                        prop_type,
                        prop_enum_list,
                        prop_required,
                        prop_cde,
                        prop_key,
                    ) = self._read_each_prop(node_name=node, prop_name=property)

                    prop_append_line = {
                        "Property": [property],
                        "Description": [prop_description],
                        "Node": [node],
                        "Type": [prop_type],
                        "Enum List": [prop_enum_list],
                        "Required": [prop_required],
                        "Key": [prop_key],
                        "CDE": [prop_cde],
                    }
                    prop_return_df = pd.concat(
                        [prop_return_df, pd.DataFrame(prop_append_line)],
                        ignore_index=True,
                    )
        return prop_return_df
