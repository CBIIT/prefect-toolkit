from src.commons.datamodel import ReadDataModel
from src.commons.utils import ReadSubmTsv
from src.commons.literals import CommonsFeat
import numpy as np
import pandas as pd
from dataclasses import fields, dataclass
from typing import TypeVar

DataFrame = TypeVar("DataFrame")


class SubmVal(ReadSubmTsv):
    """A class performs validation on submission tsv files"""

    def __init__(self, filepath_list: list[str]):
        self.filepath_list = filepath_list

    @staticmethod
    def commons_feature(commons_acronym: str) -> dict:
        """Returns a dictionary of features of a commons

        Args:
            commons_acronym (str): Commons acronym

        Returns:
            dict: A dictionary of commons feature
        """
        commons_acronym = commons_acronym.lower()
        available_commons = [field.name for field in fields(CommonsFeat)]
        if commons_acronym not in available_commons:
            raise ValueError(f"Unknown commons acronym {commons_acronym} provided")
        else:
            pass
        return_dict = CommonsFeat.__dataclass_fields__[commons_acronym].default_factory()
        return return_dict

    @classmethod
    def commons_delimiter(cls, commons_acronym: str) -> str:
        """Returns the delimiter of a commons

        Args:
            commons_acronym (str): Commons acronym

        Returns:
            str: Commons delimiter
        """
        commons_feature_dict = cls.commons_feature(commons_acronym=commons_acronym)
        commons_delimiter = commons_feature_dict["delimiter"]
        return commons_delimiter

    @staticmethod
    def section_header(section_name: str) -> str:
        """Returns a string which can render nice formatted header

        Args:
            section_name (str): name of a section

        Returns:
            str: A string

        Example output:
            #############
            #           #
            #  example  #
            #           #
            #############
        """
        return_str = "#" * (8 + len(section_name)) + "\n"
        return_str = return_str + "#" + " " * (6 + len(section_name)) + "#\n"
        return_str = return_str + "#   " + section_name + "   #\n"
        return_str = return_str + "#" + " " * (6 + len(section_name)) + "#\n"
        return_str = return_str + "#" * (8 + len(section_name))
        return return_str

    @staticmethod
    def report_header(
        report_path: str, tsv_folder_path: str, model_file: str, prop_file: str
    ) -> str:
        """Returns a string that can render report header

        Args:
            report_path (str): file path of validation report
            tsv_folder_path (str): folder path of tsv files
            model_file (str): data model yml file
            prop_file (str): data model property yml file

        Returns:
            str: A string
        """
        report_header = f"""#####################
#                   #
# Validation Report #
#                   #
#####################

Validation report filename:
        - {report_path}
Submission file folder for validation: 
        - {tsv_folder_path}
Model YAML file: 
        - {model_file}
Property YAML file: 
        - {prop_file}

"""
        return report_header

    def _validate_required_properties_one_file(
        self, filepath: str, req_prop_list: list[str]
    ) -> str:
        """Returns a string of validation summary on required properties for a single tsv file

        Args:
            filepath (str): A file path of tsv
            req_prop_list (list[str]): A list of required property names

        Returns:
            str: A string of validation summary of a file
        """
        file_df = self.read_tsv(file_path=filepath)
        node_type = self.get_type(file_path=filepath)
        properties = file_df.columns
        print_str = f"\n\t{node_type}\n\t----------\n\t"
        # report if all req_prop_list properties are found in the file
        unfound_property = [i for i in req_prop_list if i not in properties]
        if len(unfound_property) > 0:
            print_str = (
                print_str
                + f"ERROR: Required property {*unfound_property,} not found in file"
                + "\n\t"
            )
        else:
            pass
        # loop through every prop in properties, and check if prop is required
        check_list = []
        for property in properties:
            if property in req_prop_list:
                proprety_dict = {}
                proprety_dict["node"] = node_type
                proprety_dict["property"] = property
                if file_df[property].isna().any():
                    bad_positions = np.where(file_df[property].isna())[0] + 2
                    # print out the row number contains missing value
                    pos_print = ",".join([str(i) for i in bad_positions])
                    proprety_dict["check"] = "ERROR"
                    proprety_dict["error row"] = pos_print
                else:
                    proprety_dict["check"] = "PASS"
                    proprety_dict["error row"] = ""
                check_list.append(proprety_dict)
            else:
                pass

        check_df = pd.DataFrame.from_records(check_list)
        # wrape the text of error row if the length exceeds 25
        if check_df.shape[0] > 0:
            check_df["error row"] = check_df["error row"].str.wrap(25)
        else:
            pass

        print_str = (
            print_str
            + check_df.to_markdown(tablefmt="rounded_grid", index=False).replace(
                "\n", "\n\t"
            )
            + "\n"
        )
        return print_str

    def validate_required_properties(self, data_model: ReadDataModel) -> str:
        """Validates required properties for a list of tsv files

        Args:
            data_model (ReadDataModel): A ReadDataModel obj
            output_file (str): output file path

        Returns:
            str: A string of validation summary
        """
        prop_df = data_model.props_df
        section_title = (
            self.section_header(section_name="Required Properties Check")
            + "\nThis section is for required properties for all nodes that contain data.\nFor information on required properties per node, please see the 'Dictionary' page of the template file.\nFor each entry, it is expected that all required information has a value:\n----------\n"
        )
        validation_str = ""
        for file in self.filepath_list:
            file_type = self.get_type(file_path=file)

            # CCDI uses True/False in req field, while icdc uses Yes/No/Preferred in req field
            if "Yes" in prop_df["Required"].unique().tolist():
                required_value = "Yes"
            else:
                required_value = True

            required_prop_list = (
                prop_df[
                    (prop_df["Node"] == file_type)
                    & (prop_df["Required"] == required_value)
                ]["Property"]
                .unique()
                .tolist()
            )
            validation_str_file = self._validate_required_properties_one_file(
                filepath=file, req_prop_list=required_prop_list
            )
            validation_str = validation_str + validation_str_file
        return_str = section_title + validation_str
        del prop_df
        return return_str

    def _validate_whitespace_issue_one_file(self, filepath: str) -> str:
        """Returns a string of validation summary on required properties for a single tsv file

        Args:
            filepath (str): A file path of tsv file

        Returns:
            str: A string of validation summary of a file
        """
        file_df = self.read_tsv(file_path=filepath)
        node_type = self.get_type(file_path=filepath)
        properties = file_df.columns
        print_str = f"\n\t{node_type}\n\t----------\n\t"
        check_list = []
        for property in properties:
            # if the property is not completely empty:
            if not file_df[property].isna().all():
                property_dict = {}
                # if there are some values that do not match when positions are stripped of white space
                if (
                    file_df[property].fillna("")
                    != file_df[property].str.strip().fillna("")
                ).any():
                    property_dict["node"] = node_type
                    property_dict["property"] = property
                    bad_positions = (
                        np.where(
                            file_df[property].fillna("")
                            != file_df[property].str.strip().fillna("")
                        )[0]
                        + 2
                    )

                    # itterate over that list and print out the values
                    pos_print = ",".join([str(i) for i in bad_positions])
                    property_dict["check"] = "ERROR"
                    property_dict["error row"] = pos_print
                    check_list.append(property_dict)
                else:
                    pass
            else:
                pass
        check_df = pd.DataFrame.from_records(check_list)
        # wrape the text of error row if the length exceeds 25
        if check_df.shape[0] > 0:
            check_df["error row"] = check_df["error row"].str.wrap(25)
        else:
            pass
        print_str = (
            print_str
            + check_df.to_markdown(tablefmt="rounded_grid", index=False).replace(
                "\n", "\n\t"
            )
            + "\n"
        )
        return print_str

    def validate_whitespace_issue(self) -> str:
        """Validate whitespace issue for a list of tsv files

        Returns:
            str: A string of validation summary
        """
        section_title = (
            "\n\n"
            + self.section_header(section_name="Whitespace Check")
            + "\nThis section checks for white space issues in all nonempty properties.\n----------\n"
        )
        validation_str = ""
        for file in self.filepath_list:
            validation_str_file = self._validate_whitespace_issue_one_file(
                filepath=file
            )
            validation_str = validation_str + validation_str_file
        return_str = section_title + validation_str
        return return_str

    def _validate_terms_value_sets_one_file(
        self, filepath: str, data_model: ReadDataModel, commons_delimiter: str
    ) -> str:
        """Returns a string of validation summary on terms and value sets for a single tsv file

        Args:
            filepath (str):A file path of tsv file
            data_model (ReadDataModel): An obj of ReadDataModel
            commons_delimiter (str): Commons delimiter

        Returns:
            str: A string of validation summary of a file
        """
        file_df = self.read_tsv(file_path=filepath)
        properties = file_df.columns
        node_type = self.get_type(file_path=filepath)
        prop_df = data_model.props_df
        prop_df_node = prop_df[
            (prop_df["Node"] == node_type) & (prop_df["Type"].str.contains("enum"))
        ][["Property", "Node", "Type", "Example value"]]

        print_str = f"\n\t{node_type}\n\t----------\n\t"
        check_list = []
        if prop_df_node.shape[0] > 0:
            # this means we have at least one property in this node type has
            # enum value
            for property in properties:
                print(f"property: {property}")
                if property in prop_df_node["Property"].tolist():
                    # property type has "enum" in it
                    property_dict = {}
                    property_dict["node"] = node_type
                    property_dict["property"] = property
                    property_type = prop_df_node.loc[
                        prop_df_node["Property"] == property, "Type"
                    ].values[0]
                    property_enum_list = prop_df_node.loc[
                        prop_df_node["Property"] == property, "Example value"
                    ].tolist()[0]
                    unique_values = file_df[property].dropna().unique()
                    print(f"unique values in column: {*unique_values,}")
                    print(f"allowed enum list: {*property_enum_list,}")
                    if len(unique_values) == 0:
                        # this property col is empty
                        property_dict["check"] = "empty"
                        property_dict["error value"] = ""
                    else:
                        # this property col is not empty
                        if "array" in property_type:
                            # if the property type is array
                            # this covers array[enum], array[string;enum]
                            invalid_list = []
                            for v in unique_values:
                                if commons_delimiter in v:
                                    # if the value of an item has commons_delimiter, usually ";"
                                    v_item_list = v.split(commons_delimiter)
                                    v_invalid = [
                                        i
                                        for i in v_item_list
                                        if i not in property_enum_list
                                    ]

                                    invalid_list.extend(v_invalid)
                                else:
                                    # if the value of an item doesn't have commons_delimiter, usually ";"
                                    if v not in property_enum_list:
                                        invalid_list.append(v)
                                    else:
                                        pass
                            # extract only unique values in invalid_list, because repetitive values
                            # can occure when value split into list
                            invalid_list = list(set(invalid_list))
                            # print(f"{*invalid_list,} not found in {property_enum_list}")
                            if len(invalid_list) > 0:
                                invalid_list = ["[" + i + "]" for i in invalid_list]
                                invalid_list_str = ",\n".join(invalid_list)
                                property_dict["error value"] = invalid_list_str
                                if "string" in property_type:
                                    property_dict["check"] = (
                                        "WARNING\nfree strings allowed"
                                    )
                                else:
                                    property_dict["check"] = "ERROR\nunrecognized value"
                            else:
                                property_dict["check"] = "PASS"
                                property_dict["error value"] = ""
                        else:
                            # property is no array
                            # this covers enum, stirng;enum
                            invalid_list = []
                            for v in unique_values:
                                print(v)
                                if v not in property_enum_list:
                                    invalid_list.append(v)
                                else:
                                    pass
                            # print(f"{*invalid_list,} not found in {property_enum_list}")
                            if len(invalid_list) > 0:
                                invalid_list = ["[" + i + "]" for i in invalid_list]
                                invalid_list_str = ",\n".join(invalid_list)
                                property_dict["error value"] = invalid_list_str
                                if "string" in property_type:
                                    property_dict["check"] = (
                                        "WARNING\nfree strings allowed"
                                    )
                                else:
                                    property_dict["check"] = "ERROR\nunrecognized value"
                            else:
                                property_dict["check"] = "PASS"
                                property_dict["error value"] = ""
                    check_list.append(property_dict)
                else:
                    # property not found in prop_df_node["Property"]
                    pass
        else:
            # no enum property in prop_df_node
            pass
        del prop_df
        del prop_df_node
        # turn check_list to df
        check_df = pd.DataFrame.from_records(check_list)
        if check_df.shape[0] > 0:
            check_df["error value"] = check_df["error value"].str.wrap(45)
            check_df["property"] = check_df["property"].str.wrap(20)
            check_df["node"] = check_df["node"].str.wrap(20)
            # reorder checkdf columns
            check_df = check_df[["node", "property", "check", "error value"]]
        else:
            pass

        print_str = (
            print_str
            + check_df.to_markdown(tablefmt="rounded_grid", index=False).replace(
                "\n", "\n\t"
            )
            + "\n"
        )
        return print_str

    def validate_terms_value_sets(self, data_model: ReadDataModel, commons_acronym: str) -> str:
        """Validate terms and value sets for a list of tsv files

        Args:
            data_model (ReadDataModel): An obj of ReadDataModel
            commons_acronym (str): Common acronym

        Returns:
            str: A string of validation summary
        """
        section_title = (
            "\n\n"
            + self.section_header(section_name="Terms and Value Sets Check")
            + "\nThe following columns have controlled vocabulary on the 'Terms and Value Sets' page of the template file.\nIf the values present do not match, they will noted and in some cases the values will be replaced:\n----------\n"
        )
        validation_str = ""
        commons_delimiter =  self.commons_delimiter(commons_acronym=commons_acronym)
        for file in self.filepath_list:
            validation_str_file = self._validate_terms_value_sets_one_file(
                filepath=file,
                data_model=data_model,
                commons_delimiter=commons_delimiter,
            )
            validation_str = validation_str + validation_str_file
        return_str = section_title + validation_str
        return return_str

    def _validate_numeric_integer_one_file(
        self, filepath: str, file_numeric_dict: dict
    ) -> str:
        """Returns a summary string of numeric integer validation of a tsv file

        Args:
            filepath (str): A file path of tsv file
            file_numeric_dict (dict): A dictionary of numeric properties based off file type

        Returns:
            str: A summary string
        """
        file_df = self.read_tsv(file_path=filepath)
        file_type = self.get_type(file_path=filepath)
        properties = file_df.columns
        print_str = f"\n\t{file_type}\n\t----------\n\t"

        check_list = []
        if len(file_numeric_dict.keys()) == 0:
            # no numeric or integer property in this node
            pass
        else:
            # at least one numeric or integer property in this node
            for property in properties:
                if property in file_numeric_dict.keys():
                    # property is either numeric or integer in file_numeric_dict
                    property_dict = {}
                    property_dict["node"] = file_type
                    property_dict["property"] = property
                    property_type = file_numeric_dict[property]
                    if len(file_df[property].dropna().tolist()):
                        # there is at least one non NA value in the property column
                        error_rows = []
                        for index, row in file_df.iterrows():
                            row_property_value = row[property]
                            # if this value is not NA
                            if pd.notna(row_property_value):
                                if property_type == "number":
                                    # test if row_property_value is float
                                    try:
                                        float(row_property_value)
                                        if_valid = True
                                    except ValueError:
                                        if_valid = False
                                    if not if_valid:
                                        error_rows.append(index + 2)
                                    else:
                                        pass
                                else:
                                    # test if row_property_value is int
                                    try:
                                        int(row_property_value)
                                        if_valid = True
                                    except ValueError:
                                        if_valid = False
                                    if not if_valid:
                                        error_rows.append(index + 2)
                                    else:
                                        pass
                            else:
                                pass
                        if len(error_rows) > 0:
                            property_dict["check"] = "ERROR"
                            property_dict["error row"] = ",".join(
                                [str(i) for i in error_rows]
                            )
                        else:
                            property_dict["check"] = "PASS"
                            property_dict["error row"] = ""

                    else:
                        # the entire property column is empty
                        property_dict["check"] = "empty"
                        property_dict["error row"] = ""

                    check_list.append(property_dict)
                else:
                    # property is not number or integer
                    pass
        check_df = pd.DataFrame.from_records(check_list)
        # reformat check df column width
        if check_df.shape[0] > 0:
            check_df["error row"] = check_df["error row"].str.wrap(30)
            check_df["property"] = check_df["property"].str.wrap(25)
        else:
            pass
        # concatenate print str with check_df
        print_str = (
            print_str
            + check_df.to_markdown(tablefmt="rounded_grid", index=False).replace(
                "\n", "\n\t"
            )
            + "\n"
        )
        return print_str

    def validate_numeric_integer(self, data_model: ReadDataModel) -> str:
        """Validates integer and numeric properties for a list of tsv files

        Args:
            data_model (ReadDataModel): An obj of ReadDataModel

        Returns:
            str: A string of validation summary
        """
        section_title = (
            "\n\n"
            + self.section_header(section_name="Numeric and Integer Check")
            + "\nThis section will display any values in properties that are expected to be either numeric or integer based on the Dictionary, but have values that are not:\n----------\n"
        )

        # extract only number and integer properties
        prop_df = data_model.props_df
        prop_df_numeric = prop_df.loc[
            prop_df["Type"].isin(["integer", "number"]), ["Property", "Node", "Type"]
        ]
        del prop_df

        validation_str = ""
        for file in self.filepath_list:
            file_type = self.get_type(file_path=file)
            file_numeric_df = prop_df_numeric.loc[
                prop_df_numeric["Node"] == file_type, ["Property", "Type"]
            ]
            file_numeric_dict = dict(
                zip(file_numeric_df["Property"], file_numeric_df["Type"])
            )
            file_validation_str = self._validate_numeric_integer_one_file(
                filepath=file, file_numeric_dict=file_numeric_dict
            )
            validation_str = validation_str + file_validation_str
        return_str = section_title + validation_str
        return return_str

    def _validate_cross_links_one_file(
        self, filepath: str, type_mapping_dict: dict
    ) -> str:
        """Returns a summary str of validation on cross links of a tsv file

        Args:
            filepath (str): File path of a tsv file
            type_mapping_dict (dict): A dict with type as key and file path as value

        Returns:
            str: A summary string of a tsv file
        """
        file_df = self.read_tsv(file_path=filepath)
        file_type = self.get_type(file_path=filepath)
        print_str = f"\n\t{file_type}\n\t----------\n"
        # pull out all the linking properties
        link_props = file_df.filter(like=".", axis=1).columns.tolist()

        # check if each row has at least one link value
        if len(link_props) > 0:
            link_missing_row = []
            for index, row in file_df.iterrows():
                row_values = row[link_props].dropna().tolist()
                if len(set(row_values)) == 0:
                    link_missing_row.append(index + 2)
                else:
                    pass
            if len(link_missing_row) > 0:
                print_str = (
                    print_str
                    + f"\tERROR: The entry on row {*link_missing_row,} contains ZERO links. Every entry (except study node) should have one link to a parent node\n"
                )
            else:
                pass
        else:
            # no links prop found
            pass

        # check if more than one links were found in a single entry
        if len(link_props) > 1:
            link_multiple_row = []
            for index, row in file_df.iterrows():
                row_values = row[link_props].dropna().tolist()
                #  if there are entries that have more than one linking property value
                if len(set(row_values)) > 1:
                    link_multiple_row.append(index + 2)
                else:
                    pass
            if len(link_multiple_row) > 0:
                print_str = (
                    print_str
                    + f"\tWARNING: The entry on row {*link_multiple_row,} contains multiple links. While multiple links can occur, they are often not needed or best practice.\n"
                )
            else:
                pass
        else:
            # skip this test if no link prop or only one link prop is found
            pass

        # check if link prop value can be found in the parent node
        check_list = []
        for link_prop in link_props:
            property_dict = {}
            property_dict["node"] = file_type
            property_dict["property"] = link_prop
            # find the unique values of that linking property
            link_values = file_df[link_prop].dropna().unique().tolist()

            # if there are values in this link_prop
            if len(link_values) > 0:
                parent_type = link_prop.split(".")[0]
                parent_type_key = link_prop.split(".")[1]
                if " " in parent_type:
                    print_str = (
                        print_str
                        + f"\tERROR: the linking property [{link_prop}] has space found. Please remove any trailiing or leading space\n"
                    )
                    parent_type = parent_type.strip()
                if parent_type in type_mapping_dict.keys():
                    # if parent_type node can found in the files user provides

                    parent_file = type_mapping_dict[parent_type]
                    parent_df = self.read_tsv(file_path=parent_file)
                    linking_values = (
                        parent_df[parent_type_key].dropna().unique().tolist()
                    )

                    # test to see if all the values are found
                    # all True if all values in link_values can be found in linking values(parent node sheet id)
                    matching_links = [
                        True if id in linking_values else False for id in link_values
                    ]
                    # if not all values match, determined the mismatched values
                    if not all(matching_links):
                        mis_match_values = np.array(link_values)[
                            ~np.array(matching_links)
                        ].tolist()

                        # for each mismatched value, throw an error.
                        property_dict["check"] = "ERROR"
                        property_dict["error value"] = ",".join(mis_match_values)
                    else:
                        property_dict["check"] = "PASS"
                        property_dict["error value"] = ""
                else:
                    property_dict["check"] = (
                        f"ERROR:\nFile for [{parent_type}] not found"
                    )
                    property_dict["error value"] = ""
            else:
                property_dict["check"] = "empty"
                property_dict["error value"] = ""
            check_list.append(property_dict)
        check_df = pd.DataFrame.from_records(check_list)
        if check_df.shape[0] > 0:
            check_df["error value"] = check_df["error value"].str.wrap(30)
            check_df["property"] = check_df["property"].str.wrap(25)
            check_df = check_df[["node", "property", "check", "error value"]]
        else:
            pass
        print_str = (
            print_str
            + "\t"
            + check_df.to_markdown(tablefmt="rounded_grid", index=False).replace(
                "\n", "\n\t"
            )
            + "\n"
        )
        return print_str

    def validate_cross_links(self) -> str:
        """Validates cross links for a list of tsv files

        Returns:
            str: A string of validation summary
        """
        section_title = (
            "\n\n"
            + self.section_header(section_name="Cross Links Check")
            + "\nIf there are unexpected or missing values in the linking values between nodes, they will be reported below:\n----------\n"
        )
        type_mapping_dict = self.file_type_mapping(filepath_list=self.filepath_list)
        validation_str = ""
        for file in self.filepath_list:
            file_validation_str = self._validate_cross_links_one_file(
                filepath=file, type_mapping_dict=type_mapping_dict
            )
            validation_str = validation_str + file_validation_str
        return_str = section_title + validation_str
        return return_str

    def _validate_unique_key_id_one_file(
        self, filepath: str, node_key_df: DataFrame
    ) -> str:
        """Validate key id in a file that has key property

        Args:
            filepath (str): File path of a tsv file
            node_key_df (DataFrame): A pandas Dataframe of node key properties

        Returns:
            str: A string of validation summary of a file
        """
        file_type = self.get_type(file_path=filepath)
        print_str = f"\n\t{file_type}\n\t----------\n\t"

        # if the file has a key prop
        check_list = []
        if len(node_key_df["Key"].dropna().tolist()) > 0:
            # If there is a key prop in node_key_df
            file_df = self.read_tsv(file_path=filepath)
            key_props = node_key_df.loc[node_key_df["Key"] == True, "Property"].tolist()
            for key_prop in key_props:
                property_dict = {}
                property_dict["node"] = file_type
                property_dict["property"] = key_prop
                if file_df[key_prop].notna().any():
                    # as long as this is not empty
                    # if the length of the data frame is not the same length of the unique key property values, then we have some non-unique values
                    if len(file_df[key_prop].dropna()) != len(
                        file_df[key_prop].dropna().unique()
                    ):
                        property_dict["check"] = "ERROR"
                        # create a table of values and counts
                        freq_key_values = file_df[key_prop].value_counts()
                        # pull out a unique list of values that have more than one instance
                        not_unique_key_values = (
                            file_df[
                                file_df[key_prop].isin(
                                    freq_key_values[freq_key_values > 1].index
                                )
                            ][key_prop]
                            .unique()
                            .tolist()
                        )
                        # itterate over that list and print out the values
                        enum_print = ",".join(not_unique_key_values)
                        property_dict["error value"] = enum_print
                    else:
                        property_dict["check"] = "PASS"
                        property_dict["error value"] = ""
                else:
                    property_dict["check"] = "empty"
                    property_dict["error value"] = ""
                check_list.append(property_dict)
            check_df = pd.DataFrame.from_records(check_list)
            if check_df.shape[0] > 0:
                check_df["error value"] = check_df["error value"].str.wrap(30)
                check_df["property"] = check_df["property"].str.wrap(25)
            else:
                pass
            print_str = (
                print_str
                + check_df.to_markdown(tablefmt="rounded_grid", index=False).replace(
                    "\n", "\n\t"
                )
                + "\n"
            )
        else:
            print_str = (
                print_str
                + f"WARNING: node {file_type} file contains no Key id property\n"
            )
        return print_str

    def validate_unique_key_id(self, data_model: ReadDataModel) -> str:
        """Validates unique key id for a list of tsv files

        Args:
            data_model (ReadDataModel): An obj of ReadDataModel

        Returns:
            str: A string of validation summary
        """
        section_title = (
            "\n\n"
            + self.section_header(section_name="Unique Key Value Check")
            + "\nThe following will check for multiples of key values, which are expected to be unique.\nIf there are any unexpected values, they will be reported below:\n----------\n"
        )
        # extract only number and integer properties
        prop_df = data_model.props_df

        validation_str = ""
        for file in self.filepath_list:
            file_type = self.get_type(file_path=file)
            file_prop_df = prop_df.loc[
                prop_df["Node"] == file_type, ["Property", "Node", "Key"]
            ]
            validation_str_file = self._validate_unique_key_id_one_file(
                filepath=file, node_key_df=file_prop_df
            )
            validation_str = validation_str + validation_str_file
        return_str = section_title + validation_str
        del prop_df
        return return_str
