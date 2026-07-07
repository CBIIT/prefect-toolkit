from pymongo import MongoClient, errors
from typing import Union
from src.commons.literals import CrdcDHMongoSecrets
from src.commons.utils import get_secret
import json


class DataHubMongoDB(CrdcDHMongoSecrets):
    """A Class interacts with DataHub MongoDB"""

    def __init__(self, tier: str = "dev") -> None:
        """Inits DataHubMongoDB"""
        self.tier = tier
        if self.tier == "dev":
            self.secret_name = self.secret_name_dev
        elif self.tier == "prod":
            self.secret_name = self.secret_name_prod
        else:
            raise ValueError(f"Unknown tier: {self.tier}")

    def _mongo_connection_str(self) -> str:
        """Returns connection str of

        Returns:
            str: A string for mongodb connection
        """
        secret_name = self.secret_name
        secret_value_dict = get_secret(secret_name=secret_name)
        db_user = secret_value_dict["mongo_db_user"]
        db_password = secret_value_dict["mongo_db_password"]
        db_host = secret_value_dict["mongo_db_host"]
        db_port = secret_value_dict["mongo_db_port"]
        connection_str = f"mongodb://{db_user}:{db_password}@{db_host}:{db_port}/?authMechanism=DEFAULT&authSource=admin"
        return connection_str

    def _mongodb_client(self):
        connectionstr = self._mongo_connection_str()
        client = MongoClient(connectionstr)
        return client

    def _mongo_db_name(self) -> str:
        """Returns a mongodb database name

        Returns:
            str: db name
        """
        secret_name = self.secret_name
        secret_value_dict = get_secret(secret_name=secret_name)
        db_name = secret_value_dict["database_name"]
        return db_name

    def _find_study_version_delimiter(self, study_version_str: str) -> str:
        """Finds delimiter in the study_version str

        Args:
            study_version_str (str): a string of study version, "phs000123.v1.p1|phs000123.v3"

        Returns:
            str: delimiter used in study_version_str
        """
        if "|" in study_version_str:
            return "|"
        elif ";" in study_version_str:
            return ";"
        else:
            # No delimiter identified in the str
            return None

    def _find_latest_version(self, study_version_str: str) -> str:
        """Returns the version (string) from a study version str

        Args:
            study_version_str (str): a string of study version, "phs000123.v1.p1|phs000123.v3"

        Returns:
            str: string of version number
        """
        str_delimiter = self._find_study_version_delimiter(
            study_version_str=study_version_str
        )

        if str_delimiter is not None:
            study_version_list = study_version_str.split(str_delimiter)
            version_list = [int(i.split(".")[1][1:]) for i in study_version_list]
            latest_version = max(version_list)
            return str(latest_version)
        else:
            version = study_version_str.split(".")[1][1:]
            return version

    def get_dbgap_id(self, submission_id: str) -> Union[str, None]:
        """Returns dbGaP accession id in the submissions collection of a submission.
        Practically, it should only return one record. However, multiple dbGaP
        ids might be found associated with one submissionID in dataRecords due to testing sets

        Args:
            submission_id (str): submisisonID in datarecords collection or _id in submission

        Returns:
            str|None: a dbGaP accession number, e.g.,"phs000123"
        """
        client = self._mongodb_client()
        db_name = self._mongo_db_name()
        db = client[db_name]
        submission_collection = db[self.submission_collection]
        try:
            submission_id_query = submission_collection.find(
                {"_id": submission_id},
                {"dbGaPID": 1},
            )
            id_return = []
            for i in submission_id_query:
                i_dbgap_id = i["dbGaPID"]
                id_return.append(i_dbgap_id)
            # return a list. We should expect only a record
            if len(id_return) > 1:
                print(id_return)
            # only return first item
            if "." in id_return[0]:
                # in case the dbGaP id has other informtaion, such as phs000123.v2.p1
                return id_return[0].split(".")[0]
            else:
                return id_return[0]

        except errors.PyMongoError as pe:
            print(
                f"Failed to find submission in submissions collection: {submission_id}\nPyMongoError: {repr(pe)}"
            )
            return None
        except Exception as e:
            print(
                f"Failed to find submission in submissions collection: {submission_id}\n{repr(e)}"
            )
            return None

    def get_study_version(self, submission_id: str) -> Union[str, None]:
        """Returns study version of a submission

        Args:
            submission_id (str):

        Returns:
            str|None: string version of version number
        """
        client = self._mongodb_client()
        db_name = self._mongo_db_name()
        db = client[db_name]
        record_collection = db[self.datarecord_collection]
        try:
            record_collection_query = record_collection.find(
                {"submissionID": submission_id, "nodeType": "study"},
                {"props.study_version": 1},
            )
            if (
                record_collection.count_documents(
                    {"submissionID": submission_id, "nodeType": "study"}
                )
                > 0
            ):
                # we are only looking at the first record
                study_version_str = record_collection_query[0]["props"]["study_version"]
                study_version = self._find_latest_version(
                    study_version_str=study_version_str
                )
                return study_version
            else:
                # no version found
                return None
        except errors.PyMongoError as pe:
            print(
                f"Failed to find submission study version in dataRecords collection: {submission_id}\nPyMongoError: {repr(pe)}"
            )
            return None
        except Exception as e:
            print(
                f"Failed to find submission study version in dataRecords collection: {submission_id}\n{repr(e)}"
            )
            return None

    def get_consent_group(self, submission_id: str) -> Union[dict, None]:
        """Returns a list of consent groups of a submission

        Args:
            submission_id (str): submissionID in "dataRecords" Collection or
            _id in "submissions" Collection. We assume only one study is associated with
            this submissionID

        Returns:
            list[str]|None: A list of consent groups of a submission
        """
        client = self._mongodb_client()
        db_name = self._mongo_db_name()
        db = client[db_name]
        record_collection = db[self.datarecord_collection]
        try:
            query_return_list = record_collection.find(
                {"submissionID": submission_id, "nodeType": "consent_group"},
                {
                    "nodeID": 1,
                    "props.consent_group_name": 1,
                    "props.consent_group_number": 1,
                },
            )
            # we assume this submission id is only associated with one study
            consent_dict = {}
            if (
                record_collection.count_documents(
                    {"submissionID": submission_id, "nodeType": "consent_group"}
                )
                > 0
            ):
                for item in query_return_list:
                    item_id = item["nodeID"]
                    consent_dict[item_id] = {
                        "consent_group_name": item["props"]["consent_group_name"],
                        "consent_group_number": item["props"]["consent_group_number"],
                    }
            else:
                print(f"No consent_group node found in submission {submission_id}")
                # this will stop the flow if no consent group is found in submission
                raise ValueError(
                    f"No consent_group node found in submission {submission_id}"
                )
            return consent_dict
        except errors.PyMongoError as pe:
            print(
                f"Failed to query consent_group in dataRecords collection with submissionID: {submission_id}\nPyMongoError: {repr(pe)}"
            )
            return None
        except Exception as e:
            print(
                f"Failed to query consent_group in dataRecords collection with submissionID: {submission_id}\n{repr(e)}"
            )
            return None

    def get_study_participants(self, submission_id: str) -> Union[list[str], None]:
        """Returns a list of participant ids of a submission

        Args:
            submission_id (str): submissionID in "dataRecords" Collection or
            _id in "submissions" Collection. We assume only one study is associated with
            this submissionID

        Returns:
            list[str]|None: A list of participant ids of a submission
        """
        client = self._mongodb_client()
        db_name = self._mongo_db_name()
        db = client[db_name]
        record_collection = db[self.datarecord_collection]
        try:
            query_return_list = record_collection.find(
                {"submissionID": submission_id, "nodeType": "participant"},
                {"nodeID": 1, "props.participant_id": 1},
            )
            # we assume this submission id is only associated with one study
            participant_list = []
            if (
                record_collection.count_documents(
                    {"submissionID": submission_id, "nodeType": "participant"}
                )
                > 0
            ):
                for item in query_return_list:
                    item_id = item["props"]["participant_id"]
                    participant_list.append(item_id)
            else:
                pass
            return participant_list
        except errors.PyMongoError as pe:
            print(
                f"Failed to query particpant_id in dataRecords collection with submissionID: {submission_id}\nPyMongoError: {repr(pe)}"
            )
            return None
        except Exception as e:
            print(
                f"Failed to query particpant_id in dataRecords collection with submissionID: {submission_id}\n{repr(e)}"
            )
            return None

    def get_study_samples(self, submission_id: str) -> Union[dict, None]:
        """Returns a list of sample ids of a submission
        This functions will first query for all the samples in the submission, and find all the samples that have a direct linkage to a participant.
        For any samples that don't have a direct linkage to a participant, we are limit the case to pdx samples. A path from a pdx sample to participant can be
        participant <- sample <- pdx <- sample (pdx sample)

        This function will find follow this path to find all the pdx sample and participant pairs

        Args:
            submission_id (str): submissionID in "dataRecords" Collection or
            _id in "submissions" Collection. We assume only one study is associated with
            this submissionID

        Returns:
            list[str] | None: A list of dictionary with sample id as key and parent participant id as value
        """
        client = self._mongodb_client()
        db_name = self._mongo_db_name()
        db = client[db_name]
        record_collection = db[self.datarecord_collection]
        try:
            query_return_list = record_collection.find(
                {"submissionID": submission_id, "nodeType": "sample"},
                {"nodeID": 1, "props.sample_id": 1, "parents": 1},
            )
            # we assume this submission id is only associated with one study
            sample_dict = dict()
            sample_wo_participant_parent_list = []
            if (
                record_collection.count_documents(
                    {"submissionID": submission_id, "nodeType": "sample"}
                )
                > 0
            ):
                for item in query_return_list:
                    # item["parents"] is a list, we have situations when a sample is lack of linkage to a participant node, or a sample is linked to multiple types of nodes.
                    item_id = item["props"]["sample_id"]
                    item_parent_list = item["parents"]
                    item_has_participant_parent = False
                    for parent in item_parent_list:
                        if parent["parentType"] == "participant":
                            item_has_participant_parent = True
                            item_parent_query_response = record_collection.find(
                                {
                                    "submissionID": submission_id,
                                    "nodeType": "participant",
                                    "nodeID": parent["parentIDValue"], # in GC, this is the value of study_participant_id property. In CCDI-DCC, this matches to participant_id property
                                }
                            )

                            # now query for "participant_id" value for the participant matched
                            # this is because in GC, parentID value return the value of study_participant_id prop. Participant node also has a participant_id prop, which is the value submitted to dbGaP
                            # However in CCDI-DCC, parentIDValue should match participant_id prop under participant node
                            item_parent_id = item_parent_query_response[0]["props"][
                                "participant_id"
                            ]
                            sample_dict[item_id] = item_parent_id
                            break
                        else:
                            pass
                    if not item_has_participant_parent:
                        sample_wo_participant_parent_list.append(item_id)
                        # try to find the participant in upper level, i.e. sample -> pdx -> sample -> participant
                        sample_wo_participant_upper_participant = self._get_non_participant_sample_origin(submission_id=submission_id, sample_id = item_id)
                        sample_dict[item_id] = sample_wo_participant_upper_participant
                    else:
                        pass
            else:
                pass
            # sample_wo_participant_parent_list is a list of sample ids that don't have a direct linkage to a participant node. These samples are expected to have an participant in upper level, i.e. sample -> pdx -> sample -> participant. 
            return sample_wo_participant_parent_list, sample_dict
        except errors.PyMongoError as pe:
            print(
                f"Failed to query sample_id in dataRecords collection with submissionID: {submission_id}\nPyMongoError: {repr(pe)}"
            )
            return None
        except Exception as e:
            print(
                f"Failed to query sample_id in dataRecords collection with submissionID: {submission_id}\n{repr(e)}"
            )
            return None

    def _get_non_participant_sample_origin(self, submission_id: str, sample_id: str) -> str:
        """Returns the original participant of a sample that is NOT directly linked to a participant
        path: participant <- sample <- pdx <- sample (pdx sample)
        # Only use this function when a sample is found not having a direct participant parent. 
        # This function will raise error if the sample is not linked to a pdx parent, or the pdx parent is not linked to a sample parent, or the sample parent is not linked to a participant parent

        Args:
            submission_id (str): submissionID in "dataRecords" Collection or
            _id in "submissions" Collection. We assume only one study is associated with
            this submissionID
            sample_id (str): sample_id of a sample

        Returns:
            str: the participant_id of the original participant which 
        """
        client = self._mongodb_client()
        db_name = self._mongo_db_name()
        db = client[db_name]
        record_collection = db[self.datarecord_collection]
        try:
            query_return_list = record_collection.find(
                {"submissionID": submission_id, "nodeType": "sample", "props.sample_id": sample_id},
                {"nodeID": 1, "props.sample_id": 1, "parents": 1},
            )
            if (
                record_collection.count_documents(
                    {"submissionID": submission_id, "nodeType": "sample", "props.sample_id": sample_id}
                )
                > 0
            ):
                # there should be only one item returned, since sample_id should be unique for a given submission_id
                return_item = query_return_list[0]
                parents_list = return_item["parents"]
                # find the pdx parent or cell_line parent
                has_pdx_parent = False
                for parent in parents_list:
                    if parent["parentType"] == "pdx":
                        pdx_parent_id = parent["parentIDValue"]
                        has_pdx_parent = True
                        break
                    else:
                        pass
                if not has_pdx_parent:
                    raise ValueError(f"Sample {sample_id} in submission {submission_id} DOES NOT have a pdx parent or a participant parent")
                else:
                    # now query for the pdx sample, and this pdx record should be either pointing to a sample or a study record
                    pdx_query_return_list = record_collection.find(
                        {"submissionID": submission_id, "nodeType": "pdx", "props.pdx_id": pdx_parent_id},
                        {"nodeID": 1, "props.pdx_id": 1, "parents": 1},
                    )
                    if (
                        record_collection.count_documents(
                            {"submissionID": submission_id, "nodeType": "pdx", "props.pdx_id": pdx_parent_id}
                        )
                        > 0
                    ):
                        pdx_return_item = pdx_query_return_list[0] # there should be only one pdx_id match within a given submission
                        pdx_parents_list = pdx_return_item["parents"]
                        has_sample_parent = False
                        for parent in pdx_parents_list:
                            if parent["parentType"] == "sample":
                                sample_parent_id = parent["parentIDValue"]
                                has_sample_parent = True
                                break
                            else:
                                pass
                        if not has_sample_parent:
                            raise ValueError(f"PDX {pdx_parent_id} in submission {submission_id} DOES NOT have a sample parent. This PDX is the parent of sample {sample_id}, so we expect this PDX to have a sample parent which is linked to a participant")
                        else:
                            # now query for the sample parent, and this sample should have a participant parent
                            sample_query_return_list = record_collection.find(
                                {"submissionID": submission_id, "nodeType": "sample", "props.sample_id": sample_parent_id},
                                {"nodeID": 1, "props.sample_id": 1, "parents": 1},
                            )
                            if (
                                record_collection.count_documents(
                                    {"submissionID": submission_id, "nodeType": "sample", "props.sample_id": sample_parent_id}
                                )
                                > 0
                            ):
                                sample_return_item = sample_query_return_list[0] # there should be only one sample_id match within a given submission
                                sample_parents_list = sample_return_item["parents"]
                                has_participant_parent = False
                                for parent in sample_parents_list:
                                    if parent["parentType"] == "participant":
                                        participant_parent_id = parent["parentIDValue"]
                                        has_participant_parent = True
                                        break
                                    else:
                                        pass
                                if not has_participant_parent:
                                    raise ValueError(f"Sample {sample_parent_id} in submission {submission_id} DOES NOT have a participant parent. This sample is the parent of PDX {pdx_parent_id}, which is the parent of sample {sample_id}. We expect this sample to have a participant parent")
                                else:
                                    # query participant node using that parentIDValue
                                    participant_query_return_list = record_collection.find(
                                        {
                                            "submissionID": submission_id,
                                            "nodeType": "participant",
                                            "nodeID": participant_parent_id, # in GC, this is the value of study_participant_id property. In CCDI-DCC, this matches to participant_id property
                                        }
                                    )
                                    item_participant_id= participant_query_return_list[0]["props"]["participant_id"]
                                    return item_participant_id
            else:
                raise ValueError(f"Sample {sample_id} CAN NOT BE FOUND in submission {submission_id}")
        except errors.PyMongoError as pe:
            pe.args = (f"custom message: Failed to query sample in dataRecords collection with submissionID: {submission_id} and sample_id: {sample_id}\n{repr(pe)}",)
            raise
        except Exception as e:
            e.args = (f"custom message: Failed to query sample in dataRecords collection with submissionID: {submission_id} and sample_id: {sample_id}\n{repr(e)}")
            raise

    def get_study_participants_consent(self, submission_id: str) -> Union[dict, None]:
        """Returns a dict of participant ids and their consent code of a submission

        Args:
            submission_id (str): submissionID in "dataRecords" Collection or
            _id in "submissions" Collection. We assume only one study is associated with
            this submissionID

        Returns:
            dict|None: A dict of participant ids and their consent code of a submission
        """
        client = self._mongodb_client()
        db_name = self._mongo_db_name()
        db = client[db_name]
        record_collection = db[self.datarecord_collection]
        # if no consent group node is found, the flow will stop at this step
        consent_dict = self.get_consent_group(submission_id=submission_id)
        print(f"consent_group nodes found in DB: {consent_dict}")

        try:
            # query participants without filtering on consent_group linkage
            # there are cases that participants are only linked to study node while consent node is present
            all_participant_counts = record_collection.count_documents(
                {"submissionID": submission_id, "nodeType": "participant"},
            )
            # we assume this submission id is only associated with one study
            participant_count_with_study = record_collection.count_documents(
                {
                    "submissionID": submission_id,
                    "nodeType": "participant",
                    "parents.parentType": "study",
                }
            )
            print(
                f"{participant_count_with_study}/{all_participant_counts} participants are linked to study node"
            )

            # filter participants that have a linkage towards consent_group
            print("Querying participants with filtering on consent_group linkage")
            participant_count_with_consent = record_collection.count_documents(
                {
                    "submissionID": submission_id,
                    "nodeType": "participant",
                    "parents.parentType": "consent_group",
                }
            )
            print(
                f"{participant_count_with_consent}/{all_participant_counts} participants are linked to consent_group node"
            )

            participant_consent_dict = {}
            if participant_count_with_consent > 0:
                query_return_list = record_collection.find(
                    {
                        "nodeType": "participant",
                        "submissionID": submission_id,
                        "parents.parentType": "consent_group",
                    },
                    {"props.participant_id": 1, "parents": 1},
                )
                for item in query_return_list:
                    # get participant_id
                    item_id = item["props"]["participant_id"]
                    # value under "parents" key stores linkage info
                    item_parents = item["parents"]
                    # the query requires return participant has a linkage towards consent_group
                    for parent in item_parents:
                        if parent["parentType"] == "consent_group":
                            print(
                                f"participant {item_id} has a parentType that's a consent_group, getting consent info"
                            )
                            item_consent_id = parent["parentIDValue"]
                            item_consent_name = consent_dict[item_consent_id][
                                "consent_group_name"
                            ]
                            item_consent_number = consent_dict[item_consent_id][
                                "consent_group_number"
                            ]
                            participant_consent_dict[item_id] = {
                                "consent_group_number": item_consent_number,
                                "consent_group_name": item_consent_name,
                            }
                        else:
                            print(
                                f"participant {item_id} has a parentType other than consent_group, skipping"
                            )
                            # this is not a linkage towards consent_group
                            pass
                return participant_consent_dict
            else:
                print(f"No participant with linkage to consent_group found in submission {submission_id}")
                if len(consent_dict)==1:
                    print("Only one consent group record found in this submission, assuming all participants belong to this consent group")
                    # there is only one consent group in this submission, so we can assume all participants belong to this group consent number
                    consent_group_id = list(consent_dict.keys())[0]
                    consent_group_name = consent_dict[consent_group_id]["consent_group_name"]
                    consent_group_number = consent_dict[consent_group_id]["consent_group_number"]
                    all_participants = record_collection.find(
                        {"submissionID": submission_id, "nodeType": "participant"},
                        {"props.participant_id": 1},
                    )
                    for item in all_participants:
                        item_id = item["props"]["participant_id"]
                        participant_consent_dict[item_id] = {
                            "consent_group_number": consent_group_number,
                            "consent_group_name": consent_group_name,
                        }
                else:
                    print(f"More than one consent group found in submission {submission_id}, cannot assume all participants belong to one consent group")
                return participant_consent_dict
        except errors.PyMongoError as pe:
            print(
                f"Failed to query particpant_id in dataRecords collection with submissionID: {submission_id}\nPyMongoError: {repr(pe)}"
            )
            return None
        except Exception as e:
            print(
                f"Failed to query particpant_id in dataRecords collection with submissionID: {submission_id}\n{repr(e)}"
            )
            return None
