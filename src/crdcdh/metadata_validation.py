from pymongo import MongoClient, errors
from typing import Union
from src.commons.constants import CrdcDHMongoSecrets
from src.commons.utils import get_secret


class DataHubMongoDB(CrdcDHMongoSecrets):
    """A Class interacts with DataHub MongoDB
    """    

    def __init__(self):
        """Inits DataHubMongoDB
        """

    def _mongo_connection_str(self) -> str:
        """Returns connection str of 

        Returns:
            str: A string for mongodb connection
        """
        secret_name =  self.secret_name
        secret_value_dict =  get_secret(secret_name=secret_name)
        db_user = secret_value_dict["mongo_db_user"]
        db_password = secret_value_dict["mongo_db_password"]
        db_host = secret_value_dict["mongo_db_host"]
        db_port = secret_value_dict["mongo_db_port"]
        connection_str = f"mongodb://{db_user}:{db_password}@{db_host}:{db_port}/?authMechanism=DEFAULT&authSource=admin"
        return connection_str

    def _mongodb_client(self):
        connectionstr = self._mongo_connection_str()
        client =  MongoClient(connectionstr)
        return client

    def _mongo_db_name(self) -> str:
        """Returns a mongodb database name

        Returns:
            str: db name
        """        
        secret_name =  self.secret_name
        secret_value_dict =  get_secret(secret_name=secret_name)
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
        db_name =  self._mongo_db_name()
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
                f"Failed to find submission in submissions collection: {submission_id}\n{repr(pe)}"
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
        db_name =  self._mongo_db_name()
        db = client[db_name]
        record_collection = db[self.datarecord_colleciton]
        try:
            record_collection_query = record_collection.find(
                {"submissionID": submission_id, "nodeType": "study"},
                {"props.study_version": 1},
            )
            if record_collection.count_documents({"submissionID": submission_id, "nodeType": "study"}) > 0:
                # we are only looking at the first record
                print(record_collection.count_documents({"submissionID": submission_id, "nodeType": "study"}))
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
                f"Failed to find submission study version in dataRecords collection: {submission_id}\n{repr(pe)}"
            )
            return None
        except Exception as e:
            print(
                f"Failed to find submission study version in dataRecords collection: {submission_id}\n{repr(e)}"
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
        db_name =  self._mongo_db_name()
        db = client[db_name]
        record_collection = db[self.datarecord_colleciton]
        try:
            query_return_list = record_collection.find({"submissionID":submission_id, "nodeType":"participant"},{"nodeID":1, "props.participant_id":1})
            # we assume this submission id is only associated with one study
            participant_list = []
            for item in query_return_list:
                item_id =  item["props"]["participant_id"]
                participant_list.append(item_id)
            return participant_list
        except errors.PyMongoError as pe:
            print(f"Failed to query particpant_id in dataRecords collection with submissionID: {submission_id}\n{repr(pe)}")
            return None
        except Exception as e:
            print(
                f"Failed to query particpant_id in dataRecords collection with submissionID: {submission_id}\n{repr(e)}"
            )
            return None

    def get_study_samples(self, submission_id: str) -> Union[dict, None]:
        """Returns a list of sample ids of a submission

        Args:
            submission_id (str): submissionID in "dataRecords" Collection or
            _id in "submissions" Collection. We assume only one study is associated with
            this submissionID

        Returns:
            list[str] | None: A list of dictionary with sample id as key and parent participant id as value
        """
        client = self._mongodb_client()
        db_name =  self._mongo_db_name()
        db = client[db_name]
        record_collection = db[self.datarecord_colleciton]
        try:
            query_return_list = record_collection.find(
                {"submissionID": submission_id, "nodeType": "sample"},
                {"nodeID": 1, "props.sample_id": 1, "parents": 1},
            )
            # we assume this submission id is only associated with one study
            sample_dict = dict()
            if record_collection.count_documents({"submissionID": submission_id, "nodeType": "sample"}) > 0:
                for item in query_return_list:
                    item_id = item["props"]["sample_id"]
                    item_parent = item["parents"][0]["parentIDValue"]
                    item_parent_query_response = record_collection.find(
                        {
                            "submissionID": submission_id,
                            "nodeType": "participant",
                            "nodeID": item_parent,
                        }
                    )
                    # we only expect one participant return in this case because one sample is most likely
                    # pointing to one participant instead of multiple
                    item_parent_id = item_parent_query_response[0]["props"][
                        "participant_id"
                    ]
                    sample_dict[item_id] =  item_parent_id
            else:
                pass
            return sample_dict
        except errors.PyMongoError as pe:
            print(
                f"Failed to query sample_id in dataRecords collection with submissionID: {submission_id}\n{repr(pe)}"
            )
            return None
        except Exception as e:
            print(
                f"Failed to query sample_id in dataRecords collection with submissionID: {submission_id}\n{repr(e)}"
            )
            return None
