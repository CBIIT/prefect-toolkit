import requests
import re


class SstrHaul:
    """Class that fetches Patient ID and sample ID information
    of a dbGaP study using dbGaP accession number (phs id)
    and version number
    """

    def __init__(self, phs_accession: str, version_str:str = "0") -> None:
        self.base_url = "https://www.ncbi.nlm.nih.gov/gap/sstr/api/v1"
        self.phs_accession = phs_accession
        self.version_str = version_str
        self._validate_phs()
        self._validate_version()

    def _validate_phs(self) -> bool:
        """Validates phs_accession

        Raises:
            ValueError: Raises ValueError if phs_accession doesn't follow regular expression pattern

        Returns:
            bool: _description_

        Example of phs_accession: phs000123
        """        
        str_match = re.findall(r"^phs(\d{6})$", self.phs_accession)
        if len(str_match) == 0:
            raise ValueError(f"Invalid phs accession number detected: {self.phs_accession}")
        else:
            return True

    def _validate_version(self) -> bool:
        """Validates version str if it is a valid number or if the version is bigger 
        than the latest version found in dbGaP API

        Raises:
            ValueError: Error raised if invalid version number used

        Returns:
            bool: Returns True if validation passes

        Example of version_str: "2"
        """        
        try:
            int(self.version_str)
            latest_version_found = self.get_latest_version()
            if int(self.version_str) > latest_version_found:
                raise ValueError(f"Version in DB is bigger than the latest version vailable in dbGaP:\n- dbGaP latest version: {latest_version_found}\n- Submitted version in DB: {self.version_str}")
            else:
                pass
            return True
        except ValueError as err:
            raise ValueError(f"Invalid version number detected: {self.version_str}")

    def _get_response(self, request_url: str) -> dict:
        """Returns results of an url requests

        Args:
            request_url (str): A url

        Returns:
            dict: response of url request
        """        
        response = requests.get(request_url)
        return response.json()

    def _study_version_phrase(self) -> str:
        """Returns a phrase that can be appended to self.base_url

        Returns:
            str: A phrase that can be appended to self.base_url
        """
        if self.version_str == 0:
            return f"/study/{self.phs_accession}/subjects"
        else:
            return f"/study/{self.phs_accession}.v{self.version_str}/subjects"

    def get_latest_version(self) -> str:
        """Returns the latest version of a study from NCBI API

        Returns:
            str: A string of versioin number
        """
        study_request_url = self.base_url + f"/study/{self.phs_accession}/summary"
        request_response =  self._get_response(request_url=study_request_url)
        latest_version_available = request_response["study"]["accver"]["version"]
        return latest_version_available

    def get_participant_cnt(self) -> int:
        """Returns subject counts of a study of a specific version

        Returns:
            int: Number of participants
        """        
        study_request_url = self.base_url + self._study_version_phrase()
        request_response = self._get_response(request_url=study_request_url)
        subject_cnt = request_response["pagination"]["total"]
        del request_response
        return subject_cnt

    def get_study_participants(
        self
    ) -> dict:
        """Returns a dict of participant ids and their consent code

        Returns:
            dict: a dict of subjects and their content code of 
        a study (of a given version)
        """        
        study_request_url = self.base_url + self._study_version_phrase()
        subject_cnt = self.get_participant_cnt()
        # we default 25 subjects per page
        return_dict = dict()
        page_count = int(subject_cnt / 25) + 1
        for i in range(page_count):
            page = i + 1
            page_url = study_request_url + f"?page={page}&page_size=25"
            page_response = self._get_response(request_url=page_url)
            subjects_list = page_response["subjects"]
            for subject in subjects_list:
                subject_id = subject["submitted_subject_id"]
                consent_subject = subject["consent_code"]
                return_dict[subject_id] = consent_subject
        return return_dict

    def get_study_samples(self) -> dict:
        """Returns a dict of samples and the subjects they are associated
        with of a study (of a given version)

        Returns:
            dict: A dict of samples and the subjects
        """
        study_request_url = self.base_url + self._study_version_phrase()
        subject_cnt = self.get_participant_cnt()
        # we default 25 subjects per page
        return_dict = dict()
        page_count = int(subject_cnt / 25) + 1
        for i in range(page_count):
            page = i + 1
            page_url = study_request_url + f"?page={page}&page_size=25"
            page_response = self._get_response(request_url=page_url)
            subjects_list = page_response["subjects"]
            for subject in subjects_list:
                if "samples" in subject.keys():
                    sample_list = subject["samples"]
                    for sample in sample_list:
                        sample_id = sample["submitted_sample_id"]
                        return_dict[sample_id] =  subject["submitted_subject_id"]
                else:
                    pass
        return return_dict
