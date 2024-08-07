from dataclasses import dataclass

@dataclass
class CrdcDHMongoDB:
    """A class stores constants related to MongoDB of CRDCDH
    """
    secret_name = "bento/crdc-hub/dev2"
    # CRDC DataHub Mongo DB collection names
    submission_collection = "submissions"
    datarecord_colleciton = "dataRecords"


