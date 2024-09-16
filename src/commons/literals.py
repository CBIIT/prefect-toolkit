from dataclasses import dataclass, field

@dataclass
class CrdcDHMongoSecrets:
    """A class stores constants related to MongoDB of CRDCDH
    """
    secret_name = "bento/crdc-hub/dev2"
    # CRDC DataHub Mongo DB collection names
    submission_collection = "submissions"
    datarecord_colleciton = "dataRecords"

@dataclass
class CommonsRepo:
    icdc: dict = field(
        default_factory=lambda: {
            "repo": "https://github.com/CBIIT/icdc-model-tool",
            "model_yaml": "model-desc/icdc-model.yml",
            "props_yaml": "model-desc/icdc-model-props.yml",
            "tags_api": "https://api.github.com/repos/CBIIT/icdc-model-tool/tags",
            "master_zipball": "https://github.com/CBIIT/icdc-model-tool/archive/refs/heads/main.zip",
        }
    )
    ccdi: dict = field(
        default_factory=lambda: {
            "repo": "https://github.com/CBIIT/ccdi-model",
            "model_yaml": "model-desc/ccdi-model.yml",
            "props_yaml": "model-desc/ccdi-model-props.yml",
            "tags_api": "https://api.github.com/repos/CBIIT/ccdi-model/tags",
            "master_zipball": "https://github.com/CBIIT/ccdi-model/archive/refs/heads/main.zip",
        }
    )
    cds: dict = field(
        default_factory=lambda: {
            "repo": "https://github.com/CBIIT/cds-model",
            "model_yaml": "model-desc/cds-model.yml",
            "props_yaml": "model-desc/cds-model-props.yml",
            "tags_api": "https://api.github.com/repos/CBIIT/cds-model/tags",
            "master_zipball": "https://github.com/CBIIT/cds-model/archive/refs/heads/main.zip",
        }
    )
    c3dc: dict = field(
        default_factory=lambda: {
            "repo": "https://github.com/CBIIT/c3dc-model",
            "model_yaml": "model-desc/c3dc-model.yml",
            "props_yaml": "model-desc/c3dc-model-props.yml",
            "tags_api": "https://api.github.com/repos/CBIIT/c3dc-model/tags",
            "master_zipball": "https://github.com/CBIIT/c3dc-model/archive/refs/heads/main.zip",
        }
    )

@dataclass
class CommonsFeat:
    icdc: dict = field(
        default_factory = lambda: {
            "delimiter": ";"
        }
    )
    ccdi: dict = field(default_factory=lambda: {"delimiter": ";"})
    cds: dict = field(default_factory=lambda: {"delimiter": "|"})
    c3dc: dict = field(default_factory=lambda: {"delimiter": ";"})
