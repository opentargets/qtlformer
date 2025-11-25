from __future__ import annotations
import logging
import re
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from fsspec import filesystem
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import pandas as pd
    from typing import Self


class DatasetOrStudyNameError(Exception):
    """Raised when dataset or study name is invalid."""

    pass


def validate_name(name: str, pattern: str) -> str:
    logging.debug(f"Validating dataset name: {name}")
    if not isinstance(name, str) or name == "":
        raise DatasetOrStudyNameError("Name has to be a non-empty string.")

    _match = re.compile(pattern).fullmatch(name)
    if not _match:
        raise DatasetOrStudyNameError(
            f"Name '{name}' does not match pattern '{pattern}'."
        )

    return name


@dataclass
class QTLDataset:
    """QTL Dataset representation.

    This class represents a single QTL dataset within a study, including paths to
    its associated SuSiE credible sets and lbf variable parquet files.

    NOTE: The paths are relative to the base path of the dataset.
    """

    id: str
    """Identifier of the QTL dataset."""
    susie_cs_path: str
    """Relative path to the SuSiE credible sets parquet file."""
    susie_lbf_path: str
    """Relative path to the SuSiE lbf variable parquet file."""
    sumstats_path: str
    """Relative path to the summary statistics file."""
    study: QTLStudy
    """Parent study of the dataset."""

    @staticmethod
    def _validate_name(name: str) -> str:
        """Validate dataset name against expected pattern."""
        logging.debug(f"Validating dataset name: {name}")
        return validate_name(name, r"^QTD\d+$")

    @classmethod
    def from_path(cls, ds_id: str, study: QTLStudy) -> QTLDataset | None:
        """Build QTLDataset from path.

        Args:
            path (str): The full path to the dataset directory.
            study (QTLStudy): The parent study of the dataset.

        Returns:
            QTLDataset | None: Returns QTLDataset instance if valid, else None.
        """
        try:
            dataset_id = cls._validate_name(ds_id)
        except DatasetOrStudyNameError:
            return None

        cs = f"{study.susie_path}/{dataset_id}/{dataset_id}.credible_sets.parquet"
        lbf = f"{study.susie_path}/{dataset_id}/{dataset_id}.lbf_variable.parquet"
        ssts = f"{study.sumstats_path}/{dataset_id}/{dataset_id}.cc.parquet"
        logging.info(
            f"Checking existence of SuSiE files for dataset '{dataset_id}': "
            f"CS path: {cs}, LBF path: {lbf}, Sumstats path: {ssts}"
        )
        fs = filesystem("local")
        if not fs.exists(ssts):
            logging.warning(
                f"Dataset '{dataset_id}' is missing required summary statistics file. Skipping."
            )
            return None
        if not fs.exists(cs) or not fs.exists(lbf):
            logging.warning(
                f"Dataset '{dataset_id}' is missing required SuSiE files. Skipping."
            )
            return None

        # Transform to relative paths to the input_path provided by the user
        logging.info("Transforming SuSiE paths relative to INPUT-PATH")
        # NOTE: Relative paths are required for nextflow mounts.
        susie_cs_path = "/".join(cs.split("/")[-4:])
        susie_lbf_path = "/".join(lbf.split("/")[-4:])
        sumstats_path = "/".join(ssts.split("/")[-4:])
        return cls(
            id=dataset_id,
            susie_cs_path=susie_cs_path,
            susie_lbf_path=susie_lbf_path,
            sumstats_path=sumstats_path,
            study=study,
        )


@dataclass
class QTLStudy:
    """QTL Study representation."""

    id: str
    """Id of the QTL study."""
    susie_path: str
    """Path to the directory containing susie results."""
    datasets: list[QTLDataset]
    """List of datasets within the study."""
    sumstats_path: str
    """Path to the directory containing summary statistics."""

    @classmethod
    def from_path(cls, sumstats_path: str, susie_path: str) -> QTLStudy:
        """Build QTLStudy from path.

        Args:
            path (str): The full path to the study directory.
        Returns:
            QTLStudy: Returns QTLStudy instance.
        """
        study_id = cls._validate_name(susie_path.split("/")[-1])
        study_id = cls._validate_name(sumstats_path.split("/")[-1])
        datasets = []
        return cls(
            id=study_id,
            susie_path=susie_path,
            datasets=datasets,
            sumstats_path=sumstats_path,
        )

    @staticmethod
    def _validate_name(name: str) -> str:
        logging.debug(f"Validating study name: {name}")
        return validate_name(name, r"^QTS\d+$")

    def get_datasets(self) -> Self:
        fs = filesystem("local")

        dataset_paths = fs.ls(self.susie_path)
        dataset_ids = [p.split("/")[-1] for p in dataset_paths]
        datasets = [QTLDataset.from_path(i, self) for i in dataset_ids]
        datasets = [ds for ds in datasets if ds is not None]
        self.datasets = datasets
        return self


@dataclass
class QTLManifest:
    studies: list[QTLStudy]

    def __post_init__(self):
        logging.debug(f"Initialized QTLManifest with {len(self.studies)} studies.")
        self.df = self.transform()

    @staticmethod
    def from_path(sumstats_path: str, susie_path: str) -> QTLManifest:
        fs = filesystem("local")
        studies: list[QTLStudy] = []
        susie_dataset_paths = fs.ls(susie_path)
        sumstats_dataset_paths = fs.ls(sumstats_path)

        def _prepare_study_for_path(ssts: str, susie: str) -> None | QTLStudy:
            try:
                study = QTLStudy.from_path(ssts, susie)
                datasets = study.get_datasets()
                return datasets
            except DatasetOrStudyNameError:
                logging.warning(f"Skipping invalid study path '{ssts}'local")
                return None

        logging.info(f"Found {len(susie_dataset_paths)} blobs in {susie_path}.")
        logging.info(f"Found {len(sumstats_dataset_paths)} blobs in {sumstats_path}.")
        assert len(susie_dataset_paths) == len(sumstats_dataset_paths), (
            "The number of SuSiE paths must match the number of summary statistics paths."
        )
        th = ThreadPoolExecutor(max_workers=5)
        result = list(
            th.map(_prepare_study_for_path, sumstats_dataset_paths, susie_dataset_paths)
        )
        studies = [study for study in result if study is not None]
        return QTLManifest(studies=studies)

    def transform(self) -> pd.DataFrame:
        logging.info("Transforming manifest to DataFrame...")
        records = []
        for study in self.studies:
            for dataset in study.datasets:
                records.append(
                    {
                        "study_id": study.id,
                        "dataset_id": dataset.id,
                        "susie_cs_path": dataset.susie_cs_path,
                        "susie_lbf_path": dataset.susie_lbf_path,
                        "sumstats_path": dataset.sumstats_path,
                    }
                )
        import pandas as pd

        df = pd.DataFrame.from_records(records)
        return df

    def log_statistics(self) -> None:
        logging.info("Logging manifest statistics...")
        logging.info(f"Total studies: {len(self.studies)}")
        logging.info(self.df)

    def to_parquet(self, output_path: str) -> None:
        logging.info(f"Writing manifest to {output_path} in Parquet format.")
        fs = filesystem("local")
        with fs.open(output_path, "wb") as f:
            self.df.to_csv(f, index=False, sep="\t", header=True)
        logging.info("Manifest successfully written.")
