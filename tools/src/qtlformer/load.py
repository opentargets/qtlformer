"""Load the Gentropy datasets and coalesce the result partition files."""

from enum import Enum
from gentropy import Session, StudyIndex, StudyLocus


class GentropyDataset(Enum):
    """Dataset types supported by coalescer."""

    STUDY_INDEX = "study_index"
    STUDY_LOCUS = "study_locus"


def result_coalescer(p: str, dataset: GentropyDataset, coalesced_output: str) -> None:
    """Coalesce per-study partitioned datasets into Spark datasets.

    Args:
        p (str): Path to partitioned dataset.
        dataset (GentropyDataset): Type of Gentropy dataset.

        coalesced_output (str): Output path for coalesced dataset.
    """
    session = Session.find()

    match dataset:
        case GentropyDataset.STUDY_INDEX:
            df = StudyIndex.from_parquet(
                session, p, recursiveFileLookup=True
            ).df.repartition(1)
        case GentropyDataset.STUDY_LOCUS:
            df = StudyLocus.from_parquet(session, p, recursiveFileLookup=True).df
        case _:
            raise ValueError(f"Unsupported dataset type: {dataset}")

    df.write.mode(session.write_mode).parquet(coalesced_output)
