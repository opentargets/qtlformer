"""QTL manifest preparation tool."""

from __future__ import annotations
import typer

import logging
from typing import Annotated
from qtlformer.manifest import QTLManifest
from gentropy import Session
from qtlformer.transform import EqtlCatalogueTransformer
from qtlformer.load import GentropyDataset, result_coalescer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_path(value: str) -> str:
    if not isinstance(value, str) or value == "":
        raise typer.BadParameter("path must be valid path")
    return value


def validate_project_id(value: str) -> str:
    logger.debug(f"Validating project ID: {value}")
    if not isinstance(value, str) or value == "":
        raise typer.BadParameter("GCP project ID must be provided.")
    return value


cli = typer.Typer(no_args_is_help=True)


@cli.command()
def manifest(
    susie_path: Annotated[str, typer.Option(callback=validate_path)],
    sumstats_path: Annotated[str, typer.Option(callback=validate_path)],
    output_path: Annotated[str, typer.Option(callback=validate_path)],
) -> None:
    """Prepare QTL manifest from path and save as parquet."""

    logger.info("Starting QTL manifest preparation.")
    man = QTLManifest.from_path(sumstats_path, susie_path)
    man.log_statistics()
    man.to_parquet(output_path)
    logger.info("Reading blobs from source bucket.")


@cli.command()
def transform(
    metadata_path: Annotated[str, typer.Option(callback=validate_path)],
    lbf_path: Annotated[str, typer.Option(callback=validate_path)],
    cs_path: Annotated[str, typer.Option(callback=validate_path)],
    study_locus_path: Annotated[str, typer.Option(callback=validate_path)],
    study_index_path: Annotated[str, typer.Option(callback=validate_path)],
    sumstats_path: Annotated[str, typer.Option(callback=validate_path)],
) -> None:
    """Transform QTL SuSiE results to Gentropy StudyLocus and StudyIndex format."""

    Session(
        extended_spark_conf={
            "spark.driver.memory": "10G",
            "spark.executor.memory": "10G",
        },
        write_mode="overwrite",
    )

    logger.info("Starting SuSiE QTL transformation.")
    EqtlCatalogueTransformer(
        metadata_path=metadata_path,
        lbf_path=lbf_path,
        cs_path=cs_path,
        study_locus_out=study_locus_path,
        study_index_out=study_index_path,
        sumstats_path=sumstats_path,
    )
    logger.info("Completed SuSiE QTL transformation.")


@cli.command()
def load_study_index(
    study_index_glob: Annotated[str, typer.Option(callback=validate_path)],
    output_path: Annotated[str, typer.Option(callback=validate_path)],
) -> None:
    """Convert QTL manifest from path to study index parquet."""

    Session(
        extended_spark_conf={
            "spark.driver.memory": "10G",
            "spark.executor.memory": "10G",
        },
        write_mode="overwrite",
    )

    logger.info("Starting QTL study index loading.")
    result_coalescer(
        p=study_index_glob,
        dataset=GentropyDataset.STUDY_INDEX,
        coalesced_output=output_path,
    )
    logger.info("Completed QTL study index loading.")


@cli.command()
def load_study_locus(
    study_locus_glob: Annotated[str, typer.Option(callback=validate_path)],
    output_path: Annotated[str, typer.Option(callback=validate_path)],
) -> None:
    """Convert QTL manifest from path to study locus parquet."""
    Session(
        extended_spark_conf={
            "spark.driver.memory": "10G",
            "spark.executor.memory": "10G",
        },
        write_mode="overwrite",
    )

    logger.info("Starting QTL study locus loading.")
    result_coalescer(
        p=study_locus_glob,
        dataset=GentropyDataset.STUDY_LOCUS,
        coalesced_output=output_path,
    )
    logger.info("Completed QTL study locus loading.")
