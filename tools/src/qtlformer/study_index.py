"""Build studyIndex dataset."""

from __future__ import annotations
import polars as pl
import logging


from enum import StrEnum


class StudyType(StrEnum):
    BULK = "bulk"
    SINGLE_CELL = "single-cell"


class StudyIndex:
    @classmethod
    def from_source(metadata_uri: str, manifest_uri) -> StudyIndex:
        try:
            # Content from the bucket
            manifest = pl.read_csv(manifest_uri, separator="\t").select(
                "study_id",
                "dataset_id",
            )
        except Exception as e:
            logging.error("Failure parsing manifest_uri: %s", manifest_uri)
            raise e

        try:
            # Metadata from qtlmap pipeline
            # Expected file like
            metadata = pl.read_csv(metadata_uri, separator="\t").select(
                "study_id",
                "dataset_id",
                "study_label",
                "sample_group",
                "tissue_id",
                "tissue_label",
                "condition_label",
                "sample_size",
                "quant_method",
                "pmid",
                "study_type",
            )
        except Exception as e:
            logging.error("Failure parsing metadata_uri: %s", metadata_uri)
            raise e

        # Since we just want to include the studies found in the manifest, but metadata
        # contains entire set, we need to subset the metadata to the `study_id(s)` in manifest.
        full_manifest = manifest.join(
            metadata, on=["study_id", "dataset_id"], how="inner"
        ).unique(subset=["study_id", "dataset_id"])

        # Sanity check, we do not lose any existing study
        assert full_manifest.shape[0] == manifest.shape[0], (
            "Manifest size after joining with metadata changed."
        )

        study_index = full_manifest.select(
            pl.col("study_id").alias("studyId"), pl.col("project_id")
        )
