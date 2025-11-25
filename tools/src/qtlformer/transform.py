"""Transform SuSiE results to StudyLocus and StudyIndex format."""

from __future__ import annotations

from gentropy.datasource.eqtl_catalogue.finemapping import EqtlCatalogueFinemapping
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex
from gentropy.config import EqtlCatalogueConfig
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as f
from pyspark.sql import types as t
from gentropy import Session


class EqtlCatalogueTransformer:
    """Transform SuSiE results to Gentropy EqtlCatalogue format."""

    STUDY_LABEL_UPDATE_DICT = {
        "GTEx": "GTEx-v10",
    }

    def __init__(
        self,
        metadata_path: str,
        lbf_path: str,
        cs_path: str,
        study_locus_out: str,
        study_index_out: str,
        sumstats_path: str,
    ) -> None:
        """Initialize transformer and execute transformation.

        Args:
            metadata_path (str): Path to study metadata.
            lbf_path (str): Path to SuSiE log Bayes factor results.
            cs_path (str): Path to SuSiE credible set results.
            study_locus_out (str): Output path for StudyLocus parquet.
            study_index_out (str): Output path for StudyIndex parquet.
            sumstats_path (str): Path to summary statistics.

        NOTE: the sumstats_path is not used in the transformation itself, but is
              added to the StudyLocus dataframe as a column.
        """
        config = EqtlCatalogueConfig()
        session = Session.find()
        metadata = self.transform_metadata(metadata_path)
        fm = self.transform_fm(cs_path, lbf_path, metadata, sumstats_path)
        (
            EqtlCatalogueFinemapping.from_susie_results(fm)
            .validate_lead_pvalue(pvalue_cutoff=config.eqtl_lead_pvalue_threshold)
            .df.write.mode(session.write_mode)
            .parquet(study_locus_out)
        )
        (
            EqtlCatalogueStudyIndex.from_susie_results(fm)
            .df.coalesce(1)
            .write.mode(session.write_mode)
            .parquet(study_index_out)
        )

    @classmethod
    def transform_metadata(cls, metadata_path: str) -> DataFrame:
        """Transform study metadata to study index format.

        Returns:
            DataFrame: transformed study index metadata.

        NOTE: study_label is overwritten for all studies using the dict
        """

        return EqtlCatalogueStudyIndex.read_studies_from_source(
            metadata_path, list()
        ).withColumn("study_label", cls.update_study_label(f.col("study_label")))

    @classmethod
    def update_study_label(cls, study_label: Column) -> Column:
        """Update study label based on predefined mapping.

        Args:
            study_label (Column): Original study label column.

        Returns:
            Column: Updated study label column.

        NOTE: due to the fact that eQTL catalogue do not update study labels,
        we need to make sure we transform study labels to be able to distinguish
        between GTEx v8 and v10.
        """

        expr = f.when(f.lit(False), f.lit(None).cast(t.StringType()))

        for key, value in cls.STUDY_LABEL_UPDATE_DICT.items():
            expr = expr.when(study_label == key, f.lit(value))

        expr = expr.otherwise(study_label)
        return expr

    @classmethod
    def transform_fm(
        cls,
        cs_path: str,
        lbf_path: str,
        studies_metadata: DataFrame,
        sumstats_path: str,
    ) -> DataFrame:
        """Transform SuSiE results to finemapping format.

        Args:
            cs_path (str): Path to SuSiE credible set results.
            lbf_path (str): Path to SuSiE log Bayes factor results.
            studies_metadata (DataFrame): Transformed study metadata.
            sumstats_path (str): Path to summary statistics.

        Returns:
            DataFrame: Transformed finemapping results.
        """

        credible_sets_df = EqtlCatalogueFinemapping.read_credible_set_from_source(
            cs_path
        )
        lbf_df = EqtlCatalogueFinemapping.read_lbf_from_source(lbf_path)

        return EqtlCatalogueFinemapping.parse_susie_results(
            credible_sets_df, lbf_df, studies_metadata
        ).withColumn("summarystatsLocation", f.lit(sumstats_path))
