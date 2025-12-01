"""Test load command."""

from typer.testing import CliRunner
from qtlformer import cli
from pathlib import Path
import pandas as pd
from unittest.mock import patch
from gentropy import Session
from pyspark.sql import functions as f
from pyspark.sql import types as t


class TestApplication:
    """Test Application."""

    runner = CliRunner()

    def test_manifest_command_help(self):
        """Test the load command."""
        result = self.runner.invoke(cli, ["manifest", "--help"])
        assert result.exit_code == 0, result.output

    def test_transform_command_help(self):
        """Test the transform command."""
        result = self.runner.invoke(cli, ["transform", "--help"])
        assert result.exit_code == 0, result.output

    def test_manifest_command_with_data(self, testdata_path: Path, tmp_path: Path):
        """Test the manifest command with data."""
        output_path = (tmp_path / "qtl_manifest.tsv").as_posix()
        result = self.runner.invoke(
            cli,
            [
                "manifest",
                "--sumstats-path",
                (testdata_path / "sumstats").as_posix(),
                "--susie-path",
                (testdata_path / "susie").as_posix(),
                "--output-path",
                output_path,
            ],
        )
        assert result.exit_code == 0, result.output
        assert Path(output_path).exists(), "Output file was not created."
        df = pd.read_csv(output_path, sep="\t")
        assert not df.empty, "Output DataFrame is empty."
        assert "study_id" in df.columns, "study_id column is missing."
        assert "dataset_id" in df.columns, "dataset_id column is missing."
        assert "susie_cs_path" in df.columns, "susie_cs_path column is missing."
        assert "susie_lbf_path" in df.columns, "susie_lbf_path column is missing."
        assert "sumstats_path" in df.columns, "sumstats_path column is missing."
        assert len(df) == 2, "Expected 2 rows in the DataFrame."

    @patch("qtlformer.transform.EqtlCatalogueTransformer.transform_metadata")
    def test_transform_command_with_data(
        self, mock_transform_metadata, testdata_path: Path, tmp_path: Path
    ):
        """Test the transform command with data."""
        session = Session()
        mock_transform_metadata.return_value = session.spark.createDataFrame(
            pd.DataFrame(
                {
                    "study_id": ["QTS000001", "QTS000001"],
                    "dataset_id": ["QTD000001", "QTD000002"],
                    "study_label": ["GTEx", "GTEx"],
                    "sample_group": ["A", "A"],
                    "tissue_id": ["CL_0000235", "CL_0000235"],
                    "tissue_label": ["A", "A"],
                    "condition_label": ["naive", "naive"],
                    "sample_size": [100, 100],
                    "quant_method": ["exon", "ge"],
                    "pmid": ["12345678", "12345678"],
                    "study_type": ["bulk", "bulk"],
                }
            )
        ).withColumns(
            {
                "sample_size": f.col("sample_size").cast(t.IntegerType()),
            }
        )
        lbf_path = (
            testdata_path
            / "susie"
            / "QTS000001"
            / "QTD000001"
            / "QTD000001.lbf_variable.parquet"
        ).as_posix()

        cs_path = (
            testdata_path
            / "susie"
            / "QTS000001"
            / "QTD000001"
            / "QTD000001.credible_sets.parquet"
        ).as_posix()

        sumstats_path = (
            testdata_path
            / "sumstats"
            / "QTS000001"
            / "QTD000001"
            / "QTD000001.cc.parquet"
        ).as_posix()

        assert Path(lbf_path).exists(), "LBF path does not exist."
        assert Path(cs_path).exists(), "CS path does not exist."
        assert Path(sumstats_path).exists(), "Sumstats path does not exist."

        si_output_path = (tmp_path / "study_index.parquet").as_posix()
        sl_output_path = (tmp_path / "study_locus.parquet").as_posix()

        result = self.runner.invoke(
            cli,
            [
                "transform",
                "--metadata-path",
                "https://example.com/metadata.tsv",
                "--lbf-path",
                lbf_path,
                "--cs-path",
                cs_path,
                "--study-locus-path",
                sl_output_path,
                "--study-index-path",
                si_output_path,
                "--sumstats-path",
                sumstats_path,
            ],
        )
        assert result.exit_code == 0, result.output

        assert Path(si_output_path).exists(), "StudyIndex output file was not created."
        assert Path(sl_output_path).exists(), "StudyLocus output file was not created."
        assert Path(si_output_path).is_dir(), (
            "StudyIndex output path is not a directory."
        )
        assert Path(sl_output_path).is_dir(), (
            "StudyLocus output path is not a directory."
        )
        si_df = session.spark.read.parquet(si_output_path).toPandas()
        sl_df = session.spark.read.parquet(sl_output_path).toPandas()

        assert not si_df.empty, "StudyIndex DataFrame is empty."
        assert not sl_df.empty, "StudyLocus DataFrame is empty."
        assert sl_df.shape[0] == 4, "Expected 4 rows in StudyLocus DataFrame."
        assert si_df.shape[0] == 4, "Expected 4 rows in StudyIndex DataFrame."
