process Manifest {
  label 'regular'
  publishDir "${params.output_dir}", mode: 'copy', pattern: 'manifest.tsv'

  input:
  tuple path(susie_path), path(sumstats_path)

  output:
  path 'manifest.tsv', emit: manifest_file

  script:
  """
    qtlformer manifest \
      --susie-path ${susie_path} \
      --sumstats-path ${sumstats_path} \
      --output-path manifest.tsv
    """

  stub:
  """
    touch manifest.tsv
    """
}
