process buildManifest {
    label 'regular'
    publishDir "${params.output_dir}", mode: 'copy', pattern: 'manifest.tsv'

    input:
    path input_path

    output:
    path 'manifest.tsv', emit: manifest_file

    script:
    """
    qtlformer manifest ${input_path} manifest.tsv
    """
}
