process buildManifest {
    container 'ghcr.io/project-defiant/qtlformer:0.2.2'
    machineType 'n1-standard-4'
    time '10m'
    debug true
    publishDir "${params.output_dir}", mode: 'copy', pattern: 'manifest.parquet'

    input:
    path input_path

    output:
    path 'manifest.parquet', emit: manifest_file

    script:
    """
    ls -R ${input_path} > output.parquet
    """
}
