process buildManifest {
    label 'regular'
    publishDir "${params.output_dir}", mode: 'copy', pattern: 'manifest.tsv'

    input:
    tuple path(lbf_path), path(cs_path)
    val metadata

    output:
    path

    script:
    """
    qtlformer transform \
        --metadata-path ${metadata} \
        --lbf-path ${lbf_path} \
        --cs-path ${cs_path} \
        --study-locus-path ${study_locus_path} \
        --study-index-path ${study_index_path} \
        --sumstats-path ${sumstats_path}
    """
}
