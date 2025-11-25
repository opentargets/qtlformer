process Transform {
    label 'regular', 'copy'

    publishDir "${params.output_dir}", mode: 'copy', pattern: 'study_locus/*'
    publishDir "${params.output_dir}", mode: 'copy', pattern: 'study_index/*'

    input:
    tuple val(meta), path(cs_path), path(lbf_path)
    val metadata

    output:
    tuple val(meta), path(cs_path), path(lbf_path), emit: manifest
    path ("study_locus/*"), emit: study_locus
    path ("study_index/*"), emit: study_index

    script:
    """
    qtlformer transform \
        --metadata-path ${metadata} \
        --lbf-path ${lbf_path} \
        --cs-path ${cs_path} \
        --study-locus-path study_locus/${meta.dataset_id} \
        --study-index-path study_index/${meta.dataset_id} \
        --sumstats-path ${meta.sumstats_path}
    """
}
