nextflow.enable.dsl = 2

include { Manifest  } from './modules/manifest.nf'
include { Transform } from './modules/transform.nf'


/*
    Intro function to log parameters
*/
def intro() {
    log.info(
        """
        QTLFormer Nextflow Pipeline

        Using:

        Susie directory:    ${params.susie_dir}
        Sumstats directory: ${params.sumstats_dir}
        Output directory:   ${params.output_dir}
        Metadata:           ${params.metadata}

    """.stripIndent()
    )
}


/*
    QTLFormer workflow
    NOTE: the distinction of this workflow is to allow testing with nf-test
*/
workflow qtlformer {

    susie_ch = channel.fromPath(params.susie_dir)
    sumstats_ch = channel.fromPath(params.sumstats_dir)
    // Order of the channel [susie_path, sumstats_path]
    input_ch = susie_ch.combine(sumstats_ch)
    manifest_ch = Manifest(input_ch)
    // Transform the channel to [meta, cs_path, lbf_path]
    base_dir = params.susie_dir.toString().replaceAll("/susie\$", "")
    dataset_ch = manifest_ch.splitCsv(sep: '\t', header: true)
        | map { r ->
            [
                [
                    id: "${r.study_id}_${r.dataset_id}",
                    study_id: r.study_id,
                    dataset_id: r.dataset_id,
                    sumstats_path: "${base_dir}/${r.sumstats_path}",
                ],
                file("${base_dir}/${r.susie_cs_path}"),
                file("${base_dir}/${r.susie_lbf_path}"),
            ]
        }
    Transform(dataset_ch, params.metadata)
}


/*
    Main workflow
*/
workflow  {

    intro()
    qtlformer()
    workflow.onComplete { log.info("Pipeline complete!") }
}
