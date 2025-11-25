nextflow.enable.dsl = 2

include { Manifest } from './modules/manifest.nf'

def intro() {
    log.info(
        """
        QTLFormer Nextflow Pipeline

        Using:

        Susie directory:    ${params.susie_dir}
        Sumstats directory: ${params.sumstats_dir}
        Output directory:   ${params.output_dir}

    """.stripIndent()
    )
}

/*
 * SET UP CONFIGURATION VARIABLES
 */


workflow {

    intro()
    print(params)
    susie_ch = channel.fromPath(params.susie_dir)
    sumstats_ch = channel.fromPath(params.sumstats_dir)
    // Order of the channel [susie_path, sumstats_path]
    input_ch = susie_ch.mix(sumstats_ch)
    manifest_ch = Manifest(input_ch)
    // Transform the channel to [meta, cs_path, lbf_path]
    datasets = manifest_ch.splitCsv(sep: '\t', header: true)
        | map { r ->
            [
                [
                    id: "${r.study_id}_${r.dataset_id}",
                    study_id: r.study_id,
                    dataset_id: r.dataset_id,
                    sumstats_path: "${params.sumstats_dir}/${r.sumstats_path}",
                ],
                file("${params.susie_dir}/${r.susie_cs_path}"),
                file("${params.susie_dir}/${r.susie_lbf_path}"),
            ]
        }
    datasets.view()



    workflow.onComplete { log.info("Pipeline complete!") }
}
