nextflow.enable.dsl = 2

include { buildManifest } from './modules/build-manifest.nf'

def intro() {
    log.info(
        """
        QTLFormer Nextflow Pipeline
    """.stripIndent()
    )
}

/*
 * SET UP CONFIGURATION VARIABLES
 */


workflow {

    intro()
    print(params)
    input_ch = channel.fromPath(params.input_dir)
    manifest_ch = buildManifest(input_ch)
    // Transform the channel to [meta, cs_path, lbf_path]
    datasets = manifest_ch.splitCsv(sep: '\t', header: true)
        | map { r ->
            [
                [
                    id: "${r.study_id}_${r.dataset_id}",
                    study_id: r.study_id,
                    dataset_id: r.dataset_id,
                ],
                file(r.susie_cs_path),
                file(r.susie_lbf_path),
            ]
        }
    datasets.view()



    workflow.onComplete { log.info("Pipeline complete!") }
}
