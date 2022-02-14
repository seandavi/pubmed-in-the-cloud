from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import apache_beam as beam
import pubmed_parser as p
import httpx
import orjson
from datetime import datetime
import logging
import argparse

logger = logging.getLogger()

input_files = [
    "https://pmc-oa-opendata.s3.amazonaws.com/oa_noncomm/xml/metadata/csv/oa_noncomm.filelist.csv",
    "https://pmc-oa-opendata.s3.amazonaws.com/oa_comm/xml/metadata/csv/oa_comm.filelist.csv",
]


PMC_BASE_URL = "https://pmc-oa-opendata.s3.amazonaws.com/"


def get_file_list(fname):
    headers = "key etag".split()
    with httpx.stream("GET", fname) as r:
        logger.info(fname)
        for line in r.iter_lines():
            if line.startswith("Key"):
                continue
            retval = dict(zip(headers, line.strip().split(",")[0:2]))
            yield retval


def process_pubmed(element):
    resp = httpx.get(
        f"https://pmc-oa-opendata.s3.amazonaws.com/{element['key']}", timeout=120
    )
    d = p.parse_pubmed_xml(resp.text)
    d["paragraphs"] = p.parse_pubmed_paragraph(resp.text)
    d["citations"] = p.parse_pubmed_references(resp.text)
    d["oa_metadata"] = element
    del d["author_list"]
    del d["affiliation_list"]
    return d


def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        dest="output",
        # CHANGE 1/6: The Google Cloud Storage path is required
        # for outputting the results.
        default="gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX",
        help="Output file to write results to.",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as pipeline:

        res = (
            pipeline
            | "create" >> beam.Create(input_files)
            | "get_file_list" >> beam.FlatMap(get_file_list)
            | "avoid fusion -> reshuffle" >> beam.Reshuffle()
            | "process_pubmed" >> beam.Map(process_pubmed)
            | "json" >> beam.Map(lambda x: orjson.dumps(x))
            | "write" >> beam.io.WriteToText(known_args.output)
        )


if __name__ == "__main__":
    main()
