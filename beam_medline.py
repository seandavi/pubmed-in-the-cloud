from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import apache_beam as beam
import pubmed_parser as p
import httpx
import orjson
from datetime import datetime
import logging
import argparse
from bs4 import BeautifulSoup

logger = logging.getLogger()


PUBMED_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed"


def get_file_list():
    links = []
    for i in ["baseline", "updatefiles"]:
        res = httpx.get(f"{PUBMED_BASE_URL}/{i}/")
        b = BeautifulSoup(res.text)
        n = 0
        for link in b.find_all(href=True):
            href = link.get("href")
            if href.startswith("pubmed"):
                if href.endswith("gz"):
                    links.append(f"{PUBMED_BASE_URL}/{i}/{href}")
    return links


def process_pubmed(element):
    resp = httpx.get(element, timeout=120)
    import gzip

    z = gzip.decompress(resp.content)
    d = p.parse_medline_xml(z)
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

        links = pipeline | "get_file_list" >> beam.Create(get_file_list())
        links | beam.io.WriteToText(
            known_args.output + "_manifest", num_shards=1, shard_name_template=""
        )
        res = (
            links
            | "process_pubmed" >> beam.FlatMap(process_pubmed)
            | "json" >> beam.Map(lambda x: orjson.dumps(x))
            | "write" >> beam.io.WriteToText(known_args.output)
        )


if __name__ == "__main__":
    main()
