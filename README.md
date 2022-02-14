# Pubmed and Medline to the (Google) cloud

- Apache Beam and Dataflow
- pubmed_parser

## Medline

Includes:

- baseline
- updatefiles

For medline, the entire file is read into memory, so need to specify machine type.

```sh
python beam_medline.py \
  --output gs://pubmed-bucket-123/medline/2022-02-13/medline.json.gz --staging_location=gs://skdfsd/staging \
  --temp_location=gs://skdfsd/temp \
  --runner=DataflowRunner \
  --region=us-central1 \
  --project=omicidx-338300 \
  --requirements_file requirements.txt \
  --worker_machine_type n1-highmem-2
```

## Pubmed OpenAccess

```sh
python beam_pubmed_openaccess.py \
  --output gs://pubmed-bucket-123/medline/2022-02-13/openaccess.json.gz --staging_location=gs://skdfsd/staging \
  --temp_location=gs://skdfsd/temp \
  --runner=DataflowRunner \
  --region=us-central1 \
  --project=omicidx-338300 \
  --requirements_file requirements.txt
```
