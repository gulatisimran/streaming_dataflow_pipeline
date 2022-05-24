# Voice Dataflow Pipeline

This module contains code to run the streaming dataflow pipelines:
1. Generate mock JSON data using the *Streaming Data Generator* template and push it to a PubSub topic.
2. Ingest data generated by above pipeline from PubSub topic into BigQuery.

## Mock streaming data generator pipeline

### To run the pipeline for generating table data using gcloud command:
```bash
gcloud beta dataflow flex-template run voice-billing-mock-generator \
	--template-file-gcs-location gs://dataflow-templates-us-central1/latest/flex/Streaming_Data_Generator \
	--region us-central1 \
	--subnetwork SUBNETWORK \
	--service-account-email SERVICE_ACCOUNT \
	--parameters schemaLocation=SCHEMA_LOCATION,topic=TOPIC,qps=QPS
```
| Parameter | Description | Example |
| --- | ----------- | ---------- |
| SCHEMA_LOCATION | Location of the JSON file which has schema defined | gs://cx-{PROJECT_NUMBER}-data-billing-voice-test-mockup/random-voice-billing-generator.json |
| TOPIC | PubSub topic to push the generated data into | projects/{PROJECT_ID}/topics/voice-billing-mock-generate-data |
| QPS | Number of JSONs to be generated per second | 5 |
| SERVICE_ACCOUNT | Service account which runs the job | dataflow-job-compute-executor@{PROJECT_ID}.iam.gserviceaccount.com |
| SUBNETWORK | Subnetwork in which the job runs | regions/us-central1/subnetworks/{PROJECT_ID}-usce1 |

## PubSub to BigQuery  streaming pipeline

### To run the pipeline locally (using Direct Runner):
```bash
python ETL/voice_pipeline.py \
	--runner DirectRunner \
	--input_subscription INPUT_SUBSCRIPTION \
	--gcs_path GCS_PATH \
	--target_table TARGET_TABLE \
	--reject_table REJECT_TABLE \
	--timestamp_column TIMESTAMP_COLUMN \
	--partition_column PARTITION_COLUMN \
	--past_data_threshold PAST_DATA_THRESHOLD
```

### To run the pipeline on dataflow:
```bash
python ETL/voice_pipeline.py \
	--project PROJECT_ID \
	--region REGION \
	--runner DataflowRunner \
	--job_name voice-billing-pubsub-to-bq \
	--serviceAccount SERVICE_ACCOUNT \
	--subnetwork SUBNETWORK \
	--staging_location STAGING_LOCATION \
	--temp_location TEMP_LOCATION \
	--setup_file ETL/setup.py \
	--max_num_workers MAX_NUM_WORKERS \
	--input_subscription INPUT_SUBSCRIPTION \
	--gcs_path GCS_PATH \
	--target_table TARGET_TABLE \
	--reject_table REJECT_TABLE \
	--timestamp_column TIMESTAMP_COLUMN \
	--parition_column PARTITION_COLUMN \
	--past_data_threshold PAST_DATA_THRESHOLD
```
| Parameter | Description | Example |
| --- | ----------- | ---------- |
| INPUT_SUBSCRIPTION | PubSub subscription to read the JSONs from | projects/{PROJECT_ID}/subscriptions/voice-platform_voice-billing_v1_SUB_voice-billing-pubsub-to-bq_v1 |
| TARGET_TABLE | Full name of BQ's target table | {PROJECT_ID}:platform.voice_billing |
| REJECT_TABLE | Full name of BQ's reject table | {PROJECT_ID}:platform.voice_billing_rejected |
| TIMESTAMP_COLUMN | Timestamp column from which partition key is derived | session_start |
| PARTITION_COLUMN | Name of the partition column | session_start_date |
| PAST_DATA_THRESHOLD | Age in hours after which messages are rejected | 72 |
| PROJECT_ID | Project ID where the jobs needs to run | {PROJECT_ID} |
| REGION | Region | us-central1 |
| SERVICE_ACCOUNT | Service account which runs the job | dataflow-job-compute-executor@{PROJECT_ID}.iam.gserviceaccount.com |
| SUBNETWORK | Subnetwork in which the job runs | regions/us-central1/subnetworks/{PROJECT_ID}-usce1 |
| STAGING_LOCATION | GCS storage path for dataflow's staging files | gs://cx-{PROJECT_NUMBER}-data-staging/voice/staging |
| TEMP_LOCATION | GCS storage path for dataflow's temp files | gs://cx-{PROJECT_NUMBER}-data-staging/voice/temp |
| MAX_NUM_WORKERS | Max number of workers to be allocated for the job | 10 |
