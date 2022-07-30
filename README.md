# dataflow-split-file

Split files by context using partition based pattern matching

## Run locally

> Note that requirements.txt does not contain anything because the dataflow base docker image already has apache-beam

```
pip install -r requirements-dev.txt
export INPUT=gs://rocketech-de-pgcp-sandbox-temp/dataflow-mixed-structure/
export OUTPUT=/tmp/beam-output/
export SCHEMA_IDENTIFIERS="gameNumber|baseball_schedules^station_id|austin_bikeshare_bikeshare_stations^complaint_description|austin_311_311_service_requests"

python main.py --input ${INPUT} --output ${OUTPUT} --schema_identifiers ${SCHEMA_IDENTIFIERS}
```

## Package as Dataflow Flex template

### Build docker image and push to Artifact Registry

```
export REGION=europe-west2
export PROJECT_ID=rocketech-de-pgcp-sandbox
export AF_REPOSITORY=demo

gcloud auth configure-docker ${REGION}-docker.pkg.dev

gcloud builds submit --tag ${REGION}-docker.pkg.dev/${PROJECT_ID}/${AF_REPOSITORY}/dataflow/dataflow-split-file:latest .
```

### Create Flex template

> The bucket name is hardcoded because the gcloud validation won't allow variables

```
gcloud dataflow flex-template build "gs://rocketech-de-pgcp-sandbox-temp/demo/dataflow/templates/dataflow-split-file.json" \
 --sdk-language "PYTHON" \
 --metadata-file "metadata.json" \
 --image "${REGION}-docker.pkg.dev/${PROJECT_ID}/${AF_REPOSITORY}/dataflow/dataflow-split-file:latest"   
```

## Run the Flex template

```
export DATETIME=`date +%Y%m%d-%H%M%S`
export BUCKET_NAME=rocketech-de-pgcp-sandbox-temp
export NETWORK=private
export SUBNETWORK=regions/${REGION}/subnetworks/dataflow
export SERVICE_ACCOUNT=dataflow@rocketech-de-pgcp-sandbox.iam.gserviceaccount.com

gcloud dataflow flex-template run "dataflow-split-file-${DATETIME}" \
    --template-file-gcs-location "gs://${BUCKET_NAME}/demo/dataflow/templates/dataflow-split-file.json" \
    --parameters input="${INPUT}" \
    --parameters output="gs://rocketech-de-pgcp-sandbox-temp/dataflow-output/${DATETIME}" \
    --parameters schema_identifiers="${SCHEMA_IDENTIFIERS}" \
    --region "${REGION}" \
    --network "${NETWORK}" \
    --subnetwork "${SUBNETWORK}" \
    --disable-public-ips \
    --temp-location "gs://${BUCKET_NAME}/temp" \
    --staging-location "gs://${BUCKET_NAME}/staging" \
    --service-account-email "${SERVICE_ACCOUNT}"
```