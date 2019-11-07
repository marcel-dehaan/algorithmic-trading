


## Deploying functions

### uspr-ingest

```console
gcloud functions deploy uspr-ingest --region=us-east4 --source=uspr-ingest  --runtime=python37 \
    --service-account=functions-uspr-ingest@algo-trading-240010.iam.gserviceaccount.com --stage-bucket=trading-functions --memory=256MB \
    --trigger-bucket=naviga-staging --entry-point=streaming
```

**Verify that the fuctions was deployed**
```console

gcloud functions describe uspr-ingest  --region=us-east4 \
    --format="table[box](entryPoint, status, eventTrigger.eventType)"
```

gcloud functions deploy uspr-ingest --region=us-east4 --source=uspr-ingest  --runtime=python37 \
    --service-account=functions-uspr-ingest@algo-trading-240010.iam.gserviceaccount.com --stage-bucket=trading-functions --memory=256MB \
    --trigger-bucket=naviga-staging --entry-point=streaming