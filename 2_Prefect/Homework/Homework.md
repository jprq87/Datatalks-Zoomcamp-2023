## Week 2 Homework

## Question 1. Load January 2020 data

>Command:

```
prefect deployment build ./Flows/etl_web_to_gcs.py:etl_web_to_gcs --name Homework_Q1 --apply
```

```
prefect deployment run etl-web-to-gcs/Homework_Q1
```
>Answer:
`447770`

## Question 2. Scheduling with Cron

>Command:
```
prefect deployment build ./Flows/etl_web_to_gcs.py:etl_web_to_gcs --name Homework_Q2 --cron "0 5 1 * *" --apply
```
>Answer:
`0 5 1 * *`

# Question 3. Loading data to BigQuery

>Command:
```
prefect deployment build ./Flows/etl_gcs_to_bq_homework.py:etl_parent_flow -n Homework_Q3 --apply
```
```
prefect deployment run etl-parent-flow/Homework_Q3 --param months=[2,3] --param year=2019 --param color=yellow
```
>Results:
```
Number of rows: 7832545
```
```
Number of rows: 7019375
```
>BigQuery:
```
SELECT COUNT(*) FROM `speedy-code-376706.de_zc_dataset.rides`
```
>Answer:
`14,851,920`

## Question 4. Github Storage Block
>Command:
```
prefect deployment build ./2_Prefect/Flows/etl_web_to_gcs_homework.py:etl_web_to_gcs --name Homework_Q4_Github_Flow --apply -sb github/github-block-jprq
```
```
prefect deployment run etl-web-to-gcs/Homework_Q4_Github_Flow
```
>Results:
```
rows: 88605
```
>Answer:
`88605`

## Question 5. Email or Slack notifications
>Create GCS Bucket on Prefect Cloud:
```
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

bucket_block = GcsBucket(
gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
bucket="de-zc-jprq-bucket"
)

bucket_block.save("zoom-gcs", overwrite=True)
```

>Recreate all blocks on Prefect Cloud:

![Prefect Cloud blocks](/Images/1_cloud_blocks.png)

>Set up notifications:

![Trigger](/Images/2_automations_trigger.png)

![Actions](/Images/2_automations_actions.png)

>Cloud login:
```
prefect cloud login -k 'Personal_api_key'
```
```
prefect agent start -q default
```
```
prefect deployment build ./2_Prefect/Flows/etl_web_to_gcs_homework_2.py:etl_web_to_gcs --name Homework_Q4_Github_Flow_Cloud --apply -sb github/github-block-jprq
```
```
prefect deployment run etl-web-to-gcs/Homework_Q4_Github_Flow_Cloud
```

>Results:
```
rows: 514392
```
>Notifications:

![Email](/Images/3_notification_email.png)

![Slack](/Images/3_notification_slack.png)

>Answer:
`514392`

## Question 6. Secrets

>Blocks > Secrets:

![Email](/Images/4_blocks_secrets.png)
>Answer:
`8`