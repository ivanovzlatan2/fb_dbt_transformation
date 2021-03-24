Welcome to your new dbt project!

### How to test

1. В папката за проекта трябва да се сложи : fb-api-bq-client-secrets.json
2. В C:\Users\USER\.dbt\profiles.yml трябва да се добави:

```
fb_tap:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: C:\dbt-fb\fb-tap\fb-api-bq-client-secrets.json # replace this with the full path to your keyfile
      project: fb-abi-tool 
      dataset: dev_transform 
      threads: 1
      timeout_seconds: 300
      location: US
      priority: interactive
```
