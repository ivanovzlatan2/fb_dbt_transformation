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
      project: fb-abi-tool # Replace this with your project id
      dataset: dev_transform # Replace this with dbt_your_name, e.g. dbt_bob
      threads: 1
      timeout_seconds: 300
      location: US
      priority: interactive
```


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
