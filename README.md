# Canvas Data Processor

Canvas Data Processor scripts interact with [Instructure's Canvas Data service](https://community.canvaslms.com/community/answers/data), which is refreshed daily.

We store Canvas data in Amazon S3 for future processing. Steps:
1. download Canvas data
2. upload data to Amazon S3 (buckets are organized according to the LRS DataLake design)
3. using Redshift Spectrum, create schemas/tables in Amazon Athena (data catalog)

Amazon S3 is our storage layer, similar to a file system. The compute layers directly access and query
and recognize the schematics of the data. This eliminates the need having a long continuous running
compute cluster with storage capabilities. The external schemas created can be accessed via multiple
compute layers without duplication of data.

## Installation

### Create database

```
createuser data_processor --no-createdb --no-superuser --no-createrole --pwprompt
createdb lakeview --owner=data_processor
psql lakeview -U data_processor -f scripts/db/schema.sql
```

### Build

```
# .nvmrc file has preferred Node version
nvm use
npm install
```

## Run

### Sync DataLake with contents of Canvas Data API

`node scripts/syncToS3.js`

### Refresh the DataLake views. The views give fast access to enrollment and assignment data.

1. Configure the cron-like job. The `dataLake.cronJob.interval` config must have a [valid cron pattern](http://crontab.org).
1. Start the cron job with:
```
NODE_ENV=production node ./scripts/startCron.js
```

## Note

Canvas Data Processor also includes scripts that:
* download Canvas data dumps to your local filesystem
* translate activities into xAPI and Caliper formats
