# Data Loch

Data Loch scripts interact with [Instructure's Canvas Data service](https://community.canvaslms.com/community/answers/data), which is refreshed daily.

We store Canvas data in Amazon S3 for future processing. Steps:
1. download Canvas data
2. upload data to Amazon S3 (buckets are organized according to the DataLake design)
3. using Redshift Spectrum, create schemas/tables in AWS Glue data catalog.
4. Run queries against Redshift tables to prepare analytics data for ASC project

Amazon S3 is our storage layer, similar to a file system. The compute layers directly access and query
and recognize the schematics of the data. This eliminates the need having a long continuous running
compute cluster with storage capabilities. The external schemas created can be accessed via multiple
compute layers without duplication of data.

## Installation

### Build

```
# .nvmrc file has preferred Node version
nvm use
npm install
```

### Create and populate database

Create RedShift schema, download files from Canvas Data API and populate db.

```
node app
```

## Run

Refresh the DataLake views. The views give fast access to enrollment and assignment data.

1. Configure the cron-like job. The `dataLake.cronJob.cronTime` config must have a [valid cron pattern](http://crontab.org).
2. When the app starts it also configures and schedules cron tasks
