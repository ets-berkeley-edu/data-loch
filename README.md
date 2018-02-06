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

Refresh the DataLake views. These high-performance materialized views give applications access to analytics data.

1. Configure the cron-like job and the script to run. The `dataLake.cron.tasks` configs must have a [valid cron pattern](http://crontab.org) and the path to the cron script to run on trigger.
```
"cron": {
    "enabled": true,
    "tasks": {
      "syncDumps" : {
        "interval": "00 00 5 * * 1-5",  // Runs every weekday at 5 AM
        "script": "../store/syncDumps",
        "runOnInit": false
      }
    }
```
2. When the app starts it also configures and schedules cron tasks
3. Alternatively, the cron scripts can also be trigger externally via a REST API for ad-hoc runs.

## Cloud deployment notes
Refer [AWS deployment docs](docs/aws-deployment.md)
