# canvas-data-processor

Canvas data processor consists of scripts that can be used to interact with Instructure's hosted service called Canvas-data. Instructure provides it's clients access to the canvas data dumps which are refreshed on a nightly basis.

The scripts attempts to restore canvas data dumps in AWS environment for further processing. The following actions are performed
1. Access canvas data dumps and upload it to targetted S3 bucket locations
2. Organize the s3 bucket location so that it can be used as a data lrs-data-lake
3. Create external schemas/tables in Athena data catalog using Redshift Spectrum

The main benefit of the process is that S3 will be used as Storage layer and acts as the file system. The compute layers can directly access and query
and recognize the schematics of the data. This eliminates the need having a long continuous running compute cluster with storage capabilities.
The external schemas created can be accessed via multiple compute layers without duplication of data.

## Deployment

## Install aws cli and eb cli.

```
nvm use 6.10
npm install
node scripts/syncToS3.js  
```

## Note
Canvas data processor also houses other scripts that can be used to download the canvas data dumps on to the local system and translate the
activities into xAPI and Caliper formats easily.
