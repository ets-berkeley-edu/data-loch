# SuiteC de-identified data extracts for Researchers

The suitec-analytic app accepts canvas_course_ids as inputs from researchers,
restores suitec database on data-loch, de-identifies and exports datasets (suitec & canvas data) for research use.

## Pre-requisites
1. Check if data-loch has completed it's daily restore of canvas-data data warehouse on redshift
2. Have a list of canvas course ids for which researchers require data extracts
3. Prepare a csv file having following information without headers. Refer existing suitec_data_access_dict on data-loch for format references.
  (Available at the following location s3://<data-loch-bucket>/suiteC/dictionary/)

```
<id>,<canvas_course_id>,<research group name>,<request_date>
<id>,<canvas_course_id>,<research group name>,<request_date>
<id>,<canvas_course_id>,<research group name>,<request_date>
```

## Data Extraction steps
The document outlines step to generate de-identified datasets for suitec and canvas data
based on researchers requests. Steps:

1. Append the prepared data access dictionary csv file to the csv file on data-loch.
Include the canvas course ids and research group requesting the data extract.
```
mkdir -p ~/dictionary
aws s3 cp s3://<data-loch-bucket>/suiteC/dictionary/  ~/dictionary --recursive
```

2. Update the dictionary csv file and upload it to the s3 dictionary location on data-loch
```
aws s3 cp ~/dictionary/ s3://<data-loch-bucket>/suiteC/dictionary/ --recursive --sse
```

3. Run the bash script syncSuitecToS3 to restore suitec data in data-loch. The script has a synopsis and
description on required parameters.

The bash script downloads suitec tables from suitec-prod database and
organizes the exports in accordance to data-loch external table structures. The entire format is then
copied to s3://<data-loch-bucket>/<suitec-location> automatically.
```
bash scripts/suitec-analytics/migrateSuitecToLoch.sh
```

4. Run node script app.js to generate analytics tables on redshift and de-identify datasets based on research criteria. These tables are
then unloaded to S3 export location for suitec. Usually in the following location s3://<data-loch-bucket>/suiteC/export/<researcher_name>/<extract-date>/
```
nvm use
npm install
node scripts/suitec-analytics/app.js
```

5. Files can be downloaded to local folder using aws s3 cp command. Verify and upload the files to Box folder of the researchers
```
mkdir -p ~/data-export
aws s3 cp s3://<data-loch-bucket>/suiteC/export/<researcher_name>/<extract-date>/ ~/data-export --recursive

# Clean up local drives of sensitive data
rm -rf ~/suitec-data
rm -rf ~/data-export
rm -rf ~/dictionary
```

[SuiteC Data definitions](https://docs.google.com/document/d/1NGGrTBSt5Y9SaWxHy58_hd8zgbGRYa2CtjB5hwj-jbM/edit?usp=sharing) can be found in the Google Doc
