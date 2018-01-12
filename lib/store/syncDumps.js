/**
* Copyright ©2017. The Regents of the University of California (Regents). All Rights Reserved.
*
* Permission to use, copy, modify, and distribute this software and its documentation
* for educational, research, and not-for-profit purposes, without fee and without a
* signed licensing agreement, is hereby granted, provided that the above copyright
* notice, this paragraph and the following two paragraphs appear in all copies,
* modifications, and distributions.
*
* Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
* Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
* http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.
*
* IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
* INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
* THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
* OF THE POSSIBILITY OF SUCH DAMAGE.
*
* REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
* IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
* SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
* "AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
* ENHANCEMENTS, OR MODIFICATIONS.
*/

var async = require('async');
var fs = require('fs');
var _ = require('lodash');
var request = require('request');
var url = require('url');
var util = require('util');
var config = require('config');
var AWS = require('aws-sdk');

var canvas = require('./canvas');
var log = require('../core/logger')('store');
var storage = require('./storage.js');

const AWS_DEFAULT_REGION = config.get('aws.credentials.region');
const KEY = config.get('aws.credentials.accessKeyId');
const SECRET = config.get('aws.credentials.secretAccessKey');

AWS.config.update({accessKeyId: KEY, secretAccessKey: SECRET});
AWS.config.region = AWS_DEFAULT_REGION;

const Lambda = new AWS.Lambda({apiVersion: config.get('datalake.worker.lambda.apiVersion')});

var argv = require('yargs')
  .usage('Usage: $0 --max-old-space-size=8192')
  .describe('u', 'Download the latest Canvas Redshift data files')
  .help('h')
  .alias('h', 'help')
  .argv;

/**
* Retrieve latest requests data dump details from canvas data API
*
* @param  {Object[]}           files                       The Canvas Redshift data files to download and unzip
* @param  {Function}           callback                    Standard callback function
* @param  {String[]}           callback.requestsFiles      The latest requests related file dump details
*/
var getLatestRequestFiles = function(callback) {
  var files = [];
  var requestsFiles = [];
  canvas.dataApiRequest('/file/latest', function(latestDump) {
    files = latestDump.artifactsByTable.requests.files;

    async.eachSeries(files, function(file, done) {
      var requestsFile = {
        filename: file.filename,
        table: 'requests',
        url: file.url
      };

      requestsFiles.push(requestsFile);
      return done();

    }, function() {
      log.info('Added all request files !');
      return callback(requestsFiles);

    });
  });
};

/**
* Invoked lambda function to upload to S3 with appropriate lambda parameters.
*
* @param  {Object}           message                   Message payload that will be used as event while invoking Lambda function
* @param  {Function}         callback                  Standard callback function
* @param  {Object}           callback.err              An error that occurred, if any
*/
var invokeLambdaWorker = module.exports.invokeLambdaWorker = function invokeLambdaWorker(file, callback) {
  const payload = JSON.stringify(file);

  // Invocation type should be set to event mode instead of request-response for asynchronous executions.
  const params = {
    FunctionName: config.get('datalake.worker.lambda.dataSyncService'),
    InvocationType: 'Event',
    Payload: new Buffer.from(payload)
  };

  log.info('Invoking lambda with message: ' + file.filename);

  Lambda.invoke(params, function(err) {
    var lambdaResponse = null;
    if (err) {
      log.error({err: err}, 'Unable to invoke the Lambda worker');
      return callback(err);
    }

    log.info('Lambda invoked sucesssfully. Requests is being processed.');
    return callback();

  });
};

/**
* Invoked data-loch worker function to upload to S3 with appropriate parameters.
*
* @param  {Object}           file                      Contains the data dump details that will be uploaded to S3 buckets
* @param  {Object}           path                      REST API path to invoke
* @param  {Function}         callback                  Standard callback function
* @param  {Object}           callback.err              An error that occurred, if any
*/
var cdpApiRequest = module.exports.cdpApiRequest = function(file, path, callback) {
  var domain = config.get('datalake.worker.beanstalk.host') + '/api';
  var url = util.format('%s%s', domain, path);
  var opts = {
    url: url,
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(file)
  };

  request.post(opts, function(err, response) {
    if (err) {
      log.error({err: err}, 'Failed to interact with the CDP REST API');
      return callback({code: 500, msg: 'Failed to interact with the CDP'});
    } else if (response.statusCode !== 200) {
      log.error({err: err, statusCode: response.statusCode}, 'CDP returned a non-200 status code');
      return callback({code: 500, msg: 'CDP returned a non-200 status code'});
    }

    return callback();
  });
};

/**
* Download the Canvas Redshift data files and then upload to Amazon S3
*
* @param  {Function}           callback                Standard callback function
*/
var migrateDataToS3 = function(callback) {
  // Get the list of files, tables and signed URLs that contain complete Canvas data snapshots
  canvas.dataApiRequest('/file/sync', function(fileDump) {
    var files = [];

    var fallRegExp = new RegExp('(.requests\/201708.)|(.requests\/201709.)|(.requests\/201710.)|(.requests\/201711.)|(.requests\/201712.)');
    _.each(fileDump.files, function(file) {
      if (file.table !== 'requests') {
        files.push(file);
      } else if (file.table === 'requests' && file.partial === true || fallRegExp.test(file.url)) {
        files.push(file);
      }
    });

    log.info('Number of files found: ' + files.length);
    var path = '/uploadFileToS3';

    async.eachSeries(files, function(file, done) {
      // Send a request to elastic beanstalk instance
      cdpApiRequest(file, path, function(err) {
        if (err) {
          log.error({file: file.filename}, 'Failed to get file from Canvas Data');
          return done();
        }

        log.info({file: file.filename}, 'Processing File upoad to S3');
        return done();
      });

    }, function() {
      log.info('Added all request files !');
      return callback();
    });
  });
};

/**
* Run job to sync and organize data dumps to S3 buckets
*
* @param  {Function}           callback                Standard callback function
* @param  {Object}             callback.err            An error that occurred, if any
*/
var run = module.exports.run = function(callback) {
  migrateDataToS3(function(err) {
    if (err) {
      log.info({err: err}, 'Failed to migrate data to S3');

      return callback(err);
    }

    log.info('Complete canvas data dump refresh. All done !');
    return callback();

  });
};
