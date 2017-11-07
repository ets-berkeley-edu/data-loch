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

var _ = require('lodash');
var async = require('async');
var AWS = require('aws-sdk');
var config = require('config');
var crypto = require('crypto');
var csv = require('fast-csv');
var moment = require('moment');
var util = require('util');

var db = require('./db');
var log = require('../logger')('storage');
var dbUtil = require('./dbUtil');

var awsConfig = {
  accessKeyId: config.get('aws.credentials.accessKeyId'),
  secretAccessKey: config.get('aws.credentials.secretAccessKey'),
  region: config.get('aws.s3.region')
};
var s3 = new AWS.S3(_.merge(awsConfig, {apiVersion: '2006-03-01'}));
var athena = new AWS.Athena(_.merge(awsConfig, {apiVersion: '2017-05-18'}));

/**
 * Generate a daily hash using combination of date and string values.
 *
 * @param  {Function}           callback                    Standard callback function
 * @param  {String[]}           callback.generatedHash      The filenames of the files that have been downloaded
 */
var generateHash = module.exports.generateHash = function() {
  var date = moment().format('MM-DD-YYYY');
  // Additional strings can be added to the list to create a powerful hash.
  // Choose meaningful constant strings, as it will be required while decoding the bucket names
  var fields = _.union([ date ]);
  // Create an MD5 hash out of the fields
  var generatedHash = crypto.createHash('md5').update(fields.join('')).digest('hex');
  var dailyHash = generatedHash + '-' + date;

  return dailyHash;
};

/**
 * Download the complete snapshot of Canvas Redshift data files. This api call
 * retrieves full and partial file dumps up until the last full dump.
 * The extracts are organized in the S3 bucket to facilitate creation of external schema structures.
 * A daily hash is created with a combination of a pre selected string+date to create a per day folder
 * in the S3 data lake bucket where all the extracts dowloaded for the day reside.
 *
 * @param  {Object}           data                   Contents of the file
 * @param  {String}           filename               Name of the file downloaded from cnavas data
 * @param  {String}           table                  Table name that the file is a partition of
 * @param  {Function}         callback               Standard callback function
 * @param  {Object}           callback.err           An error object, if any
 */
var storeExtractsOnS3 = module.exports.storeExtractsOnS3 = function(data, filename, table, callback) {
  // Generates a daily hash to house the canvas-data external schema structure.
  var dailyHash = generateHash();
  var path = util.format('canvas-data/%s/%s/%s',
    dailyHash,
    table,
    filename
  );
  var putParams = {
    Bucket: config.get('aws.s3.bucket'),
    Body: data,
    Key: path,
    ServerSideEncryption: 'AES256'
  };
  var getParams = {
    Bucket: config.get('aws.s3.bucket'),
    Key: path
  };

  // Checks the object metadata to see if the file already exists on S3
  s3.headObject(getParams, function(err, data) {
    if (err) {
      // If file not found then S3 PUT to upload it
      if (err.statusCode === 404) {
        log.info({path: path}, 'Put file to Amazon S3');
        s3.putObject(putParams, function(err, data) {
          if (err) {
            return callback(err);
          }

          return callback();
        });
      }
    } else {
      log.info({path: path}, 'File already in S3; nothing to do.');

      return callback();
    }
  });
};

/**
 * @param  {Number}       year                Year of enrollments
 * @param  {String}       semesterCode        Semester of enrollments: 'B' for Spring, 'C' for Summer and 'D' for Fall.
 * @param  {Number[]}     uids                One or more user UIDs
 * @param  {Function}     callback            Standard callback function
 * @param  {Object}       callback.err        An error object, if any
 * @param  {Object}       callback.data       Results of the query
 */
var getEnrollments = module.exports.getEnrollments = function(year, semesterCode, uids, callback) {
  db.constructEnrollmentsSQL(year, semesterCode, uids, function(err, sql) {
    if (err) {
      return callback(err);
    }
    executeQuery(sql, function(err, data) {
      if (err) {
        return callback(err);
      }

      return callback(null, data);
    });
  });
};

/**
 * @param  {String}       sql                 SQL query to execute
 * @param  {Function}     callback            Standard callback function
 * @param  {Object}       callback.err        An error object, if any
 * @param  {Object}       callback.data       Results of SQL query
 */
var executeQuery = function(sql, callback) {
  var s3Bucket = config.get('aws.s3.bucket');
  var s3ObjectPrefix = 'tmp/data-lake-query-results/';
  var outputLocation = 's3://' + s3Bucket + '/' + s3ObjectPrefix;
  var params = {
    QueryString: sql,
    QueryExecutionContext: {
      Database: 'canvas'
    },
    ResultConfiguration: {
      EncryptionConfiguration: {
        EncryptionOption: 'SSE_S3'
      },
      OutputLocation: outputLocation
    }
  };

  athena.startQueryExecution(params, function(err, data) {
    if (err) {
      log.error({err: err, sql: sql}, 'Athena query failed');

      return callback(err);
    }
    new Promise((resolve, reject) => {
      getQueryResults(data.QueryExecutionId, resolve, reject);

    }).then((resultsData) => {
      var s3Key = s3ObjectPrefix + data.QueryExecutionId + '.csv';

      async.series([
        function(callback) {
          s3.deleteObject({Bucket: s3Bucket, Key: s3Key}, function() {
            s3.deleteObject({Bucket: s3Bucket, Key: s3Key + '.metadata'}, callback);
          });
        }
      ], function(err, data) {
        if (err) {
          log.warn({err: err, s3Params: s3Params}, 'Failed to clean up Amazon S3 after Athena query');
        }
      });

      return callback(null, resultsData);
    });
  });
};

/**
 * @param  {String}       queryId             Amazon Athena QueryExecutionId used to gather results, when ready
 * @param  {Function}     resolve             Callback function (see Promise definition) invoked upon success
 * @param  {Function}     reject              Callback function (see Promise definition) invoked upon error
 */
var getQueryResults = function(queryId, resolve, reject) {
  athena.getQueryExecution({QueryExecutionId: queryId}, function(err, data) {
    if (err) {
      return reject(err);
    }
    if (_.includes(['SUBMITTED', 'RUNNING'], data.QueryExecution.Status.State)) {
      // Recursion. If status is 'RUNNING' then retry until 'DONE'.
      return getQueryResults(queryId, resolve, reject);
    }

    athena.getQueryResults({QueryExecutionId: queryId}, function(err, results) {
      if (err) {
        log.error({err: err, queryId: queryId}, 'Athena getQueryResults failed');

        return reject(err);
      }

      dbUtil.transformAthenaResult(results, function(transformed) {
        return resolve(transformed);
      });
    });
  });
};
