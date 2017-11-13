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
 *  OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *  REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
 * SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
 * "AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 */

var _ = require('lodash');
var AWS = require('aws-sdk');
var async = require('async');
var config = require('config');
var crypto = require('crypto');
var moment = require('moment');
var request = require('request');
var fs = require('fs');
var util = require('util');

var log = require('../core/logger')('store/storage');

var s3Params = {
  accessKeyId: config.get('aws.credentials.accessKeyId'),
  secretAccessKey: config.get('aws.credentials.secretAccessKey'),
  region: config.get('datalake.s3.region'),
  apiVersion: '2006-03-01',
  useAccelerateEndpoint: true
};

var s3 = new AWS.S3(s3Params);
var s3Stream = require('s3-upload-stream')(new AWS.S3(s3Params));

/**
 * Generate a daily hash using combination of date and string values.
 *
 * @param  {Function}           callback                    Standard callback function
 * @param  {String[]}           callback.generatedHash      The filenames of the files that have been downloaded
 */
var generateHash = module.exports.generateHash = function() {
  var date = moment().format('MM-DD-YYYY');
  // Add the platform URL to the list of fields to use to generate a uuid
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
var storeExtractsOnS3 = module.exports.storeExtractsOnS3 = function(file, callback) {
  // Generates a daily hash to house the canvas-data external schema structure.
  var dailyHash = generateHash();
  var path = '';

  if (file.table !== 'requests') {
    path = util.format('%s/%s/%s/%s',
      config.get('datalake.canvasData.directory.daily'),
      dailyHash,
      file.table,
      file.filename);

  } else {
    path = util.format('%s/%s/%s',
      config.get('datalake.canvasData.directory.currentTerm'),
      file.table,
      file.filename);
  }

  var putParams = {
    Bucket: config.get('datalake.s3.bucket'),
    Key: path,
    ContentType: 'text/plain',
    ContentEncoding: 'gzip',
    ServerSideEncryption: 'AES256'
  };

  // Create the S3 upload stream.
  var s3upload = s3Stream.upload(putParams);
  var start = Date.now();

  request(file.url).on('response', function(res) {
    log.info(res.statusCode);
    if (res.statusCode !== 200) {
      // throw new Error('Status not 200');
      log.error('Status not 200.');
    }
    res.pipe(s3upload)
      .on('error', function() {
        log.error({err: err, file: file.filename}, 'Unable to gunzip Canvas data file stream');
      })
      .on('finish', function() {
        var end = Date.now();
        log.info({file: file.filename, duration: end - start}, 'Finished Canvas data file upload');
      });

    return callback();

  });
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
var uploadFileIfNotExists = module.exports.uploadFileIfNotExists = function(file, callback) {
  var dailyHash = generateHash();
  var path = '';
  if (file.table !== 'requests') {
    path = util.format('%s/%s/%s/%s',
      config.get('datalake.canvasData.directory.daily'),
      dailyHash,
      file.table,
      file.filename);
  } else {
    path = util.format('%s/%s/%s',
      config.get('datalake.canvasData.directory.currentTerm'),
      file.table,
      file.filename);
  }

  var getParams = {
    Bucket: config.get('datalake.s3.bucket'),
    Key: path
  };

  s3.headObject(getParams, function(err, data) {
    if (err) {
      // If file is not found upload it to S3 using S3 put
      if (err.statusCode === 404) {
        log.info('File not found. Uploading to S3');
        // organize and store canvas data files in the data lake on S3 buckets
        storeExtractsOnS3(file, function(err) {
          if (err) {
            log.error({err: err, file: file.filename}, 'Error uploading data dump to s3');
            return callback(err);
          }

          log.info({file: file.filename}, 'Uploading data dump to s3');
          return callback(null, 'File upload to s3 in progress');
        });
      } else {
        return callback(err);
      }

    } else if (data) {
      log.info('File already exists. Skipping');
      return callback(null, 'File already exists. Skipping');

    }

  });
};
