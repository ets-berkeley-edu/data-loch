/**
 * Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.
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
 * 'AS IS'. REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 */

var http = require('http');
var Mixpanel = require('mixpanel');
var md5 = require('MD5');
var stringify = require('csv-stringify');
var _ = require('lodash');
var AWS = require('aws-sdk');
var config = require('config');
var fs = require('fs');
var util = require('util');

var log = require('../../../lib/core/logger')('mixpanelExport');

var s3Params = {
  accessKeyId: config.get('aws.credentials.accessKeyId'),
  secretAccessKey: config.get('aws.credentials.secretAccessKey'),
  region: config.get('datalake.s3.region'),
  apiVersion: '2006-03-01',
  useAccelerateEndpoint: true
};

var s3 = new AWS.S3(s3Params);
var s3Stream = require('s3-upload-stream')(new AWS.S3(s3Params));
var apiKey = config.get('datalake.suitec.mixpanel.key');
var apiSecret = config.get('datalake.suitec.mixpanel.secret');

var uploadMixpanelEventsToS3 = module.exports.uploadMixpanelEventsToS3 = function(callback) {
  var putParams = {
    Bucket: config.get('datalake.s3.bucket'),
    Key: config.get('datalake.suitec.directory.mixpanelEvents') + '/export.json',
    ContentType: 'text/plain',
    ServerSideEncryption: 'AES256'
  };

  // Create the S3 upload stream.
  var s3upload = s3Stream.upload(putParams);
  // cmd line input to get the event to filter the request by.
  var eventFilter = process.argv[2];
  // Provide date range for exports
  var fromDate = config.get('datalake.suitec.mixpanel.exportParams.from_date');
  var toDate = config.get('datalake.suitec.mixpanel.exportParams.to_date');
  // query params needed to make Mixpanel signature
  var today = new Date();
  // 1E8 is approximately a day in milliseconds
  var expireUTC = today.getTime() + 1E8;
  // parameter list to export mixpanel raw events.
  var params = [
    'from_date=' + fromDate,
    'to_date=' + toDate,
    'api_key=' + apiKey,
    'expire=' + expireUTC
  ];

  // if the user has supplied an event to filter by, push it to params array
  if (eventFilter !== undefined) {
    params.push('event=' + JSON.stringify([ eventFilter ]));
  }

  // create the signature
  var paramsConcat = params.sort().join('');
  var signature = md5(paramsConcat + apiSecret);
  // concat query params
  var base_url = 'http://data.mixpanel.com/api/2.0/export/?';
  var request = base_url + params.join('&') + '&sig=' + signature;

  // make get request to Mixpanel API
  var req = http.get(request, function(err, response) {
    if (err) {
      log.error('Got error: ' + err.message);
      return callback(err);
    }

    log.info('Streaming mixpanel events from export api to S3 data loch location. This may take a while.');
    // Multi part streming to s3
    response.pipe(s3upload)
      .on('error', function(err) {
        log.error({err: err}, 'Unable to gunzip Canvas data file stream');
        return callback(err);
      })
      .on('finish', function() {
        log.info('Finished Canvas data file upload');
        return callback();
      });
  });
};

uploadMixpanelEventsToS3(function(err) {
  if (err) {
    log.error({err: err});
  }

  log.info('Mixpanel events have been uploaded to data-loch s3 buckets successfully.');
});
