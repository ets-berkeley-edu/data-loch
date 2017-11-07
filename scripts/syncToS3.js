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
var request = require('request');

var canvas = require('../lib/store/canvas');
var log = require('../lib/logger')('syncToS3');
var storage = require('../lib/store/storage.js');

var argv = require('yargs')
  .usage('Usage: $0 --max-old-space-size=8192')
  .describe('u', 'Download the latest Canvas Data files')
  .help('h')
  .alias('h', 'help')
  .argv;

/**
 * Upload the compressed data files for each table from Canvas Data on to S3 buckets.
 *
 * @param  {Object[]}           files                   The Canvas data files to download and unzip
 * @param  {Function}           callback                Standard callback function
 * @param  {String[]}           callback.filenames      The filenames of the files that have been downloaded
 */
var uploadFilesToS3 = module.exports.uploadFilesToS3 = function(files, callback) {
  // asynchronously downlaod all the data files from the url
  async.eachSeries(files, function(file, done) {
    var filename = file.filename;
    var table = file.table;

    log.info({file: filename}, 'Starting Canvas data file download');

    var options = {
      uri: file.url,
      encoding: null
    };

    // Access Canvas Data apis and get the relevant files
    request(options, function(err, response, body) {
      if (err || response.statusCode !== 200) {
        log.error({err: err}, 'Failed to get the data dump file');

        return done(err);
      }

      // organize and store canvas data files in the data lake on S3 buckets
      storage.storeExtractsOnS3(body, filename, table, function(err) {
        if (err) {
          log.error({err: err, file: filename}, 'Error uploading data dump to s3');
          return done(err);
        }

        log.info({file: filename}, 'Success uploading data dump to s3');
        return done();
      });
    });

  }, function(err) {
    if (err) {
      return callback(err);
    }

    return callback();
  });
};

/**
 * Retrieves latest requests filenames and urls corresponding to the current date.
 *
 * @param  {Function}           callback                    Standard callback function
 * @param  {String[]}           callback.requestsFiles      The filenames of the files that have been downloaded
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
 * Download and unzip a set of Canvas Data files and then upload to Amazon S3
 *
 * @param  {Function}           callback                Standard callback function
 */
var migrateDataToS3 = function(callback) {
  // Get the list of files, tables and signed URLs that contain complete Canvas data snapshots
  canvas.dataApiRequest('/file/sync', function(fileDump) {
    var files = [];

    _.each(fileDump.files, function(file) {
      // Skip adding request files. We will be adding only latest request files in the next step.
      if (file.table !== 'requests') {
        files.push(file);
      }
    });

    // Get latest request files for current date and add it to the finalized files to be downloaded.
    getLatestRequestFiles(function(requestsFiles) {
      async.eachSeries(requestsFiles, function(file, done) {
        files.push(file);
        return done();

      }, function() {
        log.info('Added sync files for requests');

        uploadFilesToS3(files, function(err) {
          if (err) {
            log.error({err: err.message}, 'Failed to upload Canvas Data files to Amazon S3');

            return callback(err);
          }
          log.info('Canvas Data files have been uploaded to Amazon S3');

          return callback();
        });
      });
    });
  });
};

var run = function() {
  migrateDataToS3(function(err) {
    if (err) {
      log.info({err: err}, 'Failed to migrate data to S3');

      process.exit(1);
    }
    log.info('Successfully migrated Canvas Data to Amazon S3.');
  });
};

run();
