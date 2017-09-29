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
var request = require('request');

var log = require('../lib/logger');
var redshiftUtil = require('../lib/util');
var storage = require('../lib/store/storage.js');
var sqlTemplates = require('../lib/store/generateRedshiftFiles');
var redshiftSetUp = require('../lib/store/redshift');

var argv = require('yargs')
  .usage('Usage: $0 --max-old-space-size=8192')
  .describe('u', 'Download the latest Canvas Redshift data files')
  .help('h')
  .alias('h', 'help')
  .argv;

/**
 * Upload the compressed data files for each table from Canvas Data on to S3 buckets.
 *
 * @param  {Object[]}           files                   The Canvas Redshift data files to download and unzip
 * @param  {Function}           callback                Standard callback function
 * @param  {String[]}           callback.filenames      The filenames of the files that have been downloaded
 */
var uploadFilesToS3 = module.exports.uploadFilesToS3 = function(files, callback) {
  var filenames = [];
  // asynchronously downlaod all the data files from the url
  async.eachSeries(files, function(file, done) {
    var filename = file.filename;
    filenames.push(filename);
    var table = file.table;
    log.info({file: filename}, 'Starting Canvas data file download');

    // TODO: Requests table has about 2000 partitions of 500 MB each. Load it separately.
    if (table === 'requests') {
      log.info({file: filename}, 'Skipping requests upload');
      return done();
    }

    var options = {
      uri: file.url,
      encoding: null
    };

    // Access Canvas Data apis and get the relevant files
    request(options, function(err, response, body) {
      if (err || response.statusCode !== 200) {
        log.error({err: err}, 'Failed to get the data dump file');
        return done(err);

      } else {
        // organize and store canvas data files in the data lake on S3 buckets
        storage.storeExtractsOnS3(body, filename, table, function(err) {
          if (err) {
            log.error({err: err, file: filename}, 'Error uploading data dump to s3');
            return done(err);
          } else {
            log.info({file: filename}, 'Success uploading data dump to s3');
            return done();
          }

        });
      }

    });
  }, function(err) {
    if (err) {
      return callback(err);
    }

    return callback(null, filenames);
  });
};

/**
 * Download and unzip a set of Canvas Redshift data files
 *
 * @param  {Function}           callback                Standard callback function
 */
var downloadFiles = function(callback) {
  // Get the list of files, tables and signed URLS that contains a complete Canvas data snapshots
  redshiftUtil.canvasDataApiRequest('/file/sync', function(fileDump) {
    var files = [];
    for (var i = fileDump.files.length - 1; i >= 0; i--) {
      files = files.concat(fileDump.files[i]);
    }

    // Upload files from reponse to S3
    uploadFilesToS3(files, function(err, filenames) {
      if (err) {
        return callback(err);
      }

      log.info('Finished downloading files');
      return callback();
    });
  });
};

downloadFiles(function(err) {
  if (err) {
    log.info('Download to S3 failed !');
    process.exit(1);
  }

  log.info('Upload to S3 successful. Proceeding with external database creation');
  sqlTemplates.createRedshiftTemplates(function(err) {
    if (err) {
      log.error('Redshift database template file generation failed. Ending process.');
      process.exit(1);
    }

    redshiftSetUp.createCanvasDb(function(err) {
      if (err) {
        log.error({err: err}, 'Canvas Data restore on Redshift instance failed');
        process.exit(1);
      }

      log.info('Canvas Data instance created on Redshift. Now available for querying.');
    });

  });
});
