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
var config = require('config');
var csv = require('fast-csv');
var fs = require('fs');

var log = require('../lib/logger')('sync');
var redshiftUtil = require('../lib/util');
var schema = require('../lib/schema');

var argv = require('yargs')
  .usage('Usage: $0 --max-old-space-size=8192')
  .describe('u', 'Download the latest Canvas Redshift data files')
  .help('h')
  .alias('h', 'help')
  .argv;

/**
 * Download the latest Canvas Redshift data files. For each table, this will
 * download the latest full dump, as well as any partial dumps that come after
 * that
 *
 * @param  {Function}             callback              Standard callback function
 * @api private
 */
var downloadFiles = function(callback) {
  // Get the table names
  redshiftUtil.canvasDataApiRequest('/file/latest', function(latestDump) {
    var tables = _.keys(latestDump.artifactsByTable);

    async.eachSeries(tables, function(table, done) {
      // Get the list of dumps for each table
      log.info({table: table}, 'Processing table');
      redshiftUtil.canvasDataApiRequest('/file/byTable/' + table, function(tableDump) {
        var full = _.find(tableDump.history, {partial: false});

        if (!full) {
          log.warn({table: table}, 'Unable to find full dump. Skipping');
          return done();
        }

        var foundFull = false;
        var files = [];
        for (var i = tableDump.history.length - 1; i >= 0; i--) {
          var dump = tableDump.history[i];
          if (foundFull || dump.sequence === full.sequence) {
            foundFull = true;
            files = files.concat(dump.files);
          }
        }

        // Download the files to disk
        redshiftUtil.downloadFiles('raw', files, function(filenames) {
          log.info({table: table, total: files.length}, 'Finished downloading files');
          return done();
        });
      });
    }, function() {
      log.info('Finished downloading all files');
      return callback();
    });
  });
};

/**
 * Merge Canvas data fact and dimension tables into consolidated files for easier
 * processing per the configuration in `schema.js`
 *
 * @param  {Function}             callback              Standard callback function
 * @api private
 */
var mergeFiles = function(callback) {
  var storageDirectory = config.get('storage') || './data/';

  // TODO: Copy the request files

  async.forEachOfSeries(schema, function(map, name, done) {
    // Don't merge the request files as they'll be copied as they are
    if (name === 'requests') {
      return done();
    }

    redshiftUtil.mergeCSVFiles('raw', map, function(rows) {

      // Write the merged rows to a consolidated CSV file
      var csvStream = csv.format({delimiter: '\t', quote: null});
      var writableStream = fs.createWriteStream(storageDirectory + 'merged/' + name);
      writableStream.on('finish', function() {
        return done();
      });

      csvStream.pipe(writableStream);
      _.each(rows, function(row) {
        csvStream.write(row);
      });
      csvStream.end();
    });
  });
};

downloadFiles(function() {
  // mergeFiles(function() {});
});
