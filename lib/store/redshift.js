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
var config = require('config');
var fs = require('fs');

var Redshift = require('node-redshift');
var log = require('../logger');

var client = {
  user: config.get('dataLake.redshiftSpectrum.user'),
  database: config.get('dataLake.redshiftSpectrum.database'),
  password: config.get('dataLake.redshiftSpectrum.password'),
  port: config.get('dataLake.redshiftSpectrum.port'),
  host: config.get('dataLake.redshiftSpectrum.host')
};

// The values passed in to the options object will be the difference between a connection pool and raw connection
var redshiftClient = new Redshift(client);

/**
 * Connects to redshift instance and executes all the sql scripts asynchronously.
 * Note - Do not use the function if the sql queries need to be executed sequentially.
 *
 * @param  {String}           sqlScript               Path to the sql script to be executed
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
var executeBulkSql = module.exports.executeBulkSql = function(sqlScript, callback) {
  fs.readFile(sqlScript, function(err, data) {
    if (err) {
      return callback(err);
    }

    var queries = data.toString().split(';');
    async.eachSeries(queries, function(query, done) {
      redshiftClient.query(query + ';', function(err, data) {
        if (err) {
          log.error({query: query, msg: 'Query execution failed'});
          return done(err);
        }

        log.info('Query executed successfully');
        return done();

      });
    }, function(err) {
      if (err) {
        return callback(err);
      }

      log.info('Redshift instance creation successful. All done !');
      return callback();
    });
  });
};


/**
 * Creates external database and tables referring the S3 storage buckets for the data lake using redshift spectrum.
 * The external tables/schema will be available in data catalog and can be accessed from both Athena and Spectrum
 *
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
var createCanvasDb = module.exports.createCanvasDb = function(callback) {
  executeBulkSql(__dirname + '/db-templates/dbCreation.sql', function(err) {
    if (err) {
      log.error({err: err}, 'Creation of Canvas database on Redshift failed.');
      return callback(err);
    }

    return callback();
  });
};

/**
 * Repoints the external database and tables referring the S3 storage buckets location using redshift spectrum.
 * When new data dumps are available on S3, if the tables already exist then repoint the external database/schemas
 * to new S3 location by running Alter statements.
 *
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
var repointCanvasDb = module.exports.repointCanvasDb = function(callback) {
  executeBulkSql(__dirname + '/db-templates/dbRepoint.sql', function(err) {
    if (err) {
      log.error({err: err}, 'Repoint of Canvas database on Redshift failed.');
      return callback(err);
    }

    return callback();
  });
};
