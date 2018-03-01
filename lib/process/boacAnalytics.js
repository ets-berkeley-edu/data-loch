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
 * "AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 */

var fs = require('fs');
var path = require('path');

var log = require('../core/logger')('process');
var db = require('../sync/db');

/**
 * Runs a prepared sql script on Redshift. In this case, the function runs the
 * queries related to the generation of BOAC analytics and materialized views
 *
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
var createBoacViews = module.exports.createBoacViews = function(callback) {
  db.runSQLScript(path.join(__dirname, '../../sql/materializedViews.sql'), function(err) {
    if (err) {
      log.error({err: err}, 'Failed to generate materialized views for BOAC.');

      return callback(err);
    }

    return callback();
  });
};

/**
 * Prepares sql script related to BOAC analytics from the template files under sync/db_templates.
 * Runs the prepared sql script on Redshift to generate BOAC analytics and refresh materialzed Views
 * in BOAC analytics schema.
 *
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
var generateBoacAnalytics = module.exports.generateBoacAnalytics = function(callback) {
  var boacViewsTemplate = path.join(__dirname, '../sync/db-templates/materializedViews.template.sql');
  var boacViewsScript = path.join(__dirname, '../../sql/materializedViews.sql');

  db.generateSqlScript(boacViewsTemplate, boacViewsScript, function(err) {
    if (err) {
      log.error({err: err, template: dbCreationTemplate, script: dbCreationScript}, 'Failed to generate SQL script using db-template.');

      return callback(err);
    }

    createBoacViews(function(err) {
      if (err) {
        log.error({err: err.message}, 'Error occured while generating BOAC materialized views.');

        return callback(err);
      }

      log.info('Successfully generated BOAC materialized views.');

      return callback();

    });
  });
};

/**
 * Run jobs to generate materialized views for BOAC
 *
 * @param  {Function}           callback                Standard callback function
 * @param  {Object}             callback.err            An error that occurred, if any
 */
var run = module.exports.run = function(callback) {
  generateBoacAnalytics(function(err) {
    if (err) {
      log.info({err: err}, 'Failed to generate materialized views for BOAC.');
      return callback(err);
    }

    log.info('Materialized views were successfully generated for BOAC.');
    return callback();
  });
};
