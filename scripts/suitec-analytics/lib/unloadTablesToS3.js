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

var _ = require('lodash');
var async = require('async');
var config = require('config');
var fs = require('fs');
var moment = require('moment');
var path = require('path');
var Redshift = require('node-redshift');

var log = require('../../../lib/core/logger')('unloadTablesToS3');
var db = require('../../../lib/sync/db.js');

var prepareUnloadData = module.exports.prepareUnloadData = function(schemaName, tableName, callback) {

  var query = 'SELECT column_name FROM information_schema.columns \
  WHERE table_schema = \'' + schemaName + '\' AND table_name = \'' + tableName + '\' \
  ORDER BY ordinal_position;';

  db.runSQL(query, function(err, data) {
    if (err) {
      log.error({err: err}, 'Query execution failed');
      return callback(err);
    }

    var date = moment().format('YYYY-MM-DD');
    var researchGroupRequestingData = config.get('datalake.suitec.researchGroupRequestingData');
    var exportLocation = 's3://' + config.get('datalake.s3.bucket') + '/suiteC/export/' + researchGroupRequestingData + '/' + date + '/' + tableName;
    var credentials = 'aws_access_key_id=' + config.get('aws.credentials.accessKeyId') +
     ';aws_secret_access_key=' + config.get('aws.credentials.secretAccessKey');

    var columns = data.rows.map(function(elem) {
      return '\\\'' + elem.column_name + '\\\' AS ' + elem.column_name;
    }).join(', ');

    var castedColumns = data.rows.map(function(elem) {
      return 'CAST(' + elem.column_name + ' AS text) AS ' + elem.column_name;
    }).join(', ');

    var selectQuery = 'SELECT 1 AS ordinal, ' + columns + ' UNION ALL ' +
      'SELECT 2 AS ordinal,' + castedColumns + ' FROM ' + schemaName + '.' + tableName +
      ' ORDER BY ordinal';

    var unloadQuery = 'UNLOAD (\'' + selectQuery + '\') ' +
     'TO \'' + exportLocation + '\' credentials \'' + credentials +
     '\' DELIMITER AS \'\t\' ESCAPE GZIP NULL AS \'\' ALLOWOVERWRITE \
     PARALLEL OFF ';

    return callback(null, unloadQuery);

  });
};

var unloadToS3 = module.exports.unloadToS3 = function(callback) {

  var schema = config.get('datalake.suitec.analyticsSchema');
  var tables = [
    'activities',
    'assets',
    'asset_categories',
    'asset_comments',
    'asset_users',
    'asset_whiteboard_elements',
    'courses',
    'events',
    'ei_score_configs',
    'mixpanel_events',
    'user_enrollments',
    'whiteboards',
    'whiteboard_chats',
    'whiteboard_elements',
    'whiteboard_members',
    'canvas_discussions_entry',
    'canvas_discussions_topic',
    'canvas_conversations'
  ];

  async.eachSeries(tables, function(table, done) {
    prepareUnloadData(schema, table, function(err, unloadQuery) {
      if (err) {
        log.error({err: err}, 'Error occured');
        return done(err);
      }

      db.runSQL(unloadQuery, function(err, data) {
        if (err) {
          log.error({err: err}, 'Query execution failed');
          return done(err);
        }

        log.info({table: table}, 'Unload to S3 successful');
        return done();
      });
    });

  }, function(err) {
    if (err) {
      log.error('Some tables were not exported correctly');
      return callback(err);
    }

    log.info('Export to S3 successful.');
    return callback();
  });
};
