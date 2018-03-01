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

var auth = require('basic-auth');
var config = require('config');

var boacAnalytics = require('../process/boacAnalytics');
var DataLoch = require('../core/api');
var syncDumps = require('../store/syncDumps');
var syncDb = require('../sync/syncDb');
var log = require('../core/logger')('store/rest');

/* !
 * Sync latest Canvas data dump files on S3
 */
DataLoch.apiRouter.post('/syncCanvasDumps', function(req, res) {
  var credentials = auth(req);
  var username = config.get('datalake.worker.beanstalk.username');
  var password = config.get('datalake.worker.beanstalk.password');

  if (!credentials || credentials.name !== username || credentials.pass !== password) {
    return res.status(403).send('Incorrect credentials. Access denied.');

  } else {
    // Sync data model
    syncDumps.run(function(err) {
      if (err) {
        log.error(err);
      }

      log.info('Canvas Dumps successfully refreshed on S3.');
    });
  }

  return res.status(200).send('Triggered cron script successfully.');
});

/* !
 * Refresh Canvas databases
 */
DataLoch.apiRouter.post('/createCanvasSchema', function(req, res) {

  var credentials = auth(req);
  var username = config.get('datalake.worker.beanstalk.username');
  var password = config.get('datalake.worker.beanstalk.password');

  if (!credentials || credentials.name !== username || credentials.pass !== password) {
    return res.status(403).send('Incorrect credentials. Access denied.');

  } else {
    // Sync data model
    syncDb.run(function(err) {
      if (err) {
        log.error(err);
      }

      log.info('Canvas Databases successfully refreshed on Redshift.');
    });
  }

  return res.status(200).send('Triggered cron script successfully.');
});

/* !
 * Genrate BOAC analytics and refresh materialized views
 */
DataLoch.apiRouter.post('/generateBoacAnalytics', function(req, res) {

  var credentials = auth(req);
  var username = config.get('datalake.worker.beanstalk.username');
  var password = config.get('datalake.worker.beanstalk.password');

  if (!credentials || credentials.name !== username || credentials.pass !== password) {
    return res.status(403).send('Incorrect credentials. Access denied.');

  } else {
    // Sync data model
    boacAnalytics.run(function(err) {
      if (err) {
        log.error(err);
      }

      log.info('Successfully generated BOAC materialized views.');
    });
  }

  return res.status(200).send('Triggered cron script successfully.');
});
