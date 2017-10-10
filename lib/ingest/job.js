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

var config = require('config');
var cron = require('cron');
var log = require('../logger')('ingestRunner');

var cronTime = config.get('ingest.cronTime');
var ingestJob = null;
// TODO: Consider storing 'lastExecutionDate' in the db
var lastExecutionDate = config.get('ingest.lastExecutionDate');

/**
 * Scan the DataLake and update db accordingly.
 *
 * @param  {Function}     [callback]            Standard callback function
 */
var ingestTask = function(callback) {
  // If runOnInit=true then 'ingestJob' will be null on first iteration
  lastExecutionDate = ingestJob ? ingestJob.lastDate() : lastExecutionDate;

  log.info('Ingest task is starting...');

  return callback();
};

/**
 * This function is invoked when the ingest task is complete.
 */
var onComplete = function() {
  if (ingestJob) {
    log.info('This iteration is complete. The task will again at ' + ingestJob.nextDates(1));
  }
};

/**
 * Configure and start a cron-like job that will execute the Ingest task per cronTime config.
 *
 * @param  {Function}     [callback]            Standard callback function
 */
var scheduleIngest = function(callback) {
  try {
    var runOnInit = Boolean(config.get('ingest.runOnInit'));

    ingestJob = new cron.CronJob({
      cronTime: config.get('ingest.cronTime'),
      onTick: ingestTask,
      onComplete: onComplete,
      runOnInit: runOnInit,
      start: true,
      timeZone: 'America/Los_Angeles'
    });

    if (!runOnInit) {
      log.info('The first task will run at ' + ingestJob.nextDates(1));
    }

    return callback();

  } catch (err) {
    return callback(err);
  }
};

var start = module.exports.start = function(callback) {
  var ingestEnabled = Boolean(config.get('ingest.enabled'));

  if (ingestEnabled) {
    log.info('Ingest job is enabled; it will run according to the \'ingest.cronTime\' schedule.');

    scheduleIngest(function(err) {
      if (err) {
        log.error({err: err}, 'We might have an invalid \'ingest.cronTime\' configuration.');
      }
    });

  } else {
    log.warn('Ingest is disabled. DataLake scans and db updates will not happen.');
  }
};
