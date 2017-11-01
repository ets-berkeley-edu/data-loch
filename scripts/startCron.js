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
var config = require('config');
var cron = require('cron');
var log = require('../lib/logger')('startCron');

// TODO: Consider storing 'lastExecutionDate' in the db
var job = null;
var lastExecutionDate = null;
var tasks = [ require('../lib/cron-tasks/refreshDataLakeViews') ];

/**
 * A set of tasks run synchronously.
 */
var runTasks = function(callback) {
  // Run all tasks
  var done = _.after(tasks.length, callback);

  _.each(tasks, function(task, index) {
    task.run(function(err) {

      return done(err);
    });
  });
};

/**
 * Configure and start a cron-like job that will execute the task per 'interval' config.
 *
 * @param  {Function}     [callback]            Standard callback function
 */
var schedule = function(callback) {
  try {
    var runOnInit = Boolean(config.get('dataLake.cronJob.runOnInit'));

    job = new cron.CronJob({
      cronTime: config.get('dataLake.cronJob.interval'),
      onTick: runTasks,
      onComplete: onComplete,
      runOnInit: runOnInit,
      start: true,
      timeZone: 'America/Los_Angeles'
    });

    if (!runOnInit) {
      log.info('The first task will run at ' + job.nextDates(1));
    }

    return callback();

  } catch (err) {
    return callback(err);
  }
};

/**
 * This function is invoked when the 'scrape' task is complete.
 */
var onComplete = function() {
  if (job) {
    log.info('Cron has completed a round of the requested tasks; the job will run again at ' + job.nextDates(1));
  }
};

var start = function(callback) {
  var enabled = Boolean(config.get('dataLake.cronJob.enabled'));

  if (enabled) {
    schedule(function(err) {
      if (err) {
        log.error({err: err}, 'We might have an invalid \'dataLake.cronJob.interval\' configuration.');
      }

      return callback();
    });

  } else {
    log.warn('This cron job is disabled. DataLake scans and db updates will not happen.');

    return callback();
  }
};

// This cron-like job keeps BOAC datasource synced with the DataLake.
start(function(callback) {
  log.info('Cron started.');

  return;
});
