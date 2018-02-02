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
var cron = require('cron');
var log = require('../core/logger')('startCron');

// TODO: Consider storing 'lastExecutionDate' in the db
var job = null;
var lastExecutionDate = null;

/**
 * A set of tasks run synchronously.
 */
var runTasks = function(task, callback) {
  log.info('Running cron job : ' + JSON.stringify(task));
  task.run(function(err) {
    if (err) {
      log.error({err: err}, 'Error running cron script');
    }
  });
};

/**
 * Configure and start a cron-like job that will execute the task per 'interval' config.
 *
 * @param  {Function}     [callback]            Standard callback function
 */
var schedule = function(opts, callback) {
  try {
    var runOnInit = Boolean(opts.runOnInit);

    var job = new cron.CronJob({
      cronTime: opts.interval,
      onTick: function() {
        runTasks(opts.script);
      },
      onComplete: onComplete,
      runOnInit: runOnInit,
      start: true,
      timeZone: 'America/Los_Angeles'
    });

    if (!runOnInit) {
      log.info('The ' + opts.cronTask + ' task will run at ' + job.nextDates(1));
    }

    return callback();

  } catch (err) {
    return callback(err);
  }
};

/**
 * Configure and start a cron-like job that will execute the task per 'interval' config.
 *
 * @param  {Function}     [callback]            Standard callback function
 */
var prepareCronTasks = function(callback) {
  var tasks = ['syncDumps', 'syncDb'];
  var opts = {};
  async.eachSeries(tasks, function(task, done) {
    opts = {
      cronTask: task,
      interval: config.get('datalake.cron.tasks.' + task + '.interval'),
      script: require(config.get('datalake.cron.tasks.' + task + '.script')),
      runOnInit: config.get('datalake.cron.tasks.' + task + '.runOnInit')
    };

    schedule(opts, function(err) {
      if (err) {
        log.error({err: err}, 'We might have an invalid cron configuration for ' + task);
        return done(err);
      }

      return done();
    });

  }, function(err) {
    if (err) {
      log.error({err: err}, 'Cron schedule failed for one or many tasks.');
      return callback(err);
    }

    log.info('All cron jobs are scheduled successfuly !');
    return callback();
  });

};

/**
 * This function is invoked when the 'scrape' task is complete.
 */
var onComplete = function() {
  if (job) {
    log.info('Cron has completed a round of the requested tasks; the job will run again at ' + job.nextDates(1));
  }
};

var start = module.exports.start = function(callback) {
  var enabled = Boolean(config.get('datalake.cron.enabled'));

  if (enabled) {
    prepareCronTasks(function(err) {
      if (err) {
        log.error({err: err}, 'We might have an invalid cron configuration.');
      }

      return callback();
    });

  } else {
    log.warn('This cron job is disabled. DataLake scans and db updates will not happen.');

    return callback();
  }
};
