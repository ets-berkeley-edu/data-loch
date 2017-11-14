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
var bunyan = require('bunyan');
var config = require('config');
var fs = require('fs');
var PrettyStream = require('bunyan-prettystream');

var loggers = {};

/**
 * If logger does not exist then it is created.
 *
 * @param  {String}     loggerName    Name as it will appear in the logs
 * @return {Object}                   Bunyan logger
 */
module.exports = function(loggerName) {
  var name = loggerName || 'data-loch';

  loggers[name] = loggers[name] || createLogger(name);

  return loggers[name];
};

/**
 * Wrap output-stream with goodness.
 *
 * @param  {Stream}     stream        For example, process.stdout
 * @return {Stream}                   An enhanced stream
 */
var prettify = function(stream) {
  var prettyStream = new PrettyStream({
    useColor: config.get('logger.useColor')
  });

  prettyStream.pipe(stream);

  return prettyStream;
};

/**
 * @param  {Object}     a        Interpreted as boolean
 * @param  {Object}     b        Interpreted as boolean
 * @return {Boolean}             Read about XOR: https://en.wikipedia.org/wiki/XOR_gate
 */
var xor = function(a, b) {
  return !!(a ? !b : b);
};

/**
 * Create a logger with the provided name
 *
 * @param  {String}     name    The name of the logger to create
 * @api private
 */
var createLogger = function(name) {
  var loggerConfigs = config.get('logger.streams');
  var streams = [];

  _.each(loggerConfigs, function(loggerConfig) {
    var nextEntry = {
      level: loggerConfig.level || 'info'
    };
    if (xor(loggerConfig.stream, loggerConfig.path)) {
      if (loggerConfig.stream === 'stdout') {
        nextEntry.stream = prettify(process.stdout);

      } else if (loggerConfig.stream === 'stderr') {
        nextEntry.stream = prettify(process.stderr);

      } else if (_.endsWith(loggerConfig.path, '.log')) {
        nextEntry.stream = prettify(fs.createWriteStream(loggerConfig.path));

      } else if (_.endsWith(loggerConfig.path, '.json')) {
        nextEntry.path = loggerConfig.path;

      } else {
        throw Error('Missing or invalid property for \'stream\' or \'path\'');
      }

    } else {
      throw Error('An entry in \'logger\' config must specify \'stream\' or \'path\' but not both');
    }

    streams.push(nextEntry);
  });

  return bunyan.createLogger({
    name: name,
    serializers: {
      err: bunyan.stdSerializers.err,
      req: bunyan.stdSerializers.req,
      res: bunyan.stdSerializers.res
    },
    streams: streams
  });
};
