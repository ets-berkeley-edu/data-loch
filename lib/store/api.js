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
var request = require('request');
var fs = require('fs');

var canvas = require('./canvas.js');
var storage = require('./storage.js');
var log = require('../core/logger')('store/api');

/**
 * The function determines which the statement type for the incoming request so that suitable handlers can be invoked subsequently.
 *
 * @param  {Object}         file                         Context object contains lambda runtime information such as functionName, CloudWatch log group etc.
 * @param  {Object}         callback.err                 An error that occurred if statement type is not XAPI or CALIPER type, if any
 * @param  {Object}         callback.result              Statement type that was determined
 */

var processRequest = module.exports.processRequest = function(file, callback) {
  // organize and store canvas data files in the data lake on S3 buckets
  storage.uploadFileIfNotExists(file, function(err, result) {
    if (err) {
      return callback(err);
    }

    return callback(null, result);
  });
};

/**
 * The function determines which the statement type for the incoming request so that suitable handlers can be invoked subsequently.
 *
 * @param  {Object}         request                      Context object contains lambda runtime information such as functionName, CloudWatch log group etc.
 * @param  {Object}         callback.err                 An error that occurred if statement type is not XAPI or CALIPER type, if any
 * @param  {Object}         callback                     Statement type that was determined
 */

var validateRequest = module.exports.validateRequest = function(request, callback) {
  if (request.hasOwnProperty('url') && request.hasOwnProperty('table') && request.hasOwnProperty('filename') && request.hasOwnProperty('partial')) {
    log.info('Request is valid');
  } else {
    return callback(new Error('Invalid request options provided.'));
  }

  return callback();
};
