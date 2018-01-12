/**
 * Copyright ©2016. The Regents of the University of California (Regents). All Rights Reserved.
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
var bodyParser = require('body-parser');
var config = require('config');
var express = require('express');
var http = require('http');
var util = require('util');

var log = require('./logger')('core/server');

/**
* Start the Learning Record Store Express server on the configured port
*
* @return {Express}                The created express server
*/
var setUpServer = module.exports.setUpServer = function() {
  // Create the express server
  var app = express();

  // Expose the HTTP server on the express app to allow other modules to hook into it
  app.httpServer = http.createServer(app);

  // Start listening for requests
  var port = config.get('app.port');

  app.httpServer.listen(port, 'localhost');

  // Don't output pretty JSON
  app.set('json spaces', 0);

  // Don't output the x-powered-by header
  app.set('x-powered-by', false);

  // Indicate that the Learning Record Store is being used behind a reverse proxy
  // @see http://expressjs.com/guide/behind-proxies.html
  app.enable('trust proxy');

  // Parse application/x-www-form-urlencoded
  app.use(bodyParser.urlencoded({
    extended: false
  }));

  // Parse application/json
  app.use(bodyParser.json());

  // Catch-all error handler
  app.use(function(err, req, res, next) {
    log.error({
      err: err,
      req: req,
      res: res
    }, 'Unhandled error in the request chain, caught at the default error handler');
    return abort(res, 500, 'An unexpected error occurred');
  });

  log.info(util.format('Canvas data lake server is listening at http://127.0.0.1:%s', port));

  return app;

};

/**
* Abort a request with a given code and response message
*
* @param  {Response}   res         The express response object
* @param  {Number}     code        The HTTP response code
* @param  {String}     message     The message body to provide as a reason for aborting the request
* @api private
*/
var abort = function(res, code, message) {
  res.setHeader('Connection', 'Close');
  return res.status(code).send(message);
};
