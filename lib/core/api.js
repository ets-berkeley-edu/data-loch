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
var appPackage = require('../../../package.json');
var config = require('config');
var db = require('../sync/db');
var express = require('express');
var fs = require('fs');
var path = require('path');
var util = require('util');

var logger = module.exports.logger = require('./logger');
var log = logger('core');
var Modules = require('./modules');
var Server = require('./server');

/**
 * Initialize the Data Loch
 *
 * @param  {Function}       callback            Standard callback function
 * @param  {Object}         callback.err        An error object, if any
 */
var init = module.exports.init = function(callback) {
  // All unexpected or uncaught errors will be caught and logged here. At this point we cannot
  // guarantee that the system is functioning properly anymore so we kill the process. When running
  // in production, the service script will automatically respawn the instance
  process.on('uncaughtException', function(err) {
    log.error({err: err}, 'Uncaught exception was raised, restarting the process');
    process.exit(1);
  });

  // Initialize the modules
  Modules.init(function() {
    // Initialize the Express server
    initializeServer();

    return callback();
  });
};

/**
 * Initialize the Data Loch app server and initialize the REST API endpoints
 *
 * @api private
 */
var initializeServer = function() {
  // Initialize the Express server
  var appServer = module.exports.appServer = Server.setUpServer();

  // A router for all routes on /api
  var apiRouter = module.exports.apiRouter = express.Router();

  appServer.use('/api', apiRouter);

  // Check if a `rest.js` file exists in the `lib` folder of each
  // module. If such a file exists, we require it. This allows other
  // modules to add in their own REST apis
  var dataLochModules = Modules.getAvailableModules();
  _.each(dataLochModules, function(module) {
    var restFile = path.join(__dirname, '..', module, '/rest.js');
    if (fs.existsSync(restFile)) {
      log.info({module: module}, util.format('Registering REST APIs for %s', module));
      require(restFile);
    }
  });
  log.info('Finished initializing REST APIs');
};


/**
 * Get the current version number of the Data Loch app.
 *
 * @param  {Function}      callback                Standard callback function
 * @param  {Object}        callback.version        The current version number of the Data Loch app
 */
var getVersion = module.exports.getVersion = function(callback) {
  var version = {
    'version': appPackage.version
  };

  // Add build info, if available
  var buildStats = '../../../config/build-stats.json';
  if (fs.existsSync(path.join(__dirname, buildStats))) {
    _.extend(version, require(buildStats));
  };

  return callback(version);
};


/**
 * Get the status of the Data Loch app. By being able to execute this function, the Data Loch
 * Node.js process is up by definition. Next to this, the database connection and the ability to
 * run simple queries on redshift is tested
 *
 * @param  {Function}       callback                Standard callback function
 * @param  {Object}         callback.status         The current status of the Data Loch app
 * @param  {Boolean}        callback.status.app     Whether the Data Loch app is up. When this function can be successfully called, the Data Loch app is up by definition
 * @param  {Boolean}        callback.status.db      Whether the database is up and Data Loch is able to successfully communicate with it
 */
var getStatus = module.exports.getStatus = function(callback) {
  // Indicate that the Node.js process is up
  var status = {
    'app': true
  };

  // Check if db connection is working fine and app can run simple queries
  var externalSchema = config.get('datalake.canvasData.externalSchema');
  var query = 'SELECT tablename FROM SVV_EXTERNAL_TABLES WHERE schemaname=\'' + externalSchema + '\'';

  db.runSQL(query, function(err, data) {
    status.db = err ? false : true;
    return callback(status);
  });
};
