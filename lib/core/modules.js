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
var fs = require('fs');
var path = require('path');
var util = require('util');

var log = require('./logger')('core/modules');

var cachedModules = [];

/**
 * Cache the available Data Loch modules
 *
 * @param  {Function}       callback            Standard callback function
 * @param  {Object}         callback.err        An error object, if any
 */
var init = module.exports.init = function(callback) {
  // Get all the library directories.
  // Get all the node modules
  var nodeModulesDir = getNodeModulesDir();
  fs.readdir(nodeModulesDir, function(err, modules) {
    if (err) {
      log.error({err: err}, 'Unable to read the libraries directory');
      return callback({code: 500, msg: 'Unable to read the libraries directory'});
    }

    // Filter the available modules down to the Data Loch modules. All Data Loch
    // modules will begin with
    cachedModules = _.filter(modules, function(module) {
      return module.match(/^.+$/);
    });

    return callback();
  });
};

/**
 * Get the available Data Loch modules
 *
 * @return {String[]}                           The available Data Loch modules
 */
var getAvailableModules = module.exports.getAvailableModules = function() {
  return cachedModules;
};

/**
 * Get the path to the node_modules directory
 *
 * @return {String}                             The path to the node_modules directory
 * @api private
 */
var getNodeModulesDir = function() {
  var dir = util.format('%s/../', __dirname);
  return path.normalize(dir);
};
