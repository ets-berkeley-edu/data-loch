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

var log = require('../logger')('util');

var transformAthenaResult = module.exports.transformAthenaResult = function(results, callback) {
  var parseResultRow = function(row) {
    return _.map(row.Data, 'VarCharValue');
  };
  var rows = results.ResultSet.Rows;
  var headerRow = parseResultRow(rows[0]);
  var dataRows = _.map(_.drop(rows), parseResultRow);

  log.info({headerRow: headerRow, dataRows: dataRows}, 'Convert query-results to a proper hash');

  convertQueryResults(headerRow, dataRows, function(transformed) {
    return callback(transformed);
  });
};

/**
 * @param  {Array}        keys           Column names of db result-set
 * @param  {Array}        rows           Each row in resultset is an array of values
 * @param  {Function}     callback       Standard callback function
 */
var convertQueryResults = function(keys, rows, callback) {
  var results = [];
  var done = _.after(rows.length, callback);

  // Iterate across the resultset
  _.each(rows, function(row) {
    var index = 0;
    var resultRow = _.keyBy(row, function(o) {
      return keys[index++];
    });
    results.push(resultRow);

    return done(results);
  });
};
