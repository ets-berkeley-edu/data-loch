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
var crypto = require('crypto');
var csv = require('fast-csv');
var fs = require('fs');
var readline = require('readline');

var fields = [
  'id',
  'timestamp',
  'verb.id',
  'verb.display.en-US',
  'object.objectType',
  'object.id',
  'object.definition.type',
  'object.definition.name.en-US',
  'object.definition.description.en-US',
  'object.definition.extensions.https://canvas.instructure.com/xapi/assignments/submissions_types',
  'result.response',
  'result.completion',
  'result.score.raw',
  'result.score.scaled',
  'result.score.min',
  'result.score.max',
  'context.platform',
  'context.contextActivities.grouping.objectType',
  'context.contextActivities.grouping.definition.name.en-US',
  'context.contextActivities.grouping.id',
  'context.contextActivities.parent.objectType',
  'context.contextActivities.parent.id'
];

var directory = config.get('storage') || './data/';

var csvStream = csv.createWriteStream({headers: true});
var writableStream = fs.createWriteStream(directory + 'statements/ESPM 50AC - LEC 001.csv');
csvStream.pipe(writableStream);

var readstream = fs.createReadStream(directory + 'statements/ESPM 50AC - LEC 001.txt');
var rl = readline.createInterface({
  input: readstream
});

rl.on('line', function(line) {
  var statement = JSON.parse(line);
  var obj = {
    actor: crypto.createHash('md5').update(_.get(statement, 'actor.account.name')).digest('hex')
  };
  _.each(fields, function(field) {
    obj[field] = _.get(statement, field);
  });
  csvStream.write(obj);
});
