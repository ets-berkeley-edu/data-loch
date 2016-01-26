var _ = require('lodash');
var config = require('config');
var crypto = require('crypto');
var csv = require('fast-csv');
var fs = require('fs');
var readline = require('readline');

var fields = ['id', 'timestamp', 'verb.id', 'verb.display.en-US', 'object.objectType', 'object.id', 'object.definition.type', 'object.definition.name.en-US', 'object.definition.description.en-US',
              'object.definition.extensions.https://canvas.instructure.com/xapi/assignments/submissions_types', 'result.response', 'result.completion', 'result.score.raw', 'result.score.scaled',
              'result.score.min', 'result.score.max', 'context.platform', 'context.contextActivities.grouping.objectType', 'context.contextActivities.grouping.definition.name.en-US',
              'context.contextActivities.grouping.id', 'context.contextActivities.parent.objectType', 'context.contextActivities.parent.id'];

var directory = config.get('storage') || './data/';

var csvStream = csv.createWriteStream({headers: true});
var writableStream = fs.createWriteStream(directory + 'statements/ESPM 50AC - LEC 001.csv');
csvStream.pipe(writableStream);

var readstream = fs.createReadStream(directory + 'statements/ESPM 50AC - LEC 001.txt');
var rl = readline.createInterface({
  'input': readstream
});

rl.on('line', function(line) {
  var statement = JSON.parse(line);
  var obj = {
    'actor': crypto.createHash('md5').update(_.get(statement, 'actor.account.name')).digest('hex')
  };
  _.each(fields, function(field) {
    obj[field] = _.get(statement, field);
  });
  csvStream.write(obj);
});
