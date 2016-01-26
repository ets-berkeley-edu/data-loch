var _ = require('lodash');
var config = require('config');
var fs = require('fs');
var moment = require('moment');
var argv = require('yargs')
  .usage('Usage: $0 --max-old-space-size=8192')
  .help('h')
  .alias('h', 'help')
  .argv;

var log = require('../lib/logger');
var redshiftStatements = require('../lib/statements');

// Variable that will keep track of the write streams for every day file
var writeStreams = {}

redshiftStatements.init(config.get('statements'), 'merged', function(statement) {
  // Open a write stream for the day of the statement
  var statementDate = new Date();
  var day = moment(statement.timestamp).utc().format('YYYYMMDD');
  if (!writeStreams[day]) {
    var directory = config.get('storage') || './data/';
    writeStreams[day] = fs.createWriteStream(directory + 'statements/' + day + '.txt');
  }
  // Write the statement
  writeStreams[day].write(JSON.stringify(statement) + '\n');
}, function() {
  _.each(writeStreams, function(writeStream) {
    writeStream.end();
  });
  console.log('ALL DONE!!!');
});
