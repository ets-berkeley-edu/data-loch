var _ = require('lodash');
var config = require('config');
var fs = require('fs');
var moment = require('moment');
var argv = require('yargs')
  .usage('Usage: $0 --max-old-space-size=8192 -c [course_id]')
  .describe('c', 'The Canvas id of the course to generate the statements for')
  .help('h')
  .alias('h', 'help')
  .argv;

var log = require('../lib/logger');
var redshiftData = require('../lib/data');
var redshiftStatements = require('../lib/statements');

if (!argv.c) {
  log.error('A valid course id should be provided');
}

// Extract the provided course ids and convert them to
// string to make them match the format of the extracted CSV files
var course_ids = _.isArray(argv.c) ? argv.c : [argv.c];
course_ids = _.map(course_ids, function(course_id) {
  return '' + course_id;
});

// Variable that will keep track of the supplied courses and their enrollments
var courses = {};
// Variable that will keep track of the generated statements
var statements = [];

/**
 * Get the supplied courses
 *
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var getCourses = function(callback) {
  redshiftData.getCourses('subset', function(course, done) {
    if (_.contains(course_ids, course.canvas_id)) {
      courses['https://bcourses.berkeley.edu/api/v1/courses/' + course.canvas_id] = {
        'course': course,
        'enrollments': {}
      };
    }
    return done();
  }, callback);
};

/**
 * Get the users enrolled in the supplied courses
 *
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var getCourseEnrollments = function(callback) {
  redshiftData.getUsers('subset', function(users) {
    redshiftData.getEnrollments('subset', function(enrollment, done) {
      _.each(courses, function(course, course_id) {
        if (course.course.id === enrollment.course_id) {
          var user = users[enrollment.user_id];
          courses[course_id].enrollments[user.sis_user_id] = user;
        }
      });
      if (courses[enrollment.course_id]) {
        users[enrollment.user_id] = true;
      }
      return done();
    }, callback);
  });
};

/**
 * TODO
 */
var generateStatements = function(callback) {
  redshiftStatements.init(config.get('statements'), 'subset', function(statement) {
    statements.push(statement);
  }, function() {
    // Sort the statements by date
    statements.sort(function(a, b) {
      if (a.timestamp > b.timestamp) {
        return 1;
      } else if (a.timestamp < b.timestamp) {
        return -1;
      } else {
        return 0;
      }
    });

    // Get the last event for every supplied course
    _.each(statements, function(statement) {
      var course = courses[ _.get(statement, 'context.contextActivities.grouping.id')];
      if (course && statement.verb.id !== 'http://adlnet.gov/expapi/verbs/registered') {
        course.course.last_activity = moment(statement.timestamp).utc();
      }
    });

    // Open a write stream for every supplied course
    var writeStreams = {};
    var directory = config.get('storage') || './data/';
    _.each(courses, function(course) {
      writeStreams[course.course.canvas_id] = fs.createWriteStream(directory + 'statements/' + course.course.code + '.txt');
    });

    // Check for every course if the record should be written
    _.each(statements, function(statement) {
      _.each(courses, function(course, course_id) {
        var write = false;
        // A. The event took place in the context of the course
        if (_.get(statement, 'object.id') === course_id || _.get(statement, 'context.contextActivities.grouping.id') === course_id) {
          write = true;
        // B. The event actor is a login or logout event for a student enrolled in the course
        } else if (course.enrollments[statement.actor.account.name] && (statement.verb.id === 'https://brindlewaye.com/xAPITerms/verbs/loggedin' || statement.verb.id === 'https://brindlewaye.com/xAPITerms/verbs/loggedout')) {
          // Only include the statement if it happened after the course started and before it ended
          var timestamp = moment(statement.timestamp).utc();
          var course_start = moment(course.course.created_at).utc();
          var last_activity = course.course.last_activity;
          if (timestamp.isBefore(last_activity) && timestamp.isAfter(course_start)) {
            write = true;
          }
        }

        if (write) {
          writeStreams[course.course.canvas_id].write(JSON.stringify(statement) + '\n');
        }
      });
    });

    // Close the write streams
    _.each(writeStreams, function(writeStream) {
      writeStream.end();
    });
    return callback();
  });
};

getCourses(function() {
  getCourseEnrollments(function() {
    generateStatements(function() {
      log.info('Done !');
    })
  });
})
