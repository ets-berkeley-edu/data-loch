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
var async = require('async');
var config = require('config');
var csv = require('fast-csv');
var fs = require('fs');
var glob = require('glob');

var log = require('../lib/logger');
var redshiftData = require('../lib/data');
var schema = require('../lib/schema');

var argv = require('yargs')
  .usage('Usage: $0 --max-old-space-size=8192 -c [course_id]')
  .describe('c', 'The Canvas id of the course to extract the subset for')
  .help('h')
  .alias('h', 'help')
  .argv;

if (!argv.c) {
  log.error('A valid course id should be provided');
}

// Extract the provided course ids and convert them to
// string to make them match the format of the extracted CSV files
var course_ids = _.isArray(argv.c) ? argv.c : [ argv.c ];
course_ids = _.map(course_ids, function(course_id) {
  return '' + course_id;
});

// Variable that will keep track of the ids of the requested courses
var courses = {};
// Variable that will keep track of the ids of the users enrolled in the courses
var users = {};

/**
 * Get the supplied courses
 *
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var extractCourses = function(callback) {
  redshiftData.getCourses('merged', function(course, done) {
    if (_.contains(course_ids, course.canvas_id)) {
      courses[course.id] = true;
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
var extractCourseEnrollments = function(callback) {
  redshiftData.getEnrollments('merged', function(enrollment, done) {
    if (courses[enrollment.course_id]) {
      users[enrollment.user_id] = true;
    }
    return done();
  }, callback);
};

/**
 * Filter all merged Canvas data files down to the records related to the supplied courses.
 * Note that this excludes all files that are unused for statement generation
 *
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsets = function(callback) {
  // Generate subsets for the users
  generateSubsetUsers(function() {
    // Generate subsets for the courses
    generateSubsetCourses(function() {
      // Generate subsets for the assignments
      generateSubsetAssignments(function() {
        // Generate subsets for the discussion data files
        generateSubsetDiscussions(function() {
          return callback();
        });
      });
    });
  });
};

/**
 * Filter all user-related merged Canvas data files down to the records related to the supplied courses
 *
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetUsers = function(callback) {
  // Generate a subset of the `users` file
  generateSubset('users', 'id', users, function() {
    // Generate a subset of the `enrollments` file
    generateSubset('enrollments', 'course_id', courses, function() {
      return callback();
    });
  });
};

/**
 * Filter all course-related merged Canvas data files down to the records related to the supplied courses
 *
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetCourses = function(callback) {
  // Generate a subset of the `courses` file
  generateSubset('courses', 'id', courses, function() {
    // Generate a subset of the `course_sections` file
    generateSubset('course_sections', 'course_id', courses, function() {
      return callback();
    });
  });
};

/**
 * Filter all assignment-related merged Canvas data files down to the records related to the supplied courses
 *
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetAssignments = function(callback) {
  // Generate a subset of the `assignments` file
  generateSubset('assignments', 'course_id', courses, function() {
    // Generate a subset of the `submissions` files
    generateSubset('submissions', 'course_id', courses, function() {
      // Generate a subset of the `submission_comments` files
      generateSubset('submission_comments', 'course_id', courses, function() {
        return callback();
      });
    });
  });
};

/**
 * Filter all discussion-related merged Canvas data files down to the records related to the supplied courses
 *
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetDiscussions = function(callback) {
  // Generate a subset of the `discussion_topics` file
  generateSubset('discussion_topics', 'course_id', courses, function() {
    // Generate a subset of the `discussion_entries` file
    generateSubset('discussion_entries', 'course_id', courses, function() {
      return callback();
    });
  });
};

/**
 * Generate a subset for a merged Canvas data file file based on a set of ids to filter by
 * and write the subset to a new merged Canvas data file
 *
 * @param  {String}         file                                The base name to the data file(s) that need to be filtered to the provided subpopulation
 * @param  {String}         filterField                         The name of the field on which the rows should be filtered
 * @param  {Object|String}  filter                              The list of filter values to allow
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubset = function(file, filterField, filter, callback) {
  // When a string value has been provided as the filter value, convert
  // it into a hash filter object to allow for using the same lookup
  // strategy as when filtering on multiple values
  var rowFilter = filter;
  if (_.isString(filter)) {
    rowFilter = {};
    rowFilter[filter] = true;
  }

  var filterIndex = getFieldIndex(file, filterField);
  subset(file, function(row) {
    return rowFilter[row[filterIndex]];
  }, callback);
};

/**
 * Filter all request-related Canvas data files down to the records related to the supplied courses
 *
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetRequests = function(callback) {
  var courseIndex = getFieldIndex('requests', 'course_id');
  var userIndex = getFieldIndex('requests', 'user_id');
  var urlIndex = getFieldIndex('requests', 'url');

  subset('requests', function(row) {
    if (courses[row[courseIndex]]) {
      return true;
    } else if (users[row[userIndex]] && row[urlIndex].indexOf('/login/') === 0) {
      return true;
    } else {
      return false;
    }
  }, callback);
};

/**
 * Extract the index of a field in a merged Canvas data file row
 * based on the schema defined in `schema.js`
 *
 * @param  {String}         file                                The name of the merged Canvas data file per the top level properties in `schema.js`
 * @param  {String}         fieldName                           The name of the field to get the index for
 * @return {Number}                                             The index of the requested field in the requested merged Canvas data field
 * @api private
 */
var getFieldIndex = function(file, fieldName) {
  var fieldIndex = 0;
  var index = 0;
  _.each(schema[file], function(schemaFile) {
    _.each(schemaFile.mapping, function(field) {
      var key = _.keys(field)[0];
      if (field[key] === true) {
        if (key === fieldName) {
          fieldIndex = index;
        }
        index++;
      }
    });
  });
  return fieldIndex;
};

/**
 * Filter the rows in a Canvas data file and write the results to a new CSV file
 *
 * @param  {String}         file                                The name of the merged Canvas data file per the top level properties in `schema.js`
 * @param  {Function}       rowFilter                           Filter function that will return `true` if the row should be retained or `false` if the row should be filtered out
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var subset = function(file, rowFilter, callback) {
  var retainedRows = 0;
  var totalRows = 0;

  var directory = config.get('storage') || './data/';
  glob(directory + 'merged/' + file + '*', null, function(err, paths) {
    async.eachSeries(paths, function(path, done) {
      var start = Date.now();
      log.info('Processing ' + path);
      // Run through all of the rows in the requested data file, filter out the
      // rows and write the results to a new CSV file
      csv.fromPath(path, {delimiter: '\t', quote: null})
        .transform(function(row) {
          totalRows++;
          var retain = rowFilter(row);
          if (retain) {
            retainedRows++;
            return row;
          } else {
            return null;
          }
        })
        .pipe(csv.createWriteStream({delimiter: '\t', quote: null}))
        .pipe(fs.createWriteStream(directory + 'subset/' + file, {flags: 'a'}))
        .on('finish', function() {
          var end = Date.now();
          log.info({duration: start - end}, 'Finished processing ' + path);
          return done();
        });
    }, function() {
      log.info({file: file, total: totalRows, retained: retainedRows}, 'Finished generating a subset for a file');
      return callback();
    });
  });
};

extractCourses(function() {
  extractCourseEnrollments(function() {
    generateSubsets(function() {
      // generateSubsetRequests(function() {
      log.info('DONE!');
      // });
    });
  });
});
