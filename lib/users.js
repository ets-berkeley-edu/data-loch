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

var redshiftUtil = require('./util');

// Variable that will keep track of the parsed term file(s)
var parsedTerms = {};
// Variable that will keep track of the parsed course file(s)
var parsedCourses = {};
// Variable that will keep track of the parsed course section file(s)
var parsedSections = {};

// Variable that will keep track of the constructed user, pseudonym,
// course, section and role tree
var userTree = {};

// Variable that will keep track of the folder within the data folder where
// the data files reside
var subpath = '';

/**
 * Build a tree of all users that contains their pseudonyms, courses, sections
 * and roles
 *
 * @param  {String}       [subpath]             The folder within the data folder in which the data files reside
 * @param  {Function}     callback              Standard callback function
 * @param  {Object}       callback.userTree     The generated user tree
 */
var buildUserTree = module.exports.buildUserTree = function(_subpath, callback) {
  if (_subpath) {
    subpath = _subpath + '/';
  }

  parseUsers(function() {
    parsePseudonyms(function() {
      parseTerms(function() {
        parseCourses(function() {
          parseSections(function() {
            parseEnrollments(function() {
              console.log('Constructed user tree');
              return callback(userTree);
            });
          });
        });
      });
    });
  });
};

/**
 * Parse the user data file and add all users to the user tree
 *
 * @param  {Function}     callback              Standard callback function
 * @api private
 */
var parseUsers = function(callback) {
  redshiftUtil.parseDataFiles(subpath + 'user_dim', function(users) {
    _.each(users, function(user) {
      var parsedUser = parseRow(user, {
        id: 0,
        canvas_id: 1,
        name: 3,
        time_zone: 4,
        created_at: 5
      });
      parsedUser.pseudonyms = [];
      parsedUser.terms = {};
      // Store the user in the user tree
      userTree[parsedUser.id] = parsedUser;
    });
    users = null;
    return callback();
  });
};

/**
 * Parse the pseudonym data files and connect them to the users in the user tree
 *
 * @param  {Function}     callback              Standard callback function
 * @api private
 */
var parsePseudonyms = function(callback) {
  redshiftUtil.parseDataFiles(subpath + 'pseudonym_dim', function(pseudonyms) {
    _.each(pseudonyms, function(pseudonym) {
      var parsedPseudonym = parseRow(pseudonym, {
        id: 0,
        user_id: 2,
        workflow_state: 4,
        last_request_at: 5,
        last_login_at: 6,
        sis_user_id: 15,
        unique_name: 16
      });
      // Store the pseudonym under the corresponding user
      userTree[parsedPseudonym.user_id].pseudonyms.push(parsedPseudonym);
    });
    pseudonyms = null;
    return callback();
  });
};

/**
 * Parse the term data file and cache the terms
 *
 * @param  {Function}     callback              Standard callback function
 * @api private
 */
var parseTerms = function(callback) {
  redshiftUtil.parseDataFiles(subpath + 'enrollment_term_dim', function(terms) {
    _.each(terms, function(term) {
      var parsedTerm = parseRow(term, {id: 0, canvas_id: 1, name: 3, sis_source_id: 6});
      // Store the term in the parsed terms
      parsedTerms[parsedTerm.id] = parsedTerm;
    });
    terms = null;
    return callback();
  });
};

/**
 * Parse the course data files and cache the courses
 *
 * @param  {Function}     callback              Standard callback function
 * @api private
 */
var parseCourses = function(callback) {
  redshiftUtil.parseDataFiles(subpath + 'course_dim', function(courses) {
    _.each(courses, function(course) {
      var parsedCourse = parseRow(course, {
        id: 0,
        canvas_id: 1,
        enrollment_term_id: 4,
        name: 5,
        code: 6,
        created_at: 8,
        sis_source_id: 12
      });
      // Store the course in the parsed courses
      parsedCourses[parsedCourse.id] = parsedCourse;
    });
    courses = null;
    return callback();
  });
};

/**
 * Parse the course section data files and cache the course sections
 *
 * @param  {Function}     callback              Standard callback function
 * @api private
 */
var parseSections = function(callback) {
  redshiftUtil.parseDataFiles(subpath + 'course_section_dim', function(sections) {
    _.each(sections, function(section) {
      var parsedSection = parseRow(section, {id: 0, canvas_id: 1, name: 2, course_id: 3, sis_source_id: 15});
      // Store the section in the parsed sections
      parsedSections[parsedSection.id] = parsedSection;
    });
    sections = null;
    return callback();
  });
};

/**
 * Parse the enrollment data files and connect the terms, courses, sections and roles to each user
 *
 * @param  {Function}     callback              Standard callback function
 * @api private
 */
var parseEnrollments = function(callback) {
  // Parse the enrollments
  redshiftUtil.parseDataFiles(subpath + 'enrollment_dim', function(enrollments) {
    var parsedEnrollments = {};
    _.each(enrollments, function(enrollment, index) {
      var parsedEnrollment = parseRow(enrollment, {id: 0, type: 5, workflow_state: 6});
      parsedEnrollments[parsedEnrollment.id] = parsedEnrollment;
    });

    // Parse the enrollment facts
    redshiftUtil.parseDataFiles(subpath + 'enrollment_fact', function(enrollmentFacts) {
      _.each(enrollmentFacts, function(enrollmentFact) {

        var parsedEnrollmentFact = parseRow(enrollmentFact, {
          enrollment_id: 0,
          user_id: 1,
          course_id: 2,
          enrollment_term_id: 3,
          course_section_id: 4
        });
        // Attach the term, course, section and role for each enrollment to the corresponding user
        var userId = parsedEnrollmentFact.user_id;
        var termId = parsedEnrollmentFact.enrollment_term_id;
        var term = _.extend({}, parsedTerms[termId]);
        term.courses = {};
        var courseId = parsedEnrollmentFact.course_id;
        var course = _.extend({}, parsedCourses[courseId]);
        course.sections = {};
        var sectionId = parsedEnrollmentFact.course_section_id;
        var section = _.extend({}, parsedSections[sectionId], parsedEnrollments[parsedEnrollmentFact.enrollment_id]);

        userTree[userId].terms[termId] = userTree[userId].terms[termId] || term;
        userTree[userId].terms[termId].courses[courseId] = userTree[userId].terms[termId].courses[courseId] || course;
        userTree[userId].terms[termId].courses[courseId].sections[sectionId] = section;

      });

      enrollments = null;
      enrollmentFacts = null;

      return callback();
    });
  });
};

/**
 * Convert a parsed row from the CSV data files into an object
 *
 * @param  {Object[]}     row                   The parsed CSV row to convert to an object
 * @param  {Object}       fields                Object where the keys represent the names of the fields to use in the converted row object, and the keys represent the index of the corresponding field in the parsed CSV row
 * @return {Object}                             The converted row object
 * @api private
 */
var parseRow = function(row, fields) {
  var parsedRow = {};
  _.each(fields, function(index, name) {
    parsedRow[name] = row[index];
  });
  return parsedRow;
};
