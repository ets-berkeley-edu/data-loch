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

var redshiftUtil = require('./util');
var schema = require('./schema');

/* USERS */

/**
 * Get all users
 *
 * @param  {String}       subpath                                                       The folder within the data folder in which to parse the data
 * @param  {Function}     userCallback                                                  Callback function executed for every user if a final `callback` function is provided. Otherwise, this function will act as a standard callback function that returns all users at once
 * @param  {Object}       userCallback.user                                             The parsed user
 * @param  {Number}       userCallback.user.id                                          Unique surrogate id for the user
 * @param  {Number}       userCallback.user.canvas_id                                   Primary key for this user in the Canvas users table
 * @param  {String}       userCallback.user.name                                        Name of the user
 * @param  {Date}         userCallback.user.created_at                                  Timestamp when the user was created in the Canvas system
 * @param  {Number}       userCallback.user.pseudonym_id                                Unique surrogate id for the pseudonym
 * @param  {Date}         userCallback.user.updated_at                                  Timestamp when this pseudonym was last updated in Canvas
 * @param  {String}       userCallback.user.sis_user_id                                 Correlated id for the record for this course in the SIS system (assuming SIS integration is configured)
 * @param  {Function}     [callback]                                                    Standard callback function
 */
var getUsers = module.exports.getUsers = function(subpath, userCallback, callback) {
  getData(arguments, 'users');
};

/* COURSES */

/**
 * Get all courses
 *
 * @param  {String}       subpath                                                       The folder within the data folder in which to parse the data
 * @param  {Function}     courseCallback                                                Callback function executed for every course if a final `callback` function is provided. Otherwise, this function will act as a standard callback function that returns all courses at once
 * @param  {Object}       courseCallback.course                                         The parsed course
 * @param  {Number}       courseCallback.course.id                                      Unique surrogate id for a course
 * @param  {Number}       courseCallback.course.canvas_id                               Primary key for this course in the canvas courses table
 * @param  {Number}       courseCallback.course.enrollment_term_id                      Foreign key to enrollment term table
 * @param  {String}       courseCallback.course.name                                    The friendly name of the course
 * @param  {Date}         courseCallback.course.created_at                              Timestamp when the course object was created in Canvas
 * @param  {Date}         courseCallback.course.start_at                                Timestamp for when the course starts
 * @param  {Date}         courseCallback.course.conclude_at                             Timestamp for when the course finishes
 * @param  {Function}     [callback]                                                    Standard callback function
 */
var getCourses = module.exports.getCourses = function(subpath, courseCallback, callback) {
  getData(arguments, 'courses');
};

/**
 * Get all course sections
 *
 * @param  {String}       subpath                                                       The folder within the data folder in which to parse the data
 * @param  {Function}     sectionCallback                                               Callback function executed for every course section if a final `callback` function is provided. Otherwise, this function will act as a standard callback function that returns all course sections at once
 * @param  {Object}       sectionCallback.section                                       The parsed section
 * @param  {Number}       sectionCallback.section.id                                    Unique surrogate id for the course section
 * @param  {Number}       sectionCallback.section.canvas_id                             Primary key for this record in the Canvas course_sections table
 * @param  {String}       sectionCallback.section.name                                  Name of the section
 * @param  {Number}       sectionCallback.section.course_id                             Foreign key to the associated course
 * @param  {Number}       sectionCallback.section.enrollment_term_id                    Foreign key to the associated enrollment term
 * @param  {Date}         sectionCallback.section.start_at                              Section start date
 * @param  {Date}         sectionCallback.section.end_at                                Section end date
 * @param  {Date}         sectionCallback.section.created_at                            Timestamp for when this section was entered into the system
 * @param  {String}       sectionCallback.section.sis_source_id                         Id for the correlated record for the section in the SIS (assumming SIS integration has been properly configured)
 * @param  {Function}     [callback]                                                    Standard callback function
 */
var getSections = module.exports.getSections = function(subpath, sectionCallback, callback) {
  getData(arguments, 'course_sections');
};

/**
 * Get all course enrollments
 *
 * @param  {String}       subpath                                                       The folder within the data folder in which to parse the data
 * @param  {Function}     enrollmentCallback                                            Callback function executed for every course enrollment if a final `callback` function is provided. Otherwise, this function will act as a standard callback function that returns all course enrollments at once
 * @param  {Object}       enrollmentCallback.enrollment                                 The parsed enrollment
 * @param  {Number}       enrollmentCallback.enrollment.id                              Unique surrogate id for the enrollment
 * @param  {Number}       enrollmentCallback.enrollment.canvas_id                       Primary key for this record in the Canvas enrollments table
 * @param  {Number}       enrollmentCallback.enrollment.course_section_id               Foreign key to the course section for this enrollment
 * @param  {Number}       enrollmentCallback.enrollment.role_id                         Foreign key to the role of the person enrolled in the course
 * @param  {String}       enrollmentCallback.enrollment.type                            Enrollment type: TaEnrollment, DesignerEnrollment, StudentEnrollment, TeacherEnrollment, StudentViewEnrollment, ObserverEnrollment
 * @param  {Date}         enrollmentCallback.enrollment.created_at                      Timestamp for when this section was entered into the system
 * @param  {Date}         enrollmentCallback.enrollment.start_at                        Enrollment start date
 * @param  {Date}         enrollmentCallback.enrollment.end_at                          Enrollment end date
 * @param  {Date}         enrollmentCallback.enrollment.completed_at                    Enrollment completed date
 * @param  {String}       enrollmentCallback.enrollment.sis_source_id                   Id of correlated enrollment in the SIS (assuming the SIS is configured properly)
 * @param  {Number}       enrollmentCallback.enrollment.course_id                       Foreign key to course for this enrollment
 * @param  {Number}       enrollmentCallback.enrollment.user_id                         Foreign key to user for the enrollment
 * @param  {Number}       enrollmentCallback.enrollment.enrollment_term_id              Foreign key for the attributes of the enrollment
 * @param  {Number}       enrollmentCallback.enrollment.computed_final_score            Final score for the enrollment
 * @param  {Number}       enrollmentCallback.enrollment.computed_current_score          Current score for the enrollment
 * @param  {Function}     [callback]                                                    Standard callback function
 */
var getEnrollments = module.exports.getEnrollments = function(subpath, enrollmentCallback, callback) {
  getData(arguments, 'enrollments');
};

/* DISCUSSIONS */

/**
 * Get all discussion topics
 *
 * @param  {String}       subpath                                                       The folder within the data folder in which to parse the data
 * @param  {Function}     discussionCallback                                            Callback function executed for every discussion topic if a final `callback` function is provided. Otherwise, this function will act as a standard callback function that returns all discussion topics at once
 * @param  {Object}       discussionCallback.discussion                                 The parsed discussion topic
 * @param  {Number}       discussionCallback.discussion.id                              Unique surrogate id for the discussion topic
 * @param  {Number}       discussionCallback.discussion.canvas_id                       Primary key to the discussion_topics table in Canvas
 * @param  {String}       discussionCallback.discussion.title                           Title of the discussion topic
 * @param  {String}       discussionCallback.discussion.message                         Message text for the discussion topic
 * @param  {String}       discussionCallback.discussion.type                            Discussion topic type. Two types are default (blank) and announcement
 * @param  {Date}         discussionCallback.discussion.created_at                      Timestamp when the discussion topic was first saved in the system
 * @param  {String}       discussionCallback.discussion.discussion_type                 Type of discussion topic: default(blank), side_comment, threaded. threaded indicates that replies are threaded where side_comment indicates that replies in the discussion are flat. See related Canvas Guide https://guides.instructure.com/m/4152/l/60423-how-do-i-create-a-threaded-discussion
 * @param  {Number}       discussionCallback.discussion.course_id                       Foreign key to the course dimension
 * @param  {Number}       discussionCallback.discussion.enrollment_term_id              Foreign Key to enrollment term table
 * @param  {Number}       discussionCallback.discussion.user_id                         Foreign key to the user dimension for the user that created the discussion topic
 * @param  {Number}       discussionCallback.discussion.assignment_id                   Foreign key to the assignment dimension
 * @param  {Function}     [callback]                                                    Standard callback function
 */
var getDiscussions = module.exports.getDiscussions = function(subpath, discussionCallback, callback) {
  getData(arguments, 'discussion_topics');
};

/**
 * Get all discussion entries
 *
 * @param  {String}       subpath                                                       The folder within the data folder in which to parse the data
 * @param  {Function}     entriesCallback                                               Callback function executed for every discussion entry if a final `callback` function is provided. Otherwise, this function will act as a standard callback function that returns all discussion entries at once
 * @param  {Object}       entriesCallback.entry                                         The parsed discussion entry
 * @param  {Number}       entriesCallback.entry.id                                      Unique surrogate id for the discussion entry
 * @param  {Number}       entriesCallback.entry.canvas_id                               Primary key for this record in the Canvas discussion_entries table
 * @param  {String}       entriesCallback.entry.message                                 Full text of the entry's message
 * @param  {Date}         entriesCallback.entry.created_at                              Timestamp when the discussion entry was created
 * @param  {Number}       entriesCallback.entry.parent_discussion_entry_id              Foreign key to the reply that it is nested underneath
 * @param  {Number}       entriesCallback.entry.user_id                                 Foreign key to the user that created this entry
 * @param  {Number}       entriesCallback.entry.topic_id                                Foreign key to associated discussion topic
 * @param  {Number}       entriesCallback.entry.course_id                               Foreign key to associated course
 * @param  {Number}       entriesCallback.entry.enrollment_term_id                      Foreign Key to enrollment term table
 * @param  {Function}     [callback]                                                    Standard callback function
 */
var getDiscussionEntries = module.exports.getDiscussionEntries = function(subpath, entriesCallback, callback) {
  getData(arguments, 'discussion_entries');
};

/* ASSIGNMENTS */

/**
 * Get all assignments
 *
 * @param  {String}       subpath                                                       The folder within the data folder in which to parse the data
 * @param  {Function}     assignmentCallback                                            Callback function executed for every assignment if a final `callback` function is provided. Otherwise, this function will act as a standard callback function that returns all assignments at once
 * @param  {Object}       assignmentCallback.assignment                                 The parsed assignment
 * @param  {Number}       assignmentCallback.assignment.id                              Unique surrogate id for the assignment
 * @param  {Number}       assignmentCallback.assignment.canvas_id                       Primary key for this record in the Canvas assignments table
 * @param  {Number}       assignmentCallback.assignment.course_id                       Foreign key to the course associated with this assignment
 * @param  {String}       assignmentCallback.assignment.title                           Title of the assignment
 * @param  {String}       assignmentCallback.assignment.description                     Long description of the assignment
 * @param  {Date}         assignmentCallback.assignment.due_at                          Timestamp for when the assignment is due
 * @param  {Number}       assignmentCallback.assignment.points_possible                 Total points possible for the assignment
 * @param  {String}       assignmentCallback.assignment.grading_type                    Describes how the assignment will be graded (gpa_scale, pass_fail, percent, points, not_graded, letter_grade)
 * @param  {String}       assignmentCallback.assignment.submission_types                Comma separated list of valid methods for submitting the assignment (online_url, media_recording, online_upload, online_quize, external_tool, online_text_entry, online_file_upload)
 * @param  {Date}         assignmentCallback.assignment.created_at                      Timestamp of the first time the assignment was entered into the system
 * @param  {Boolean}      assignmentCallback.assignment.peer_reviews                    True if peer reviews are enabled for this assignment
 * @param  {Number}       assignmentCallback.assignment.enrollment_term_id              Foreign Key to enrollment term table
 * @param  {Function}     [callback]                                                    Standard callback function
 */
var getAssignments = module.exports.getAssignments = function(subpath, assignmentCallback, callback) {
  getData(arguments, 'assignments');
};

/**
 * Get all assignment submissions
 *
 * @param  {String}       subpath                                                       The folder within the data folder in which to parse the data
 * @param  {Function}     submissionCallback                                            Callback function executed for every assignment submission if a final `callback` function is provided. Otherwise, this function will act as a standard callback function that returns all assignment submissions at once
 * @param  {Object}       submissionCallback.submission                                 The parsed submission
 * @param  {Number}       submissionCallback.submission.id                              Unique surrogate id for the submission
 * @param  {Number}       submissionCallback.submission.canvas_id                       Primary key of this record in the Canvas submissions table
 * @param  {String}       submissionCallback.submission.body                            Text content for the submission
 * @param  {String}       submissionCallback.submission.url                             URL content for the submission
 * @param  {String}       submissionCallback.submission.grade                           Letter grade mapped from the score by the grading scheme
 * @param  {Date}         submissionCallback.submission.submitted_at                    Timestamp of when the submission was submitted
 * @param  {String}       submissionCallback.submission.submission_type                 Type of subimission (online_url, media_recording, online_upload, online_quize, external_tool, online_text_entry, online_file_upload, discussion_topic)
 * @param  {Date}         submissionCallback.submission.created_at                      Timestamp of when the submission was created
 * @param  {Boolean}      submissionCallback.submission.has_rubric_assessment           TBD
 * @param  {Number}       submissionCallback.submission.assignment_id                   Foreign key to assignment dimension
 * @param  {Number}       submissionCallback.submission.course_id                       Foreign key to course dimension of course associated with the assignment
 * @param  {Number}       submissionCallback.submission.enrollment_term_id              Foreign Key to enrollment term table
 * @param  {Number}       submissionCallback.submission.user_id                         Foreign key to user dimension of user who submitted the assignment
 * @param  {Number}       submissionCallback.submission.grader_id                       Foreign key to the user dimension of user who graded the assignment
 * @param  {Number}       submissionCallback.submission.score                           Numeric grade given to the submission
 * @param  {Number}       submissionCallback.submission.published_score                 TBD
 * @param  {Function}     [callback]                                                    Standard callback function
 */
var getAssignmentSubmissions = module.exports.getAssignmentSubmissions = function(subpath, submissionCallback, callback) {
  getData(arguments, 'submissions');
};

/**
 * Get all assignment submission comments
 *
 * @param  {String}       subpath                                                       The folder within the data folder in which to parse the data
 * @param  {Function}     commentCallback                                               Callback function executed for every assignment submission comment if a final `callback` function is provided. Otherwise, this function will act as a standard callback function that returns all assignment submission comments at once
 * @param  {Object}       commentCallback.comment                                       The parsed assignment submission comment
 * @param  {Number}       commentCallback.comments[id].id                               TBD
 * @param  {Number}       commentCallback.comments[id].canvas_id                        TBD
 * @param  {Number}       commentCallback.comments[id].submission_id                    TBD
 * @param  {Number}       commentCallback.comments[id].recipient_id                     TBD
 * @param  {Number}       commentCallback.comments[id].author_id                        TBD
 * @param  {String}       commentCallback.comments[id].comment                          TBD
 * @param  {Date}         commentCallback.comments[id].created_at                       TBD
 * @param  {Number}       commentCallback.comments[id].assignment_id                    Foreign key to assignment dimension
 * @param  {Number}       commentCallback.comments[id].course_id                        Foreign key to course dimension of course associated with the assignment
 * @param  {Number}       commentCallback.comments[id].commenter_user_id                TBD
 * @param  {Function}     [callback]                                                    Standard callback function
 */
var getAssignmentSubmissionComments = module.exports.getAssignmentSubmissionComments = function(subpath, commentCallback, callback) {
  getData(arguments, 'submission_comments');
};

/* FILES */

var fileCache = {};

/**
 * Get a file. As this information is not available via Canvas Data, a REST API call will be performed instead
 *
 * @param  {String}       id                                                            The id of the file to retrieve
 * @param  {Function}     callback                                                      Standard callback function
 * @param  {Object}       callback.err                                                  An error that occurred, if any
 * @param  {Object}       callback.file                                                 The retrieved file
 * @param  {Number}       [callback.course_id]                                          The id of the course in which the file resides
 */
var getFile = module.exports.getFile = function(id, callback) {
  // Check if the file has already been requests
  if (fileCache[id]) {
    return callback(null, fileCache[id].file, fileCache[id].course_id);
  }

  // Get the file information
  redshiftUtil.canvasApiRequest('/api/v1/files/' + id, function(err, file) {
    // Get the information about the folder the file resides in
    redshiftUtil.canvasApiRequest('/api/v1/folders/' + file.folder_id, function(err, folder) {
      var course_id = null;
      if (folder.context_type === 'Course') {
        course_id = folder.context_id;
      }
      // Cache the file
      fileCache[id] = {
        'file': file,
        'course_id': course_id
      };
      return callback(err, file, course_id);
    });
  });
};

/* REQUESTS */

/**
 * Get all requests
 *
 * @param  {String}       subpath                                                       The folder within the data folder in which to parse the data
 * @param  {Function}     requestCallback                                               Callback function executed for every request if a final `callback` function is provided. Otherwise, this function will act as a standard callback function that returns all requests at once
 * @param  {Object}       requestCallback.request                                       The parsed request
 * @param  {String}       requestCallback.request.id                                    The request ID assigned by the canvas system
 * @param  {Date}         requestCallback.request.timestamp                             Timestamp when the request was made in UTC
 * @param  {String}       requestCallback.request.timestamp_year                        Year when the request was made
 * @param  {String}       requestCallback.request.timestamp_month                       Month when the request was made
 * @param  {String}       requestCallback.request.timestamp_day                         Day when the request was made
 * @param  {Number}       requestCallback.request.user_id                               The foreign key in user_dim for the user that made the request
 * @param  {Number}       requestCallback.request.course_id                             The foreign key in course_dim for the course that owned the page requested (NULL if not applicable)
 * @param  {Number}       requestCallback.request.root_account_id                       The foreign key in account_dim for the root account on which this request was made
 * @param  {Number}       requestCallback.request.course_account_id                     The foreign key in account_dim for the account the associated course is owned by
 * @param  {Number}       requestCallback.request.quiz_id                               Quiz foreign key if page request is for a quiz, otherwise NULL
 * @param  {Number}       requestCallback.request.discussion_id                         Discussion foreign key if page request is for a discussion, otherwise NULL
 * @param  {Number}       requestCallback.request.conversation_id                       Conversation foreign key if page request is for a conversation, otherwise NULL
 * @param  {Number}       requestCallback.request.assignment_id                         Assignment foreign key if page request is for an assingnment, otherwise NULL
 * @param  {String}       requestCallback.request.url                                   First 256 characters of the requested URL
 * @param  {String}       requestCallback.request.user_agent                            First 256 characters of the User Agent header received from the users browser/client software
 * @param  {String}       requestCallback.request.http_method                           HTTP method/verb (GET,PUT,POST etc.) that was sent with the request
 * @param  {String}       requestCallback.request.remote_ip                             IP address that was recorded from the request
 * @param  {Number}       requestCallback.request.interaction_micros                    Total time required to service the request in microseconds
 * @param  {String}       requestCallback.request.web_application_controller            The controller the Canvas web application used to service this request
 * @param  {String}       requestCallback.request.web_application_action                The controller the Canvas web application used to service this request
 * @param  {String}       requestCallback.request.web_application_context_type          The containing object type the Canvas web application used to service this request
 * @param  {String}       requestCallback.request.web_application_context_id            The containing object's id the Canvas web application used to service this request
 * @param  {Function}     [callback]                                                    Standard callback function
 */
var getRequests = module.exports.getRequests = function(subpath, requestCallback, callback) {
  getData(arguments, 'requests');
};

/* UTIL */

/**
 * Get all data in a schema file or loop through all rows individually
 *
 * @param  {Object[]}     args                                                          The arguments passed into the caller function
 * @param  {String}       args[0]                                                       The folder within the data folder in which to parse the data
 * @param  {Function}     args[1]                                                       Callback function executed for every row if an `args[2]` `callback` function is provided. Otherwise, this function will act as a standard callback function that returns all data at once
 * @param  {Function}     [args[2]]                                                     Standard callback function
 * @param  {String}       schemaName                                                    The name of the schema file to parse as per the top level keys in `schema.js`
 * @api private
 */
var getData = function(args, schemaName) {
  if (args.length === 3) {
    loopData(args[0], schemaName, args[1], args[2]);
  } else {
    getAllData(args[0], schemaName, args[1]);
  }
};

/**
 * Get all data in a schema file
 *
 * @param  {String}       subpath                                                       The folder within the data folder in which to parse the data
 * @param  {String}       schemaName                                                    The name of the schema file to parse as per the top level keys in `schema.js`
 * @param  {Function}     callback                                                      Standard callback function
 * @param  {Object}       callback.data                                                 Retrieved data from the schema file keyed by the id for each data row
 * @api private
 */
var getAllData = function(subpath, schemaName, callback) {
  var items = {};

  // Loop through all records in the schema file and collect them in a single object
  loopData(subpath, schemaName, function(obj, done) {
    items[obj.id] = obj;
    return done();
  }, function() {
    return callback(items);
  });
};

/**
 * Loop through the rows in a schema file one by one
 *
 * @param  {String}       subpath                                                       The folder within the data folder in which to parse the data
 * @param  {String}       schemaName                                                    The name of the schema file to parse as per the top level keys in `schema.js`
 * @param  {Function}     dataCallback                                                  Callback function executed for every data row
 * @param  {Object}       dataCallback.data                                             The parsed data row containing the properties as defined in the mappings for the schema file
 * @param  {Function}     callback                                                      Standard callback function
 * @api private
 */
var loopData = function(subpath, schemaName, dataCallback, callback) {
  // Loop through the records one by one and pass them to the data row callback function
  redshiftUtil.parseCSVFiles(subpath + '/' + schemaName, function(row, done) {
    var obj = convertRowToObject(row, schema[schemaName]);
    dataCallback(obj, function(err) {
      return done();
    });
  }, function() {
    return callback();
  });
};

/**
 * Convert a data row to an object using the properties as defined in the mappings for the schema file
 *
 * @param  {Object[]}     row                                                           The data row to convert to an object
 * @param  {Object}       schema                                                        The mapping for the schema file as defined in `schema.js`
 * @return {Function}                                                                   The converted object
 */
var convertRowToObject = function(row, schema) {
  var object = {};
  // Apply all properties to the parsed object
  var index = 0;
  _.each(schema, function(file) {
    _.each(file.mapping, function(field) {
      var key = _.keys(field)[0];
      var value = row[index];
      if (field[key] === true) {
        object[key] = (value === '\\N' ? null : value);
        index++;
      }
    });
  });
  return object;
};
