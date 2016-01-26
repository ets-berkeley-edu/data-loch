var _ = require('lodash');
var async = require('async');
var config = require('config');
var fs = require('fs');
var moment = require('moment');
var xapicaliper = require('xapicaliper');

var log = require('./logger');
var RedshiftData = require('./data');
var RedshiftUtil = require('./util');

// Variable that will keep track of the full user list
var users = null;
// Variable that will keep track of the full course list
var courses = null;
// Variable that will keep track of the full discussion list
var discussions = null;
// Variable that will keep track of the full assignment list
var assignments = null;
// Variable that will keep track of the full assignment submission list
var assignmentSubmissions = null;

// Variable that will keep track of the configuration
var config = null;
// Variable that will keep track of the folder from which to load the Canvas data
var subpath = null;
// Function that will be called when a statement has been generated
var statementCallback = null;

// Variable that will keep track of all generated statement ids
var statementIds = {};

// User id that will be used to use the dummy user
var NODATA_USER_ID = 0;
// Dummy user that will be used when insufficient user information is available
var nodata_user = null;

/* COURSE */

/**
 * Process all course related learning activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processCourses = function(callback) {
  processCourseCreations(function() {
    processCourseEnrollments(function() {
      processCourseUnenrollments(function() {
        return callback();
      });
    });
  });
};

/**
 * Process all course creation activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processCourseCreations = function(callback) {
  // As there is no data about who created the course, add the dummy
  // user as the creator
  _.each(courses, function(course) {
    course.user_id = NODATA_USER_ID;
  });

  processStatements('Course creations', courses, xapicaliper.course.create, {
    'metadata': function(course, _course, callback) {
      return callback({
        'id': generateCanvasAPIURL('courses/' + course.canvas_id),
        'name': course.code,
        'description': course.name,
        'start': (course.start_at ? moment.utc(course.start_at).toDate() : null),
        'end': (course.conclude_at ? moment.utc(course.conclude_at).toDate() : null)
      });
    }
  }, callback);
};

/**
 * Process all course enrollment activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processCourseEnrollments = function(callback) {
  RedshiftData.getEnrollments(subpath, function(enrollment, done) {
    enrollment.created_at = enrollment.start_at || enrollment.created_at;
    processStatement(enrollment, xapicaliper.course.enroll, {
      'metadata': function(request, course, callback) {
        return callback({
          'course': generateCanvasAPIURL('courses/' + course.canvas_id)
        });
      }
    }, done);
  }, callback);
};

/**
 * Process all course unenrollment activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processCourseUnenrollments = function(callback) {
  RedshiftData.getEnrollments(subpath, function(enrollment, done) {
    if (!enrollment.end_at) {
      return done();
    }

    processStatement(enrollment, xapicaliper.course.leave, {
      'timestamp': 'end_at',
      'metadata': function(request, course, callback) {
        return callback({
          'course': generateCanvasAPIURL('courses/' + course.canvas_id)
        });
      }
    }, done);
  }, callback);
};

/* SESSION */

/**
 * Process a login activity
 *
 * @param  {Object}             request                   Request that represents the login activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processLogin = function(request, callback) {
  processStatement(request, xapicaliper.session.login, {
    'timestamp': 'timestamp',
    'metadata': function(request, course, callback) {
      return callback({
        'id': generateCanvasAPIURL('sessions/' + request.id)
      });
    }
  }, callback);
};

/**
 * Process a course navigation activity
 *
 * @param  {Object}             request                   Request that represents the course navigation activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processCourseNavigation = function(request, callback) {
  processStatement(request, xapicaliper.session.navigateToCourse, {
    'timestamp': 'timestamp',
    'metadata': function(request, course, callback) {
      return callback({
        'id': generateCanvasAPIURL('courses/' + course.canvas_id),
        'name': course.name
      });
    }
  }, callback);
};

/**
 * Process a logout activity
 *
 * @param  {Object}             request                   Request that represents the logout activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processLogout = function(request, callback) {
  processStatement(request, xapicaliper.session.logout, {
    'timestamp': 'timestamp',
    'filter': function(request, callback) {
      return callback();
    }
  }, callback);
};

/* DISCUSSIONS */

/**
 * Process all discussion related learning activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processDiscussions = function(callback) {
  processDiscussionCreations(function() {
    processDiscussionPosts(function() {
      return callback();
    });
  });
};

/**
 * Process a discussion tool navigation activity
 *
 * @param  {Object}             request                   Request that represents the discussion tool navigation activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processDiscussionToolNavigation = function(request, callback) {
  processStatement(request, xapicaliper.session.navigateToPage, {
    'timestamp': 'timestamp',
    'metadata': function (request, course, callback) {
      return callback({
        'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/discussion_topics'),
        'name': 'Discussion Tool'
      });
    }
  }, callback);
};

/**
 * Process all discussion topic creation activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processDiscussionCreations = function(callback) {
  processStatements('Create discussion', discussions, xapicaliper.discussion.start, {
    'metadata': function(discussion, course, callback) {
      return callback({
        'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/discussion_topics/' + discussion.canvas_id),
        'title': discussion.title,
        'body': discussion.message
      });
    }
  }, callback);
};

/**
 * Process a discussion read activity
 *
 * @param  {Object}             request                   Request that represents the discussion read activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processDiscussionRead = function(request, callback) {
  processStatement(request, xapicaliper.discussion.read, {
    'timestamp': 'timestamp',
    'metadata': function (request, course, callback) {
      var discussion = discussions[request.discussion_id];
      return callback({
        'discussion': generateCanvasAPIURL('courses/' + course.canvas_id + '/discussion_topics/' + discussion.canvas_id)
      });
    }
  }, callback);
};

/**
 * Process all discussion post activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processDiscussionPosts = function(callback) {
  RedshiftData.getDiscussionEntries(subpath, function(entries) {
    processStatements('Discussion posts', entries, xapicaliper.discussion.post, {
      'metadata': function(entry, course, callback) {
        var discussion = discussions[entry.topic_id];
        var metadataObj = {
          'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/discussion_topics/' + discussion.canvas_id + '/entries/' + entry.canvas_id),
          'discussion': generateCanvasAPIURL('courses/' + course.canvas_id + '/discussion_topics/' + discussion.canvas_id),
          'body': entry.message
        };
        if (entry.parent_discussion_entry_id) {
          var parent = entries[entry.parent_discussion_entry_id];
          metadataObj.parent = generateCanvasAPIURL('courses/' + course.canvas_id + '/discussion_topics/' + discussion.canvas_id + '/entries/' + parent.canvas_id);
        }
        return callback(metadataObj);
      }
    }, callback);
  });
};

/* ASSIGNMENTS */

/**
 * Process all assignment related learning activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignments = function(callback) {
  processAssignmentCreations(function() {
    processAssignmentSubmissions(function () {
      processAssignmentGrading(function () {
        processAssignmentFeedback(function () {
          return callback();
        });
      });
    });
  });
};

/**
 * Process an assignment tool navigation activity
 *
 * @param  {Object}             request                   Request that represents the assignment tool navigation activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignmentToolNavigation = function(request, callback) {
  processStatement(request, xapicaliper.session.navigateToPage, {
    'timestamp': 'timestamp',
    'metadata': function (request, course, callback) {
      return callback({
        'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/assignments'),
        'name': 'Assignment Tool'
      });
    }
  }, callback);
};

/**
 * Process all assignment creation activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignmentCreations = function(callback) {
  processStatements('Create assignment', assignments, xapicaliper.assignment.create, {
    'filter': function(assignment, callback) {
      // The creator of an assignment is currently not provided.
      // Add a Nodata user until the information is availabe
      assignment.user_id = nodata_user.id;
      return callback(true);
    },
    'metadata': function(assignment, course, callback) {
      return callback({
        'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id),
        'title': assignment.title,
        'description': assignment.description,
        'due_at': assignment.due_at,
        'max_points': parseFloat(assignment.points_possible),
        'submission_types': assignment.submission_types.split(',')
      });
    }
  }, callback);
};

/**
 * Process an assignment view activity
 *
 * @param  {Object}             request                   Request that represents the assignment view activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignmentView = function(request, callback) {
  processStatement(request, xapicaliper.assignment.view, {
    'timestamp': 'timestamp',
    'metadata': function (request, course, callback) {
      var assignment = assignments[request.assignment_id];
      return callback({
        'assignment': generateCanvasAPIURL('courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id)
      });
    }
  }, callback);
};

/**
 * Process all assignment submission activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignmentSubmissions = function(callback) {
  processStatements('Submit assignment', assignmentSubmissions, xapicaliper.assignment.submit, {
    'metadata': function (assignmentSubmission, course, callback) {
      var assignment = assignments[assignmentSubmission.assignment_id];
      return callback({
        'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id + '/submissions/' + assignmentSubmission.canvas_id),
        'assignment': generateCanvasAPIURL('courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id),
        'submission': (assignmentSubmission.body || assignmentSubmission.url)
      });
    }
  }, callback);
};

/**
 * Process all assignment submission grading activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignmentGrading = function(callback) {
  processStatements('Assignment grading', assignmentSubmissions, xapicaliper.assignment.grade, {
    'timestamp': 'graded_at',
    'user': 'grader_id',
    'metadata': function (assignmentSubmission, course, callback) {
      var assignment = assignments[assignmentSubmission.assignment_id];
      var grade = (assignmentSubmission.score || assignmentSubmission.published_score);
      grade = _.isString(grade) ? parseFloat(grade) : null;
      var grade_max = assignment.points_possible;
      grade_max = _.isString(grade_max) ? parseFloat(grade_max) : null;
      return callback({
        'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id + '/submissions/' + assignmentSubmission.canvas_id),
        'assignment': generateCanvasAPIURL('courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id),
        'grade': grade,
        'grade_max': grade_max,
        'grade_min': 0
      });
    }
  }, callback);
};

/**
 * Process all assignment submission feedback activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignmentFeedback = function(callback) {
  RedshiftData.getAssignmentSubmissionComments(subpath, function (assignmentSubmissionComment, done) {
    processStatement(assignmentSubmissionComment, xapicaliper.assignment.feedback, {
      'user': 'commenter_user_id',
      'metadata': function (assignmentSubmissionComment, course, callback) {
        var assignment = assignments[assignmentSubmissionComment.assignment_id];
        var assignmentSubmission = assignmentSubmissions[assignmentSubmissionComment.submission_id];
        return callback({
          'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id + '/submissions/' + assignmentSubmission.canvas_id + '/feedback/' + assignmentSubmissionComment.canvas_id),
          'submission': generateCanvasAPIURL('courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id + '/submissions/' + assignmentSubmission.canvas_id),
          'feedback': assignmentSubmissionComment.comment
        });
      }
    }, done);
  }, callback);
};

/* FILES */

/**
 * Process a files tool navigation activity
 *
 * @param  {Object}             request                   Request that represents the files tool navigation activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processFilesToolNavigation = function(request, callback) {
  processStatement(request, xapicaliper.session.navigateToPage, {
    'timestamp': 'timestamp',
    'metadata': function (request, course, callback) {
      return callback({
        'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/files'),
        'name': 'Files Tool'
      });
    }
  }, callback);
};

/**
 * Process a file upload activity
 *
 * @param  {Object}             request                   Request that represents the file upload activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processFileUpload = function(request, callback) {
  // Get the file information from the Canvas REST API and enhance the row
  // with the file information
  var file_id = request.url.split('?')[0].split('/')[3];
  RedshiftData.getFile(file_id, function(err, file, course_id) {
    request.file = file;
    request.course_id = course_id;

    processStatement(request, xapicaliper.file.upload, {
      'timestamp': 'timestamp',
      'metadata': function (request, course, callback) {
        var id = 'files/' + request.file.id;
        if (request.course_id) {
          id = 'courses/' + request.course_id + + '/' + id;
        }
        return callback({
          'id': generateCanvasAPIURL(id),
          'title': request.file.display_name,
          'url': request.file.url,
          'size': request.file.size,
          'mime_type': request.file['content-type']
        });
      }
    }, callback);
  });
};

/**
 * Process a file preview activity
 *
 * @param  {Object}             request                   Request that represents the file preview activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processFilePreview = function(request, callback) {
  processStatement(request, xapicaliper.file.preview, {
    'timestamp': 'timestamp',
    'metadata': function (request, course, callback) {
      var file_id = request.url.split('?')[0].split('/')[4];
      return callback({
        'file': generateCanvasAPIURL('courses/' + course.canvas_id + '/files/' + file_id)
      });
    }
  }, callback);
};

/**
 * Process a file download activity
 *
 * @param  {Object}             request                   Request that represents the file download activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processFileDownload = function(request, callback) {
  processStatement(request, xapicaliper.file.download, {
    'timestamp': 'timestamp',
    'metadata': function (request, course, callback) {
      var file_id = request.url.split('?')[0].split('/')[4];
      return callback({
        'file': generateCanvasAPIURL('courses/' + course.canvas_id + '/files/' + file_id)
      });
    }
  }, callback);
};

/* SYLLABUS */

/**
 * Process all syllabus tool navigation activities
 *
 * @param  {Object}             request                   Request that represents the syllabus tool navigation activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processSyllabusToolNavigation = function(request, callback) {
  processStatement(request, xapicaliper.session.navigateToPage, {
    'timestamp': 'timestamp',
    'metadata': function (request, course, callback) {
      return callback({
        'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/syllabus'),
        'name': 'Syllabus Tool'
      });
    }
  }, callback);
};

/* MODULES */

/**
 * Process a modules tool navigation activity
 *
 * @param  {Object}             request                   Request that represents the modules tool navigation activity
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processModulesToolNavigation = function(request, callback) {
  processStatement(request, xapicaliper.session.navigateToPage, {
    'timestamp': 'timestamp',
    'metadata': function (request, course, callback) {
      return callback({
        'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/modules'),
        'name': 'Modules Tool'
      });
    }
  }, callback);
};

/* PAGES */

/**
 * Process a page view activity
 *
 * @param  {Object}             request                   Request that represents the page view
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processPageView = function(request, callback) {
  processStatement(request, xapicaliper.session.navigateToPage, {
    'timestamp': 'timestamp',
    'metadata': function (request, course, callback) {
      var page = request.url.split('/')[6];
      return callback({
        'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/pages/' + page),
        'name': page
      });
    }
  }, callback);
};

/* REQUESTS */

/**
 * TODO
 */
var processRequests = function(callback) {
  // Course navigation
  var courseRegEx = new RegExp('^\/courses\/[0-9]+$');
  // Discussion tool navigation
  var discussionRegEx1 = new RegExp('^\/courses\/[0-9]+\/discussion_topics([?]|\z)');
  var discussionRegEx2 = new RegExp('^\/api\/v1\/courses\/[0-9]+\/discussion_topics([?]|\z)');
  // Discussion read
  var discussionReadRegEx1 = new RegExp('^\/courses\/[0-9]+\/discussion_topics\/[0-9]+');
  var discussionReadRegEx2 = new RegExp('^\/api\/v1\/courses\/[0-9]+\/discussion_topics\/[0-9]+\/view');
  // Assignments tool navigation
  var assignmentRegEx = new RegExp('^\/courses\/[0-9]+\/assignments$');
  // Assignment view
  var assignmentRegEx = new RegExp('^\/courses\/[0-9]+\/assignments\/[0-9]+$');
  // Files tool navigation
  var fileNavigationRegEx1 = new RegExp('^\/courses\/[0-9]+\/files$');
  var fileNavigationRegEx2 = new RegExp('^\/courses\/[0-9]+\/files[.]json');
  // File uploads
  var fileUploadRegEx = new RegExp('^\/files\/s3_success\/[0-9]+');
  // File preview
  var filePreviewRegEx1 = new RegExp('^\/courses\/[0-9]+\/files\/[0-9]+\/preview');
  var filePreviewRegEx2 = new RegExp('^\/courses\/[0-9]+\/files\/[0-9]+\/inline_view');
  // File download
  var fileDownloadRegEx1 = new RegExp('^\/courses\/[0-9]+\/files\/[0-9]+\/download');
  var fileDownloadRegEx2 = new RegExp('^\/courses\/[0-9]+\/files\/[0-9]+\/course%20files');
  // Syllabus tool navigation
  var syllabusRegEx = new RegExp('^\/courses\/[0-9]+\/assignments\/syllabus');
  // Modules tool navigation
  var moduleRegEx = new RegExp('^\/courses\/[0-9]+\/modules$');
  // Page view activity
  var pageViewRegEx = new RegExp('^\/api/v1/courses\/[0-9]+\/pages\/');

  log.info('Processing requests');
  var requestsProcessed = 0;

  RedshiftData.getRequests(subpath, function(request, done) {
    requestsProcessed++;
    if (requestsProcessed % 1000 === 0) {
      log.info({'total': requestsProcessed}, 'Processing requests');
    }

    // Process the appropriate activity
    if (request.url.indexOf('/login/') === 0) {
      processLogin(request, done);
    } else if (courseRegEx.test(request.url)) {
      processCourseNavigation(request, done);
    } else if (request.url.indexOf('/logout') === 0) {
      processLogout(request, done);
    } else if ((discussionRegEx1.test(request.url) || discussionRegEx2.test(request.url)) && request.http_method === 'GET') {
      processDiscussionToolNavigation(request, done);
    } else if ((discussionReadRegEx1.test(request.url) || discussionReadRegEx2.test(request.url)) && request.http_method === 'GET' && discussions[request.discussion_id]) {
      processDiscussionRead(request, done);
    } else if (assignmentRegEx.test(request.url) && request.http_method === 'GET') {
      processAssignmentToolNavigation(request, done);
    } else if (assignmentRegEx.test(request.url) && request.http_method === 'GET') {
      processAssignmentView();
    } else if ((fileNavigationRegEx1.test(request.url) || fileNavigationRegEx2.test(request.url)) && request.http_method === 'GET') {
      processFilesToolNavigation(request, done);
    } else if (fileUploadRegEx.test(request.url) && request.http_method === 'GET') {
      processFileUpload(request, done);
    } else if ((filePreviewRegEx1.test(request.url) || filePreviewRegEx2.test(request.url)) && request.http_method === 'GET') {
      processFilePreview(request, done);
    } else if ((fileDownloadRegEx1.test(request.url) || fileDownloadRegEx2.test(request.url)) && request.http_method === 'GET') {
      processFileDownload(request, done);
    } else if (syllabusRegEx.test(request.url) && request.http_method === 'GET') {
      processSyllabusToolNavigation(request, done);
    } else if (moduleRegEx.test(request.url) && request.http_method === 'GET') {
      processModulesToolNavigation(request, done);
    } else if (pageViewRegEx.test(request.url) && request.http_method === 'GET') {
      processPageView(request, done);
    } else {
      return done();
    }
  }, callback);
};

/* UTIL */

/**
 * Generate learning activity statements for a set of data
 *
 * @param  {String}             type                      Readable name for the learning activity statement types that will be generated. This is used for logging purposes only
 * @param  {Object[]}           data                      Rows of data for which learning activity statements should be generated
 * @param  {Function}           statementGenerator        The xAPI/Caliper utility statement generation function that should be used
 * @param  {Object}             [opts]                    Optional properties
 * @param  {String}             [opts.timestamp]          The name of the field in the row that represents the time at which the activity took place. Defaults to `created_at`
 * @param  {String}             [opts.user]               The name of the field in the row that represents the user that performed the activity. Defaults to `user_id`
 * @param  {Function}           [opts.filter]             Function that determines whether a learning activity should be created for a row
 * @param  {Object}             [opts.filter.row]         The row for which the filter function determines whether a learning activity should be created
 * @param  {Function}           [opts.filter.callback]    Callback function to call when the filter has been applied
 * @param  {Function}           [opts.metadata]           Function that will return the metadata for the learning activity as required by the xAPI/Caliper utility
 * @param  {Object}             [opts.metadata.row]       The row for which to generate the metadata
 * @param  {Function}           [opts.metadata.callback]  Callback function to call when metadata has been assembled
 * @param  {Function}           callback                  Standard callback function
 */
var processStatements = function(type, data, statementGenerator, opts, callback) {
  log.info({'type': type}, 'Starting statement processing');

  // how many have failed processing and how many hav
  var processed = 0;
  // Keep track of how many statements have failed
  var skipped = 0;
  // Keep track of how many statements have failed to process
  var failed = 0;

  // Default filter
  opts.filter = opts.filter || function(row, filterCallback) { return filterCallback(true); };

  async.eachSeries(data, function(row, done) {
    // Check if the row should be processed
    opts.filter(row, function(match) {
      if (!match) {
        return done();
      }

      processStatement(row, statementGenerator, opts, function(err, statement) {
        if (err && err.skipped) {
          skipped++;
        } else if (err) {
          failed++;
        } else {
          processed++;
        }
        return done();
      });
    });
  }, function() {
    log.info({'type': type, 'processed': processed, 'skipped': skipped, 'failed': failed}, 'Finished processing statements');
    return callback();
  });
};

/**
 * Generate a learning activity statement
 *
 * @param  {Object}             data                      Data for which the learning activity statement should be generated
 * @param  {Function}           statementGenerator        The xAPI/Caliper utility statement generation function that should be used
 * @param  {Object}             [opts]                    Optional properties
 * @param  {String}             [opts.timestamp]          The name of the field in the row that represents the time at which the activity took place. Defaults to `created_at`
 * @param  {String}             [opts.user]               The name of the field in the row that represents the user that performed the activity. Defaults to `user_id`
 * @param  {Function}           [opts.metadata]           Function that will return the metadata for the learning activity as required by the xAPI/Caliper utility
 * @param  {Object}             [opts.metadata.row]       The row for which to generate the metadata
 * @param  {Function}           [opts.metadata.callback]  Callback function to call when metadata has been assembled
 * @param  {Function}           callback                  Standard callback function
 */
var processStatement = function(data, statementGenerator, opts, callback) {
  // Default timestamp field
  opts.timestamp = opts.timestamp || 'created_at';
  // Default user field
  opts.user = opts.user || 'user_id';
  // Default metadata generator
  opts.metadata = opts.metadata || function(row, course, metadataCallback) { return metadataCallback(null); };

  var user = users[data[opts.user]];
  if (user && user.sis_user_id) {
    // Get the course in which the activity took place
    var course = null;
    // The context object that will be passed into the statement generator
    var context = null;
    if (data.course_id && courses[data.course_id]) {
      course = courses[data.course_id];
      context = {
        'id': generateCanvasAPIURL('courses/' + course.canvas_id),
        'name': course.name
      };
    }
    // Get the metadata object
    opts.metadata(data, course, function(metadata) {
      // Generate the statement and store it in a Learning Record Store
      var timestamp = moment.utc(data[opts.timestamp]).toDate();;
      statementGenerator(config, {
        'timestamp': timestamp,
        'actor': getActor(user),
        'metadata': metadata,
        'context': context
      }, function(err, statement) {
        if (err) {
          log.error({'err': err, 'statement': statement}, 'Failed to process statement');
        } else {
          log.debug({'statement': statement}, 'Successfully processed statement');
          statementCallback(statement);
        }
        return callback(err, statement);
      });
    });
  } else {
    return callback({'skipped': true});
  };
};


/**
 * Generate a full Canvas REST API URL
 *
 * @param  {String}             path                      The relative path of the Canvas REST API following the `/api/v1` part
 * @return {String}                                       The full Canvas REST API URL for the provided relative path
 * @api private
 */
var generateCanvasAPIURL = function(path) {
  return config.platform.url + '/api/v1/' + path;
};

/**
 * Convert a user to an actor for the xAPI/Caliper utility
 *
 * @param  {Object}             user                      User object as parsed from the Canvas Data files to convert to an actor
 * @return {Object}                                       The generated actor object
 * @api private
 */
var getActor = function(user) {
  return {
    'id': 'http://berkeley.edu/directory/' + user.sis_user_id,
    'id_source': 'http://berkeley.edu/directory',
    'name': user.name,
    'created': user.created_at,
    'updated': user.updated_at
  };
};

/**
 * TODO
 */
var init = module.exports.init = function(_config, _subpath, _statementCallback, callback) {
  config = _config;
  subpath = _subpath;
  statementCallback = _statementCallback || function() {};

  // Dummy user that will be used when no data is available
  nodata_user = {
    'id': NODATA_USER_ID,
    'sis_user_id': 'UID:' + NODATA_USER_ID,
    'name': config.platform.name + '.NODATA'
  };

  // Load the list of users
  RedshiftData.getUsers(subpath, function(_users) {
    users = _users;
    users[NODATA_USER_ID] = nodata_user;
    // Load the list of courses
    RedshiftData.getCourses(subpath, function(_courses) {
      courses = _courses;
      // Load the list of discussions
      RedshiftData.getDiscussions(subpath, function(_discussions) {
        discussions = _discussions;

        // Load the list of assignments
        RedshiftData.getAssignments(subpath, function(_assignments) {
          assignments = _assignments;

          // Load the list of assignment submissions
          RedshiftData.getAssignmentSubmissions(subpath, function(_assignmentSubmissions) {
            assignmentSubmissions = _assignmentSubmissions;

            // Process the course events
            processCourses(function() {
              // Process the discussion events
              processDiscussions(function () {
                // Process the assessment events
                processAssignments(function () {
                  // Process all events from the request logs
                  processRequests(function() {
                    log.info('Finished processing all Canvas Data activities');
                    return callback();
                  });
                });
              });
            });
          });
        });
      });
    });
  });
};
