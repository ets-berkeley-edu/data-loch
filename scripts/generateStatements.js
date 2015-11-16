var _ = require('lodash');
var async = require('async');
var config = require('config');
var xapicaliper = require('xapicaliper');
var argv = require('yargs')
  .usage('Usage: $0 --max-old-space-size=8192')
  .help('h')
  .alias('h', 'help')
  .argv;

var log = require('../lib/logger');
var RedshiftData = require('../lib/data');
var RedshiftUtil = require('../lib/util');

// Variable that will keep track of the full user list
var users = null;
// Variable that will keep track of the full course list
var courses = null;

// xAPI/Caliper utility configuration
var CONFIG = config.get('statements');

/* SESSION */

/**
 * Process all session related learning activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processSessions = function(callback) {
  processLogins(function() {
    processCourseNavigation(function() {
      processLogouts(function() {
        return callback();
      });
    });
  });
};

/**
 * Process all login activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processLogins = function(callback) {
  RedshiftData.getRequests('subset', function(requests) {
    processStatements('Logins', requests, xapicaliper.session.login, {
      'timestamp': 'timestamp',
      'filter': function(request, callback) {
        return callback(request.url.indexOf('/login/') === 0);
      },
      'metadata': function(request, course, callback) {
        return callback({
          'id': request.id
        });
      }
    }, callback);
  });
};

/**
 * Process all course navigation activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processCourseNavigation = function(callback) {
  RedshiftData.getRequests('subset', function(requests) {
    var courseRegEx = new RegExp('^\/courses\/[0-9]+$');
    processStatements('Course navigation', requests, xapicaliper.session.navigateToCourse, {
      'timestamp': 'timestamp',
      'filter': function(request, callback) {
        return callback(courseRegEx.test(request.url));
      },
      'metadata': function(request, course, callback) {
        return callback({
          'id': generateCanvasAPIURL('courses/' + course.canvas_id),
          'name': course.name
        });
      }
    }, callback);
  });
};

/**
 * Process all logout activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processLogouts = function(callback) {
  RedshiftData.getRequests('subset', function(requests) {
    processStatements('Logouts', requests, xapicaliper.session.logout, {
      'timestamp': 'timestamp',
      'filter': function(request, callback) {
        return callback(request.url.indexOf('/logout') === 0);
      }
    }, callback);
  });
};

/* DISCUSSIONS */

/**
 * Process all discussion related learning activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processDiscussions = function(callback) {
  processDiscussionToolNavigation(function() {
    processDiscussionCreations(function() {
      processDiscussionReads(function() {
        processDiscussionPosts(function() {
          return callback();
        });
      });
    });
  });
};

/**
 * Process all discussion tool navigation activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processDiscussionToolNavigation = function(callback) {
  RedshiftData.getRequests('subset', function (requests) {
    var discussionRegEx1 = new RegExp('^\/courses\/[0-9]+\/discussion_topics([?]|\z)');
    var discussionRegEx2 = new RegExp('^\/api\/v1\/courses\/[0-9]+\/discussion_topics([?]|\z)');
    processStatements('Discussion tool navigation', requests, xapicaliper.session.navigateToPage, {
      'timestamp': 'timestamp',
      'filter': function(request, callback) {
        return callback((discussionRegEx1.test(request.url) || discussionRegEx2.test(request.url)) && request.http_method === 'GET');
      },
      'metadata': function (request, course, callback) {
        return callback({
          'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/discussion_topics'),
          'name': 'Discussion Tool'
        });
      }
    }, callback);
  });
};

/**
 * Process all discussion topic creation activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processDiscussionCreations = function(callback) {
  RedshiftData.getDiscussions('subset', function(discussions) {
    processStatements('Create discussion', discussions, xapicaliper.discussion.start, {
      'metadata': function(discussion, course, callback) {
        return callback({
          'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/discussion_topics/' + discussion.canvas_id),
          'title': discussion.title,
          'body': discussion.message
        });
      }
    }, callback);
  });
};

/**
 * Process all discussion read activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processDiscussionReads = function(callback) {
  RedshiftData.getDiscussions('subset', function(discussions) {
    RedshiftData.getRequests('subset', function (requests) {
      var discussionRegEx1 = new RegExp('^\/courses\/[0-9]+\/discussion_topics\/[0-9]+');
      var discussionRegEx2 = new RegExp('^\/api\/v1\/courses\/[0-9]+\/discussion_topics\/[0-9]+\/view');
      processStatements('Discussion reads', requests, xapicaliper.discussion.read, {
        'timestamp': 'timestamp',
        'filter': function(request, callback) {
          return callback((discussionRegEx1.test(request.url) || discussionRegEx2.test(request.url)) && request.http_method === 'GET');
        },
        'metadata': function (request, course, callback) {
          var discussion = discussions[request.discussion_id];
          return callback({
            'discussion': generateCanvasAPIURL('courses/' + course.canvas_id + '/discussion_topics/' + discussion.canvas_id)
          });
        }
      }, callback);
    });
  });
};

/**
 * Process all discussion post activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processDiscussionPosts = function(callback) {
  RedshiftData.getDiscussions('subset', function(discussions) {
    RedshiftData.getDiscussionEntries('subset', function(entries) {
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
  processAssignmentToolNavigation(function() {
    processAssignmentCreations(function() {
      processAssignmentViews(function () {
        processAssignmentSubmissions(function () {
          processAssignmentGrading(function () {
            processAssignmentFeedback(function () {
              return callback();
            });
          });
        });
      });
    });
  });
};

/**
 * Process all assignment tool navigation activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignmentToolNavigation = function(callback) {
  RedshiftData.getRequests('subset', function (requests) {
    var assignmentRegEx = new RegExp('^\/courses\/[0-9]+\/assignments$');
    processStatements('Assignment tool navigation', requests, xapicaliper.session.navigateToPage, {
      'timestamp': 'timestamp',
      'filter': function(request, callback) {
        return callback(assignmentRegEx.test(request.url) && request.http_method === 'GET');
      },
      'metadata': function (request, course, callback) {
        return callback({
          'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/assignments'),
          'name': 'Assignment Tool'
        });
      }
    }, callback);
  });
};

/**
 * Process all assignment creation activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignmentCreations = function(callback) {
  RedshiftData.getAssignments('subset', function(assignments) {
    processStatements('Create assignment', assignments, xapicaliper.assignment.create, {
      // Note: Add a random assignment creator until
      // that information is available in the data dump
      'filter': function(assignment, callback) {
        assignment.user_id = _.sample(_.keys(users));
        return callback(true);
      },
      'metadata': function(assignment, course, callback) {
        return callback({
          'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id),
          'title': assignment.title,
          'description': assignment.description,
          'submission_types': assignment.submission_types.split(',')
        });
      }
    }, callback);
  });
};

/**
 * Process all assignment read activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignmentViews = function(callback) {
  RedshiftData.getAssignments('subset', function(assignments) {
    RedshiftData.getRequests('subset', function (requests) {
      var assignmentRegEx = new RegExp('^\/courses\/[0-9]+\/assignments\/[0-9]+$');
      processStatements('Assignment views', requests, xapicaliper.assignment.view, {
        'timestamp': 'timestamp',
        'filter': function(request, callback) {
          return callback(assignmentRegEx.test(request.url) && request.http_method === 'GET');
        },
        'metadata': function (request, course, callback) {
          var assignment = assignments[request.assignment_id];
          return callback({
            'assignment': generateCanvasAPIURL('courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id)
          });
        }
      }, callback);
    });
  });
};

/**
 * Process all assignment submission activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignmentSubmissions = function(callback) {
  RedshiftData.getAssignments('subset', function(assignments) {
    RedshiftData.getAssignmentSubmissions('subset', function(assignmentSubmissions) {
      processStatements('Submit assignment', assignmentSubmissions, xapicaliper.assignment.submit, {
        'metadata': function (assignmentSubmission, course, callback) {
          var assignment = assignments[assignmentSubmission.assignment_id];
          return callback({
            'id': generateCanvasAPIURL('/courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id + '/submissions/' + assignmentSubmission.canvas_id),
            'assignment': generateCanvasAPIURL('/courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id),
            'submission': (assignmentSubmission.body || assignmentSubmission.url)
          });
        }
      }, callback);
    });
  });
};

/**
 * Process all assignment submission grading activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignmentGrading = function(callback) {
  RedshiftData.getAssignments('subset', function(assignments) {
    RedshiftData.getAssignmentSubmissions('subset', function(assignmentSubmissions) {
      processStatements('Assignment grading', assignmentSubmissions, xapicaliper.assignment.receive_grade, {
        'timestamp': 'graded_at',
        'metadata': function (assignmentSubmission, course, callback) {
          var assignment = assignments[assignmentSubmission.assignment_id];
          var grader = null;
          if (assignmentSubmission.grader_id) {
            grader = getActor(users[assignmentSubmission.grader_id]);
          }
          var grade = assignmentSubmission.score || assignmentSubmission.published_score;
          if (grade) {
            grade = parseFloat(grade);
          }
          return callback({
            'id': generateCanvasAPIURL('/courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id + '/submissions/' + assignmentSubmission.canvas_id),
            'assignment': generateCanvasAPIURL('/courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id),
            'grade': grade,
            'grader': grader
          });
        }
      }, callback);
    });
  });
};

/**
 * Process all assignment submission feedback activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processAssignmentFeedback = function(callback) {
  RedshiftData.getAssignments('subset', function(assignments) {
    RedshiftData.getAssignmentSubmissions('subset', function(assignmentSubmissions) {
      RedshiftData.getAssignmentSubmissionComments('subset', function (assignmentSubmissionComments) {
        processStatements('Assignment feedback', assignmentSubmissionComments, xapicaliper.assignment.feedback, {
          'user': 'commenter_user_id',
          'metadata': function (assignmentSubmissionComment, course, callback) {
            var assignment = assignments[assignmentSubmissionComment.assignment_id];
            var assignmentSubmission = assignmentSubmissions[assignmentSubmissionComment.submission_id];
            return callback({
              'id': generateCanvasAPIURL('/courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id + '/submissions/' + assignmentSubmission.canvas_id + '/feedback/' + assignmentSubmissionComment.canvas_id),
              'submission': generateCanvasAPIURL('/courses/' + course.canvas_id + '/assignments/' + assignment.canvas_id + '/submissions/' + assignmentSubmission.canvas_id),
              'feedback': assignmentSubmissionComment.comment
            });
          }
        }, callback);
      });
    });
  });
};

/* FILES */

/**
 * Process all files related learning activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processFiles = function(callback) {
  processFilesToolNavigation(function() {
    processFileUploads(function() {
      processFilePreviews(function () {
        processFileDownloads(function () {
          return callback();
        });
      });
    });
  });
};

/**
 * Process all assignment tool navigation activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processFilesToolNavigation = function(callback) {
  RedshiftData.getRequests('subset', function (requests) {
    var fileRegEx = new RegExp('^\/courses\/[0-9]+\/files');
    processStatements('Files tool navigation', requests, xapicaliper.session.navigateToPage, {
      'timestamp': 'timestamp',
      'filter': function(request, callback) {
        return callback(fileRegEx.test(request.url) && request.http_method === 'GET');
      },
      'metadata': function (request, course, callback) {
        return callback({
          'id': generateCanvasAPIURL('courses/' + course.canvas_id + '/files'),
          'name': 'Files Tool'
        });
      }
    }, callback);
  });
};

/**
 * Process all file upload activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processFileUploads = function(callback) {
  RedshiftData.getRequests('subset', function (requests) {
    var fileRegEx = new RegExp('^\/files\/s3_success\/[0-9]+');
    processStatements('File uploads', requests, xapicaliper.file.upload, {
      'timestamp': 'timestamp',
      'filter': function(request, callback) {
        var match = (fileRegEx.test(request.url) && request.http_method === 'GET');
        if (!match) {
          return callback(false);
        }

        // Get the file information from the Canvas REST API and enhance the row
        // with the file information
        var file_id = request.url.split('?')[0].split('/')[3];
        RedshiftData.getFile(file_id, function(err, file, course_id) {
          request.file = file;
          request.course_id = course_id;
          return callback(match);
        });
      },
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
 * Process all file preview activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processFilePreviews = function(callback) {
  RedshiftData.getRequests('subset', function (requests) {
    var fileRegEx1 = new RegExp('^\/courses\/[0-9]+\/files\/[0-9]+/preview');
    var fileRegEx2 = new RegExp('^\/courses\/[0-9]+\/files\/[0-9]+/inline_view');
    processStatements('File previews', requests, xapicaliper.file.preview, {
      'timestamp': 'timestamp',
      'filter': function(request, callback) {
        return callback((fileRegEx1.test(request.url) || fileRegEx2.test(request.url)) && request.http_method === 'GET');
      },
      'metadata': function (request, course, callback) {
        var file_id = request.url.split('?')[0].split('/')[4];
        return callback({
          'file': generateCanvasAPIURL('courses/' + course.canvas_id + '/files/' + file_id)
        });
      }
    }, callback);
  });
};

/**
 * Process all file download activities
 *
 * @param  {Function}           callback                  Standard callback function
 * @api private
 */
var processFileDownloads = function(callback) {
  RedshiftData.getRequests('subset', function (requests) {
    var fileRegEx = new RegExp('^\/courses\/[0-9]+\/files\/[0-9]+/download');
    processStatements('File downloads', requests, xapicaliper.file.download, {
      'timestamp': 'timestamp',
      'filter': function(request, callback) {
        return callback(fileRegEx.test(request.url) && request.http_method === 'GET');
      },
      'metadata': function (request, course, callback) {
        var file_id = request.url.split('?')[0].split('/')[4];
        return callback({
          'file': generateCanvasAPIURL('courses/' + course.canvas_id + '/files/' + file_id)
        });
      }
    }, callback);
  });
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
 * @param  {Object}             [opts.metadata.course]    The course that corresponds to the row, if any
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

  // Default timestamp field
  opts.timestamp = opts.timestamp || 'created_at';
  // Default user field
  opts.user = opts.user || 'user_id';
  // Default filter
  opts.filter = opts.filter || function(row, callback) { return callback(true); };
  // Default metadata generator
  opts.metadata = opts.metadata || function(row, course, callback) { return callback(null); };


  async.eachSeries(data, function(row, done) {
    // Check if the row should be processed
    opts.filter(row, function(match) {
      if (!match) {
        return done();
      }

      var user = users[row[opts.user]];
      if (user && user.sis_user_id) {
        // Get the course in which the activity took place
        var course = null;
        // The context object that will be passed into the statement generator
        var context = null;
        if (row.course_id && courses[row.course_id]) {
          course = courses[row.course_id];
          context = {
            'id': generateCanvasAPIURL('courses/' + course.canvas_id),
            'name': course.name
          };
        }
        // Get the metadata object
        opts.metadata(row, course, function(metadata) {
          // Generate the statement and store it in a Learning Record Store
          statementGenerator(CONFIG, {
            'timestamp': row[opts.timestamp],
            'actor': getActor(user),
            'metadata': metadata,
            'context': context
          }, function(err, statement) {
            if (err) {
              failed++;
              log.error({'err': err, 'statement': statement}, 'Failed to process statement');
            } else {
              processed++;
              log.info({'statement': statement}, 'Successfully processed statement');
            }
            return callback();
            // TODO: return done();
          });
        });
      } else {
        skipped++;
        return done();
      }
    });
  }, function() {
    log.info({'type': type, 'processed': processed, 'skipped': skipped, 'failed': failed}, 'Finished processing statements');
    return callback();
  });
};

/**
 * Generate a full Canvas REST API URL
 *
 * @param  {String}             path                      The relative path of the Canvas REST API following the `/api/v1` part
 * @return {String}                                       The full Canvas REST API URL for the provided relative path
 * @api private
 */
var generateCanvasAPIURL = function(path) {
  return CONFIG.platform.url + '/api/v1/' + path;
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
    'id': user.sis_user_id,
    'id_source': 'http://berkeley.edu/directory',
    'name': user.name,
    'created': user.created_at,
    'updated': user.updated_at
  };
};

// Load the list of users
RedshiftData.getUsers('base', function(_users) {
  users = _users;
  RedshiftData.getCourses('base', function(_courses) {
    courses = _courses;
    // Process the session events
    processSessions(function () {
      // Process the discussion events
      processDiscussions(function () {
        // Process the assessment events
        processAssignments(function () {
          // Process the files events
          processFiles(function () {
            log.info('Finished processing all Canvas Data activities');
          });
        });
      });
    });
  });
});
