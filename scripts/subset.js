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
var csv = require('fast-csv');
var fs = require('fs');

var log = require('../lib/logger')('subset');
var redshiftUtil = require('../lib/util');
var usersUtil = require('../lib/users');

var argv = require('yargs')
  .usage('Usage: $0 --max-old-space-size=8192 -t [total_subset_size]')
  .describe('t', 'Total size of the subpopulation to generate Canvas data files for')
  .help('h')
  .alias('h', 'help')
  .argv;

/**
 * Select a random subpopulation from the overall Canvas data file set
 *
 * @param  {Number}         subpopulation_size                  The total size of the desired random user subpopulation
 * @param  {Function}       callback                            Standard callback function
 * @param  {Object}         callback.subpopulation_users        Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         callback.subpopulation_courses      Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         callback.subpopulation_terms        Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @api private
 */
var extractSubpopulation = function(subpopulation_size, callback) {
  // Generate the full user tree
  usersUtil.buildUserTree('base', function(userTree) {
    // Select the random users
    var selected_users = _.sample(_.keys(userTree), subpopulation_size);
    var subpopulation_users = _.reduce(selected_users, function(memo, user) {
      memo[user] = true;
      return memo;
    }, {});

    // Extract the courses and terms involved
    var subpopulation_terms = {};
    var subpopulation_courses = {};
    _.each(selected_users, function(user) {
      _.map(_.keys(userTree[user].terms), function(term) {
        subpopulation_terms[term] = true;
      });
      _.each(userTree[user].terms, function(term) {
        _.map(_.keys(term.courses), function(course) {
          subpopulation_courses[course] = true;
        });
      });
    });

    return callback(subpopulation_users, subpopulation_courses, subpopulation_terms);
  });
};

/**
 * Filter all Canvas data files down to the records related to a selected
 * random user subpopulation. Note that this excludes the request log files,
 * as well as `account_dim`, `date_dim`, `course_ui_navigation_item_dim` and `role_dim`
 *
 * @param  {Object}         subpopulation_users                 Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         subpopulation_courses               Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         subpopulation_terms                 Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsets = function(subpopulation_users, subpopulation_courses, subpopulation_terms, callback) {
  // Generate subsets for the user data files
  generateSubsetUsers(subpopulation_users, subpopulation_courses, subpopulation_terms, function() {
    // Generate a subset of the `enrollment_term_dim` files
    generateSubset('enrollment_term_dim', 0, subpopulation_terms, 0, function(terms) {
      // Generate subsets for the assignment data files
      generateSubsetAssignments(subpopulation_users, subpopulation_courses, subpopulation_terms, function() {
        // Generate subsets for the conversation data files
        generateSubsetConversations(subpopulation_users, subpopulation_courses, subpopulation_terms, function() {
          // Generate subsets for the courses data files
          generateSubsetCourses(subpopulation_users, subpopulation_courses, subpopulation_terms, function() {
            // Generate subsets for the navigation data files
            generateSubsetNavigation(subpopulation_users, subpopulation_courses, subpopulation_terms, function() {
              // Generate subsets for the discussion data files
              generateSubsetDiscussions(subpopulation_users, subpopulation_courses, subpopulation_terms, function() {
                // Generate subsets for the quiz data files
                generateSubsetQuizes(subpopulation_users, subpopulation_courses, subpopulation_terms, function() {
                  // Generate a subset of the `external_tool_activation_fact` files
                  generateSubset('external_tool_activation_fact', 1, subpopulation_courses, 0, function(external_tool_activations) {
                    // Generate subsets for the group data files
                    generateSubsetGroups(subpopulation_users, subpopulation_courses, subpopulation_terms, function() {
                      // Generate subsets for the submission data files
                      generateSubsetSubmissions(subpopulation_users, subpopulation_courses, subpopulation_terms, function() {
                        // Generate subsets for the wiki data files
                        generateSubsetWikis(subpopulation_users, subpopulation_courses, subpopulation_terms, function() {
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
      });
    });
  });
};

/**
 * Filter all user-related Canvas data files down to the records related to a selected
 * random user subpopulation
 *
 * @param  {Object}         subpopulation_users                 Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         subpopulation_courses               Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         subpopulation_terms                 Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetUsers = function(subpopulation_users, subpopulation_courses, subpopulation_terms, callback) {
  // Generate a subset of the `user_dim` files
  generateSubset('user_dim', 0, subpopulation_users, 0, function(users) {
    // Generate a subset of the `pseudonym_fact` files
    generateSubset('pseudonym_fact', 1, subpopulation_users, 0, function(pseudonyms) {
      // Generate a subset of the `pseudonym_dim` files
      generateSubset('pseudonym_dim', 2, subpopulation_users, 0, function(pseudonyms) {
        // Generate a subset of the `enrollment_fact` files
        generateSubset('enrollment_fact', 1, subpopulation_users, 0, function(enrollments) {
          // Generate a subset of the `enrollment_dim` files
          generateSubset('enrollment_dim', 0, enrollments, 0, function(enrollments) {
            // Generate a subset of the `enrollment_rollup_dim` files
            generateSubset('enrollment_rollup_dim', 1, subpopulation_users, 0, function(enrollments_rollup) {
              return callback();
            });
          });
        });
      });
    });
  });
};

/**
 * Filter all assignment-related Canvas data files down to the records related to a selected
 * random user subpopulation
 *
 * @param  {Object}         subpopulation_users                 Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         subpopulation_courses               Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         subpopulation_terms                 Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetAssignments = function(subpopulation_users, subpopulation_courses, subpopulation_terms, callback) {
  // Generate a subset of the `assignment_fact` files
  generateSubset('assignment_fact', 1, subpopulation_courses, 0, function(assignments) {
    // Generate a subset of the `assignment_dim` files
    generateSubset('assignment_dim', 2, subpopulation_courses, 0, function(assignments) {
      return callback();
    });
  });
};

/**
 * Filter all conversation-related Canvas data files down to the records related to a selected
 * random user subpopulation
 *
 * @param  {Object}         subpopulation_users                 Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         subpopulation_courses               Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         subpopulation_terms                 Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetConversations = function(subpopulation_users, subpopulation_courses, subpopulation_terms, callback) {
  // Generate a subset of the `conversation_dim` files
  generateSubset('conversation_dim', 5, subpopulation_courses, 0, function(conversations) {
    // Generate a subset of the `conversation_message_dim` files
    generateSubset('conversation_message_dim', 2, conversations, 0, function(conversations_messages) {
      // Generate a subset of the `conversation_message_participant_fact` files
      generateSubset('conversation_message_participant_fact', 0, conversations_messages, 0, function(conversations_message_participants) {
        return callback();
      });
    });
  });
};

/**
 * Filter all course-related Canvas data files down to the records related to a selected
 * random user subpopulation
 *
 * @param  {Object}         subpopulation_users                 Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         subpopulation_courses               Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         subpopulation_terms                 Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetCourses = function(subpopulation_users, subpopulation_courses, subpopulation_terms, callback) {
  // Generate a subset of the `course_dim` files
  generateSubset('course_dim', 0, subpopulation_courses, 0, function(courses) {
    // Generate a subset of the `course_section_dim` files
    generateSubset('course_section_dim', 3, subpopulation_courses, 0, function(sections) {
      return callback();
    });
  });
};

/**
 * Filter all navigation-related Canvas data files down to the records related to a selected
 * random user subpopulation
 *
 * @param  {Object}         subpopulation_users                 Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         subpopulation_courses               Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         subpopulation_terms                 Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetNavigation = function(subpopulation_users, subpopulation_courses, subpopulation_terms, callback) {
  // Generate a subset of the `course_ui_navigation_item_fact` files
  generateSubset('course_ui_navigation_item_fact', 4, subpopulation_courses, 1, function(navigation_items) {
    // Generate a subset of the `course_ui_canvas_navigation_dim` files
    generateSubset('course_ui_canvas_navigation_dim', 0, navigation_items, 0, function(navigation_items) {
      // Generate a subset of the `course_ui_navigation_dim_dim` files
      generateSubset('course_ui_navigation_dim_dim', 0, subpopulation_courses, 0, function(navigation_item_dims) {
        return callback();
      });
    });
  });
};

/**
 * Filter all discussion-related Canvas data files down to the records related to a selected
 * random user subpopulation
 *
 * @param  {Object}         subpopulation_users                 Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         subpopulation_courses               Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         subpopulation_terms                 Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetDiscussions = function(subpopulation_users, subpopulation_courses, subpopulation_terms, callback) {
  // Generate a subset of the `discussion_entry_fact` files
  generateSubset('discussion_entry_fact', 4, subpopulation_courses, 0, function(discussion_entries) {
    // Generate a subset of the `discussion_entry_dim` files
    generateSubset('discussion_entry_dim', 0, discussion_entries, 0, function(discussion_entries) {
      // Generate a subset of the `discussion_topic_fact` files
      generateSubset('discussion_topic_fact', 1, subpopulation_courses, 0, function(discussion_topics) {
        // Generate a subset of the `discussion_topic_dim` files
        generateSubset('discussion_topic_dim', 0, discussion_topics, 0, function(discussion_topics) {
          return callback();
        });
      });
    });
  });
};

/**
 * /**
 * Filter all quiz-related Canvas data files down to the records related to a selected
 * random user subpopulation
 *
 * @param  {Object}         subpopulation_users                 Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         subpopulation_courses               Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         subpopulation_terms                 Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetQuizes = function(subpopulation_users, subpopulation_courses, subpopulation_terms, callback) {
  // Generate a subset of the `quiz_dim` files
  generateSubset('quiz_dim', 7, subpopulation_courses, 0, function(quizes) {
    // Generate a subset of the `quiz_fact` files
    generateSubset('quiz_fact', 0, quizes, 0, function(quizes) {
      // Generate a subset of the `quiz_question_dim` files
      generateSubset('quiz_question_dim', 2, quizes, 0, function(quiz_questions) {
        // Generate a subset of the `quiz_data_quiz_question_dim` files
        generateSubset('quiz_data_quiz_question_dim', 0, quiz_questions, 0, function(quiz_questions) {
          // Generate a subset of the `quiz_question_data_quiz_question_dim` files
          generateSubset('quiz_question_data_quiz_question_dim', 2, quizes, 0, function(quiz_question_data) {
            // Generate a subset of the `quiz_question_group_dim` files
            generateSubset('quiz_question_group_dim', 2, quizes, 0, function(quiz_question_groups) {
              // Generate a subset of the `quiz_question_group_fact` files
              generateSubset('quiz_question_group_fact', 0, quiz_question_groups, 0, function(quiz_question_groups) {
                // Generate a subset of the `quiz_data_quiz_question_answer_dim` files
                generateSubset('quiz_data_quiz_question_answer_dim', 2, quiz_questions, 0, function(quiz_question_answers) {
                  // Generate a subset of the `quiz_question_data_quiz_question_answer_dim` files
                  generateSubset('quiz_question_data_quiz_question_answer_dim', 3, quiz_question_groups, 0, function() {
                    // Generate a subset of the `quiz_submission_dim` files
                    generateSubset('quiz_submission_dim', 2, quizes, 0, function(quiz_submissions) {
                      // Generate a subset of the `quiz_submission_fact` files
                      generateSubset('quiz_submission_fact', 2, quiz_submissions, 2, function(quiz_submissions) {
                        // Generate a subset of the `quiz_submission_historical_dim` files
                        generateSubset('quiz_submission_historical_dim', 2, quizes, 0, function(quiz_submissions_historical) {
                          // Generate a subset of the `quiz_submission_historical_fact` files
                          generateSubset('quiz_submission_historical_fact', 6, quiz_submissions_historical, 6, function(quiz_submissions_historical) {
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
        });
      });
    });
  });
};

/**
 * Filter all group-related Canvas data files down to the records related to a selected
 * random user subpopulation
 *
 * @param  {Object}         subpopulation_users                 Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         subpopulation_courses               Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         subpopulation_terms                 Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetGroups = function(subpopulation_users, subpopulation_courses, subpopulation_terms, callback) {
  // Generate a subset of the `group_membership_fact` files
  generateSubset('group_membership_fact', 5, subpopulation_users, 0, function(groups) {
    // Generate a subset of the `group_dim` files
    generateSubset('group_dim', 0, groups, 0, function(groups) {
      // Generate a subset of the `group_fact` files
      generateSubset('group_fact', 0, subpopulation_users, 0, function(groups) {
        return callback();
      });
    });
  });
};

/**
 * Filter all submissions-related Canvas data files down to the records related to a selected
 * random user subpopulation
 *
 * @param  {Object}         subpopulation_users                 Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         subpopulation_courses               Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         subpopulation_terms                 Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetSubmissions = function(subpopulation_users, subpopulation_courses, subpopulation_terms, callback) {
  // Generate a subset of the `submission_fact` files
  generateSubset('submission_fact', 2, subpopulation_courses, 0, function(submissions) {
    // Generate a subset of the `submission_dim` files
    generateSubset('submission_dim', 0, submissions, 0, function(submissions) {
      // Generate a subset of the `submission_comment_fact` files
      generateSubset('submission_comment_fact', 1, submissions, 0, function(submission_comments) {
        // Generate a subset of the `submission_comment_dim` files
        generateSubset('submission_comment_dim', 0, submission_comments, 0, function(submission_comments) {
          // Generate a subset of the `submission_comment_participant_fact` files
          generateSubset('submission_comment_participant_fact', 1, submission_comments, 0, function(submission_comment_participants) {
            // Generate a subset of the `submission_comment_participant_dim` files
            generateSubset('submission_comment_participant_dim', 0, submission_comment_participants, 0, function(submission_comment_participants) {
              return callback();
            });
          });
        });
      });
    });
  });
};

/**
 * Filter all wiki-related Canvas data files down to the records related to a selected
 * random user subpopulation
 *
 * @param  {Object}         subpopulation_users                 Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         subpopulation_courses               Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         subpopulation_terms                 Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetWikis = function(subpopulation_users, subpopulation_courses, subpopulation_terms, callback) {
  // Generate a subset of the `wiki_fact` files
  generateSubset('wiki_fact', 1, subpopulation_courses, 0, function(wikis) {
    // Generate a subset of the `wiki_dim` files
    generateSubset('wiki_dim', 0, wikis, 0, function(wikis) {
      // Generate a subset of the `wiki_page_fact` files
      generateSubset('wiki_page_fact', 1, wikis, 0, function(wiki_pages) {
        // Generate a subset of the `wiki_page_dim` files
        generateSubset('wiki_page_dim', 0, wiki_pages, 0, function(wiki_pages) {
          return callback();
        });
      });
    });
  });
};

/**
 * Filter all request-related Canvas data files down to the records related to a selected
 * random user subpopulation. Given the total number of request files and their size, the data
 * files will be downloaded and filtered one by one
 *
 * @param  {Object}         subpopulation_users                 Selected user subpopulation. The keys represent the user ids
 * @param  {Object}         subpopulation_courses               Courses that correspond to the selected user subpopulation. The keys represent the course ids
 * @param  {Object}         subpopulation_terms                 Terms that correspond to the selected user subpopulation. The keys represent the term ids
 * @param  {Function}       callback                            Standard callback function
 * @api private
 */
var generateSubsetRequests = function(subpopulation_users, subpopulation_courses, subpopulation_terms, callback) {
  // Get the full list of request data files
  redshiftUtil.canvasDataApiRequest('/file/byTable/requests', function(tableDump) {
    var full = _.find(tableDump.history, {partial: false});
    if (!full) {
      log.warn('Unable to find full dump for requests table');
      return false;
    }

    var foundFull = false;
    var files = [];
    for (var i = tableDump.history.length - 1; i >= 0; i--) {
      var dump = tableDump.history[i];
      if (foundFull || dump.sequence === full.sequence) {
        foundFull = true;
        files = files.concat(dump.files);
      }
    }

    async.eachSeries(files, function(file, done) {
      var start = Date.now();

      redshiftUtil.downloadFiles([ file ], false, function(filenames) {
        var filename = filenames[0];
        var retainedRows = 0;
        csv.fromPath('./data/base/' + filename, {delimiter: '\t', quote: null})
          .transform(function(row) {
            if (subpopulation_users[row[5]]) {
              retainedRows++;
              return row;
            }

            return null;
          })
          .pipe(csv.createWriteStream({delimiter: '\t', quote: null}))
          .pipe(fs.createWriteStream('./data/subset/requests', {flags: 'a'}))
          .on('finish', function() {
            var end = Date.now();
            log.info({file: filename, duration: end - start, retained: retainedRows}, 'Finished generating a subset for requests file');
            // Delete the downloaded file
            fs.unlinkSync('./data/base/' + filename);
            return done();
          });
      });
    }, function() {
      log.info('Finished generating a subset for all requests files');
      return callback();
    });
  });
};

/**
 * Generate a subset for a Canvas data file file based on a set of ids to filter by
 * and write the subset to a new Canvas data file
 *
 * @param  {String}         file                                The base name to the data file(s) that need to be filtered to the provided subpopulation
 * @param  {Number}         filterIndex                         The row index of the field by which the rows should be filtered
 * @param  {Object}         filter                              The list of filter values to allow
 * @param  {Number}         idIndex                             The row index of the identifier that should be used to represent the filtered rows
 * @param  {Function}       callback                            Standard callback function
 * @param  {Object}         callback.retainedData               Object where the keys are identifiers for the retained rows
 * @api private
 */
var generateSubset = function(file, filterIndex, filter, idIndex, callback) {
  redshiftUtil.parseDataFiles('base/' + file, function(data) {
    var retainedRows = 0;
    var retainedData = {};
    var csvStream = csv.createWriteStream({delimiter: '\t', quote: null});
    var writableStream = fs.createWriteStream('./data/subset/' + file);

    writableStream.on('finish', function() {
      log.info({
        file: file,
        total: data.length,
        retained: retainedRows
      }, 'Finished generating a subset for a file');

      return callback(retainedData);
    });

    csvStream.pipe(writableStream);
    _.each(data, function(row) {
      if (filter[row[filterIndex]]) {
        retainedRows++;
        retainedData[row[idIndex]] = true;
        csvStream.write(row);
      }
    });
    csvStream.end();
  });
};

// Extract the size of the subpopulation
var subpopulation_size = argv.t || 100;
// Extract the subpopulation and their courses and terms
extractSubpopulation(subpopulation_size, function(subpopulation_users, subpopulation_courses, subpopulation_terms) {
  generateSubsets(subpopulation_users, subpopulation_courses, subpopulation_terms, function() {
    generateSubsetRequests(subpopulation_users, subpopulation_courses, subpopulation_terms, function() {});
  });
});
