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

var log = require('../lib/logger');
var RedshiftData = require('../lib/data');

var argv = require('yargs')
  .usage('Usage: $0 --max-old-space-size=8192')
  .help('h')
  .alias('h', 'help')
  .argv;

var submissions = {};
var progress = 0;

RedshiftData.getCourses('merged', function(courses) {
  RedshiftData.getAssignments('merged', function(assignments) {
    RedshiftData.getAssignmentSubmissions('merged', function(submission, done) {
      progress++;
      if (progress % 1000 === 0) {
        log.info('Processed ' + progress + ' submissions');
      }

      // TODO
      var assignment = assignments[submission.assignment_id];
      if (!assignment) {
        return done();
      } else if (assignment.description && assignment.description.indexOf('clicker') !== -1) {
        return done();
      }

      // TODO
      submissions[assignment.course_id] = submissions[assignment.course_id] || 0;
      submissions[assignment.course_id]++;
      return done();

    }, function() {
      // TODO
      var sortable = [];
      _.each(submissions, function(submission_count, course_id) {
        sortable.push([course_id, submission_count]);
      });
      sortable.sort(function(a, b) {return b[1] - a[1];});
      for (var i = 0; i < 20; i++) {
        var course = sortable[i];
        log.info({course: courses[course[0]], submissions: course[1]});
      }
    });
  });
});
