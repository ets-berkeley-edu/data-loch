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
      if (!assignment || (assignment.description && assignment.description.indexOf('clicker') !== -1)) {
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
        log.info({'course': courses[course[0]], 'submissions': course[1]});
      }
    });
  });
});
