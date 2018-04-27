/**
 * Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.
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

var log = require('../../lib/core/logger')('suitecAnalyticsApp');
var Analytics = require('./lib/generateResearchExtracts.js');
var Unload = require('./lib/unloadTablesToS3.js');

/**
*
*
* @param  {Function}         callback                Standard callback function
* @param  {Object}           callback.err            An error object, if any
*/
Analytics.loadSchema(function(err) {
  if (err) {
    log.error({err: err}, 'SuiteC Schema restore failed on data-loch.');
    process.exit(1);
  }

  Analytics.generateAnalyticsTables(function(err) {
    if (err) {
      log.error({err: err}, 'Completed with errors.');
      process.exit(1);
    }

    log.info('Starting export of de-identified analytics tables to S3');

    Unload.unloadToS3(function(err) {
      if (err) {
        log.error({err: err}, 'Export of analytics tables to S3 failed');
        process.exit(1);
      }

      log.info('Unload of annonymized datasets complete. All done !');
    });
  });
});
