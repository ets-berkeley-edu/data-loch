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

var DataLoch = require('../core/api');
var syncDumps = require('../store/syncDumps');
var syncDb = require('../sync/syncDb');
var log = require('../core/logger')('store/rest');

/* !
 * Sync latest Canvas data dump files on S3
 */
DataLoch.apiRouter.post('/syncCanvasDumps', function(req, res) {
  // Sync data model
  syncDumps.run(function(err) {
    if (err) {
      log.error(err);
      return res.status(err.code).send(err.msg);
    }

    log.info('Canvas Dumps successfully refreshed on S3.');
    return res.sendStatus(200);

  });
});

/* !
 * Refresh Canvas databases
 */
DataLoch.apiRouter.post('/createCanvasSchema', function(req, res) {
  // Sync data model
  syncDb.run(function(err) {
    if (err) {
      log.error(err);
      return res.status(err.code).send(err.msg);
    }

    log.info('Canvas Databases successfully refreshed on Redshift.');
    return res.sendStatus(200);
  });
});
