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

var _ = require('lodash');
var async = require('async');
var config = require('config');
var fs = require('fs');
var path = require('path');
var Redshift = require('node-redshift');

var log = require('../../../lib/core/logger')('syncSuitecDb');
var db = require('../../../lib/sync/db.js');

/**
 * Upload the compressed data files for each table from Canvas Data on to S3 buckets.
 *
 * @param  {String}           templateFile            Path to the template file to generate sql scripts
 * @param  {String}           outputFile              Path to where the template output file will be uploaded
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
var generateSqlScript = function(templateFile, outputFile, callback) {
  fs.readFile(templateFile, 'utf8', function(err, template) {
    if (err) {
      return log.error('An error occurred when reading the Apache config template');
    }
    // Generate SQL script with template and replacement args.
    var templateData = {
      suitecExternalSchema: config.get('datalake.suitec.externalSchema'),
      suitecAnalyticsSchema: config.get('datalake.suitec.analyticsSchema'),
      suitecS3Location: 's3://' + config.get('datalake.s3.bucket') + '/' + config.get('datalake.suitec.directory.suitec'),
      suitecS3MixpanelLocation: 's3://' + config.get('datalake.s3.bucket') + '/' + config.get('datalake.suitec.directory.mixpanelEvents'),
      iamRole: config.get('datalake.redshift.iamRole'),
      suitecDictionaryLocation: 's3://' + config.get('datalake.s3.bucket') + '/suiteC/dictionary',
      researchGroupRequestingData: config.get('datalake.suitec.researchGroupRequestingData')
    };
    var templateOutput = _.template(template)(templateData);

    // Store the generated template output sql file
    fs.writeFile(outputFile, templateOutput, function(err) {
      if (err) {
        log.error({template: templateFile, outputFile: outputFile}, 'Failed to write SQL script to disk');

        return callback(err);
      }

      log.info({script: outputFile}, 'Successfully generated SQL script');
      return callback();
    });
  });
};

/**
 * Upload the compressed data files for each table from Canvas Data on to S3 buckets.
 *
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
var createSqlTemplates = function(callback) {
  var suitecTemplate = path.join(__dirname, '/db-templates/suitec.template.sql');
  var suitecDbScript = path.join(__dirname, '/sql/suitecDbCreate.sql');

  generateSqlScript(suitecTemplate, suitecDbScript, function(err) {
    if (err) {
      log.error({err: err, template: dbCreationTemplate, script: dbCreationScript}, 'Failed to generate SQL script using db-template.');

      return callback(err);
    }

    // var suitecProcessTemplate = path.join(__dirname, '/db-templates/process.template.sql');
    var suitecProcessTemplate = path.join(__dirname, '/db-templates/process.template.sql');
    var suitecProcessScript = path.join(__dirname, '/sql/suitecProcess.sql');

    generateSqlScript(suitecProcessTemplate, suitecProcessScript, function(err) {
      if (err) {
        log.error({err: err, template: dbRepointTemplate, script: dbRepointScript}, 'Failed to generate SQL script using db-template.');

        return callback(err);
      }

      return callback();
    });
  });
};

/**
 * Creates external database and tables referring the S3 storage buckets (data lake) using Redshift Spectrum.
 * The external tables/schema will be available in data catalog and can be accessed from both Athena and Spectrum
 *
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
var createDatabase = function(callback) {
  db.runSQLScript(path.join(__dirname, 'sql/suitecDbCreate.sql'), function(err) {
    if (err) {
      log.error({err: err}, 'Failed to create database');

      return callback(err);
    }

    return callback();
  });
};

/**
 * Creates external database and tables referring the S3 storage buckets (data lake) using Redshift Spectrum.
 * The external tables/schema will be available in data catalog and can be accessed from both Athena and Spectrum
 *
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
var generateAnalyticsTables = function(callback) {
  log.info('Run analytic pipelines... this might take a while.');
  db.runSQLScript(path.join(__dirname, 'sql/suitecProcess.sql'), function(err) {
    if (err) {
      log.error({err: err}, 'Failed to create database');

      return callback(err);
    }

    return callback();
  });
};

var loadSchema = module.exports.loadSchema = function(callback) {
  var externalSchema = config.get('datalake.suitec.externalSchema');

  db.resetExternalSchema(externalSchema, function(err) {
    if (err) {
      return callback(err);
    }

    createSqlTemplates(function(err) {
      if (err) {
        return callback(err);
      }

      createDatabase(function(err) {
        if (err) {
          log.error({err: err.message}, 'Suitec and Mixpanel data restore on Redshift instance failed');
          return callback(err);

        }

        log.info('SuiteC and Mixpanel database has been restored on data loch successfully.');
        return callback();
      });
    });
  });
};

/**
 *
 *
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
loadSchema(function(err) {
  if (err) {
    log.error('Completed with errors.');
  }

  generateAnalyticsTables(function(err) {
    if (err) {
      log.error('Completed with errors.');
    }

    log.info('All done.');
  });
});
