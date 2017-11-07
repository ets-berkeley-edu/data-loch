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
var config = require('config');
var fs = require('fs');
var path = require('path');
var Redshift = require('node-redshift');

var constants = require('./constants');
var log = require('../logger')('db');
var storage = require('./storage');

var client = {
  user: config.get('dataLake.db.user'),
  database: config.get('dataLake.db.database'),
  password: config.get('dataLake.db.password'),
  port: config.get('dataLake.db.port'),
  host: config.get('dataLake.db.host')
};

// The values passed in to the options object will be the difference between a
// connection pool and raw connection.
var redshiftClient = new Redshift(client);

/**
 * Push artifacts to Amazon Redshift and Canvas
 *
 * @param  {Function}           callback                Standard callback function
 */
var loadSchema = module.exports.loadSchema = function(callback) {
  createSqlTemplates(function(err) {
    if (err) {
      return callback(err);
    }

    createDatabase(function(err) {
      if (err) {
        log.error({err: err.message}, 'Canvas Data restore on Redshift instance failed');

        return callback(err);
      }

      log.info('Canvas Data instance has been created on Redshift.');

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
  var dbCreationTemplate = path.join(__dirname, '/db-templates/dbCreation.template.sql');
  var dbCreationScript = path.join(__dirname, '../../sql/dbCreation.sql');

  generateSqlScript(dbCreationTemplate, dbCreationScript, function(err) {
    if (err) {
      log.error({err: err, template: dbCreationTemplate, script: dbCreationScript}, 'Failed to generate SQL script using db-template.');

      return callback(err);
    }

    var dbRepointTemplate = path.join(__dirname, '/db-templates/dbRepoint.template.sql');
    var dbRepointScript = path.join(__dirname, '../../sql/dbRepoint.sql');

    generateSqlScript(dbRepointTemplate, dbRepointScript, function(err) {
      if (err) {
        log.error({err: err, template: dbRepointTemplate, script: dbRepointScript}, 'Failed to generate SQL script using db-template.');

        return callback(err);
      }

      return callback();
    });
  });
};

/**
 * @param  {Number}       year                Requested year of enrollments
 * @param  {String}       semesterCode        Requested semester of enrollments: 'B' for Spring, 'C' for Summer and 'D' for Fall.
 * @param  {Number[]}     uids                One or more user UIDs
 * @return {String}                           Valid SQL intended for the 'lakeview' database
 */
var constructEnrollmentsSQL = module.exports.constructEnrollmentsSQL = function(year, semesterCode, uids, callback) {
  var termId = constants.BCOURSES.ENROLLMENT_TERM[year + semesterCode];

  if (!termId) {
    return callback({message: 'Error. No bCourses termId available for ' + year + semesterCode});
  }

  var tokenMap = {
    enrollmentTermId: termId,
    uids: '\'' + _.concat(uids.join('\', \'')) + '\''
  };
  var sqlFile = path.join(__dirname, '../../sql/getEnrollments.sql');

  fs.readFile(sqlFile, 'utf8', function(err, template) {
    if (err) {
      return callback(err);
    }
    var sql = template;
    var done = _.after(_.size(tokenMap), callback);

    _.each(tokenMap, function(value, key) {
      sql = sql.replace(':' + key, value);

      return done(null, sql);
    });
  });
};

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
      externalDatabase: config.get('dataLake.canvasData.externalDatabase'),
      s3Location: config.get('dataLake.canvasData.s3Location') + '/' + storage.generateHash(),
      iamRole: config.get('dataLake.canvasData.iamRole')
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
 * Runs SQL on Redshift instance
 *
 * @param  {String}           sql                     SQL to be executed
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
var runSQL = function(sql, callback) {
  redshiftClient.query(sql, function(err, data) {
    if (err) {
      log.error({err: err, sql: sql}, 'SQL query failed');

      return callback(err);
    }

    return callback(err, data);
  });
};

/**
 * Connects to redshift instance and executes all the SQL scripts asynchronously.
 * Note - Do not use the function if the SQL queries need to be executed sequentially.
 *
 * @param  {String}           sqlScript               Path to the SQL script to be executed
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
var runSQLScript = function(sqlScript, callback) {
  fs.readFile(sqlScript, function(err, data) {
    if (err) {
      log.error({err: err, sqlScript: sqlScript}, 'Failed to find/open SQL script');

      return callback(err);
    }
    var queries = data.toString().split(';');

    async.eachSeries(queries, function(query, done) {
      runSQL(query + ';', function(err, data) {
        if (err) {
          return done(err);
        }

        return done();
      });
    },
    function(err) {
      if (err) {
        return callback(err);
      }
      log.info({sqlScript: sqlScript}, 'Redshift has successfully run SQL script');

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
  runSQLScript(path.join(__dirname, '../../sql/dbCreation.sql'), function(err) {
    if (err) {
      log.error({err: err}, 'Failed to create database');

      return callback(err);
    }

    return callback();
  });
};

/**
 * TODO: How and when is this function used?
 *
 * Repoints the external database and tables referring the S3 storage buckets location using redshift spectrum.
 * When new data dumps are available on S3, if the tables already exist then repoint the external database/schemas
 * to new S3 location by running Alter statements.
 *
 * @param  {Function}         callback                Standard callback function
 * @param  {Object}           callback.err            An error object, if any
 */
var repointDatabase = module.exports.repointDatabase = function(callback) {
  runSQLScript(path.join(__dirname, '../../sql/dbRepoint.sql'), function(err) {
    if (err) {
      log.error({err: err}, 'Failed to repoint database');
      return callback(err);
    }

    return callback();
  });
};
