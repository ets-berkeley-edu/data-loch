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
var crypto = require('crypto');
var csv = require('fast-csv');
var fs = require('fs');
var glob = require('glob');
var request = require('request');
var url = require('url');
var util = require('util');
var zlib = require('zlib');

var log = require('./logger');

var API_KEY = config.get('redshift.apiKey');
var API_SECRET = config.get('redshift.apiSecret');
var HOST = config.get('redshift.host');
var HTTPS = config.get('redshift.https');

var HMAC_ALG = 'sha256';

// Variable that will be used to cache the parsed data files
var parsedDataFilesCache = {};

/**
 * Parse a tab delimited Canvas Redshift data file
 *
 * @param  {String}             file                    The base name to the data file(s) that require parsing. For example, 'submission_comment_participant_dim' should be provided to parse 'submission_comment_participant_dim-part-00785' and 'submission_comment_participant_dim-part-00786'
 * @param  {Function}           callback                Standard callback function
 * @param  {Object[]}           callback.data           The aggregated data of the parsed data file(s)
 */
var parseDataFiles = module.exports.parseDataFiles = function(file, callback) {
  // Check if the file(s) have already been parsed
  if (parsedDataFilesCache[file]) {
    return callback(parsedDataFilesCache[file]);
  }

  var data = [];
  parseCSVFiles(file, function(row, done) {
    data.push(row);
    return done();
  }, function() {
    parsedDataFilesCache[file] = data;
    return callback(data);
  });
};

/**
 * TODO
 */
var parseCSVFiles = module.exports.parseCSVFiles = function(file, rowCallback, callback) {
  var directory = config.get('storage') || './data/';
  glob(directory + file + '*', null, function (err, paths) {
    async.eachSeries(paths, function(path, done) {
      // Parse the CSV file
      var stream = fs.createReadStream(path);
      var csvStream = csv({'delimiter': '\t', 'quote': null})
        .validate(function(data, next) {
          rowCallback(data, next);
        })
        .on('data', function(row) {
          log.debug({'row': row}, 'Parsed data file row');
        })
        .on('end', function() {
          log.debug({'file': path}, 'Finished parsing data file');
          return done();
        });
      stream.pipe(csvStream);
    }, function() {
      return callback();
    });
  });
};

/**
 * TODO
 */
var mergeCSVFiles = module.exports.mergeCSVFiles = function(subpath, schema, callback) {
  var merged = {};

  async.eachSeries(schema, function(file, done) {

    var idIndex = _.findIndex(file.mapping, function(mapElement) {
      return !_.isUndefined(mapElement.id);
    });
    var retainIndexes = [];
    _.each(file.mapping, function(mapElement, index) {
      if (_.values(mapElement)[0] === true) {
        retainIndexes.push(index);
      }
    });

    parseCSVFiles(subpath + '/' + file.name, function(row, rowDone) {
      var id = row[idIndex];
      merged[id] = merged[id] || [];
      _.each(retainIndexes, function(retainIndex) {
        merged[id].push(row[retainIndex]);
      });
      return rowDone();
    }, done);
  }, function() {
    return callback(merged);
  });
};

/**
 * Make an API request to the Canvas REST API. This method will page through the full
 * data set and return all data
 *
 * @param  {String}             path                    The REST API path
 * @param  {Function}           callback                Standard callback function
 * @param  {Object}             callback.data           The REST API response
 */
var canvasApiRequest = module.exports.canvasApiRequest = function(path, callback) {
  var domain = config.get('canvas.domain') || config.get('statements.lrs.url');
  var apiKey = config.get('canvas.apiKey');

  var url = util.format('%s%s', domain, path);

  var opts = {
    'url': url,
    'method': 'GET',
    'headers': {
      'Authorization': util.format('Bearer %s', apiKey)
    }
  };
  request(opts, function(err, response, body) {
    if (err) {
      log.error({'err': err}, 'Failed to interact with the Canvas REST API');
      return callback({'code': 500, 'msg': 'Failed to interact with the Canvas REST API'});
    } else if (response.statusCode !== 200) {
      log.error({'err': err,  'statusCode': response.statusCode, 'body': body}, 'Canvas returned a non-200 status code');
      return callback({'code': 500, 'msg': 'Canvas returned a non-200 status code'});
    }

    // Parse the Canvas JSON response
    var data = null;
    try {
      data = JSON.parse(body);
    } catch (parseErr) {
      log.error({'err': parseErr}, 'Failed to parse the Canvas response');
      return callback({'code': 500, 'msg': 'Failed to parse the Canvas response'});
    }

    return callback(null, data);
  });
};

/**
 * Make an API request to the Canvas Data REST API
 *
 * @param  {String}             path                    The REST API path
 * @param  {Function}           callback                Standard callback function
 * @param  {Object}             callback.data           The REST API response
 */
var canvasDataApiRequest = module.exports.canvasDataApiRequest = function(path, callback) {
  var timestamp = new Date().toISOString();
  path = '/api/account/self' + path;
  var signature = generateHMACSignature(timestamp, {
    'method': 'GET',
    'host': HOST,
    'path': path
  });

  var url = util.format('%s://%s%s', (HTTPS ? 'https' : 'http'), HOST, path);
  request({
    'url': url,
    'headers': {
      // Each request needs to signed with a signature that is keyed with the
      // API key and salted and signed with the API secret
      'Authorization': 'HMACAuth ' + API_KEY + ':' + signature,
      'Date': timestamp
    }
  }, function(error, response, body) {
    return callback(JSON.parse(body));
  });
};

/**
 * Generate the message used for Canvas Redshift HMAC API authentication
 *
 * @param  {String}             timestamp               The timestamp in RFC 7321 or ISO-8601 format to use in the message
 * @param  {Object}             reqOpts                 The Canvas Redshift REST API request options
 * @param  {String}             reqOpts.path            The REST API path
 * @param  {String}             reqOpts.method          The REST API method. One of 'GET' or 'POST'
 * @param  {String}             reqOpts.host            The REST API host
 * @param  {String}             [reqOpts.contentType]   The REST API content type
 * @param  {String}             [reqOpts.contentMD5]    The REST API MD5 header
 * @return {String}                                     The generated Canvas Redshift HMAC API authentication message
 * @api private
 */
var generateAuthenticationMessage = function(timestamp, reqOpts) {
  var urlInfo = url.parse(reqOpts.path, true);
  var sortedParams = Object.keys(urlInfo.query).sort(function(a, b) {
    return a.localeCompare(b);
  });
  var sortedParts = [];
  for (var i = 0; i < sortedParams.length; i++) {
    var paramName = sortedParams[i];
    sortedParts.push(paramName + '=' + urlInfo.query[paramName]);
  }
  var parts = [
    reqOpts.method.toUpperCase(),
    reqOpts.host || '',
    reqOpts.contentType || '',
    reqOpts.contentMD5 || '',
    urlInfo.pathname,
    sortedParts.join('&') || '',
    timestamp,
    API_SECRET
  ];
  return parts.join('\n');
};

/**
 * Perform HMAC signing for the Canvas Redshift API authentication message
 *
 * @param  {String}             timestamp               The timestamp in RFC 7321 or ISO-8601 format to use in the message
 * @param  {Object}             reqOpts                 The Canvas Redshift REST API request options
 * @param  {String}             reqOpts.path            The REST API path
 * @param  {String}             reqOpts.method          The REST API method. One of 'GET' or 'POST'
 * @param  {String}             reqOpts.host            The REST API host
 * @param  {String}             [reqOpts.contentType]   The REST API content type
 * @param  {String}             [reqOpts.contentMD5]    The REST API MD5 header
 * @return {String}                                     The HMAC signed Canvas Redshift API authentication message
 * @api private
 */
var generateHMACSignature = function(timestamp, reqOpts) {
  var message = generateAuthenticationMessage(timestamp, reqOpts);
  var hmac = crypto.createHmac(HMAC_ALG, new Buffer(API_SECRET));
  hmac.update(message);
  return hmac.digest('base64');
};

/**
 * Download and unzip a set of Canvas Redshift data files
 *
 * @param  {String}             path                    The name of the folder to download the files to
 * @param  {Object[]}           files                   The Canvas Redshift data files to download and unzip
 * @param  {String}             files[i].url            The download URL of the Canvas Redshift data file
 * @param  {String}             files[i].filename       The title of the Canvas Redshift data file. The expected extension is '.gz'
 * @param  {Function}           callback                Standard callback function
 * @param  {String[]}           callback.filenames      The filenames of the files that have been downloaded
 */
var downloadFiles = module.exports.downloadFiles = function(path, files, callback) {
  var filenames = [];

  async.eachSeries(files, function(file, done) {
    var filename = file.filename.split('.').shift();
    filenames.push(filename);
    var storageDirectory = config.get('storage') || './data/';
    var path =  storageDirectory + path + filename;
    log.info({'file': filename}, 'Starting Canvas data file download');

    // Don't download the file if it has already been downloaded
    fs.exists(path, function(exists) {
      if (exists) {
        log.info({'file': file}, 'Skipping Canvas data file download. File already exists');
        return done();
      } else {
        var start = Date.now();
        var gunzip = zlib.createGunzip();
        gunzip.on('error', function(err) {
          log.error({'err': err, 'file': filename}, 'Unable to gunzip Canvas data file stream');
          return done();
        });
        request(file.url).pipe(gunzip).pipe(fs.createWriteStream(path))
        .on('error', function() {
          log.error({'err': err, 'file': filename}, 'Unable to gunzip Canvas data file stream');
          return done();
        })
        .on('finish', function() {
          var end = Date.now();
          log.info({'file': filename, 'duration': (end - start)}, 'Finished Canvas data file download');
          return done();
        });
      }
    });
  }, function() {
    return callback(filenames);
  });
};
