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
var config = require('config');
var crypto = require('crypto');
var request = require('request');
var url = require('url');
var util = require('util');

var log = require('../core/logger')('store/canvas');

var canvasDataApiKey = config.get('canvasDataApi.key');
var canvasDataApiSecret = config.get('canvasDataApi.secret');
var canvasDataApiHost = config.get('canvasDataApi.host');
var secureHttp = config.get('canvasDataApi.https');

var HMAC_ALG = 'sha256';

/**
 * Make a request to the Canvas Data REST API
 *
 * @param  {String}             path                    The REST API path
 * @param  {Function}           callback                Standard callback function
 * @param  {Object}             callback.data           The REST API response
 */
var dataApiRequest = module.exports.dataApiRequest = function(path, callback) {
  var timestamp = new Date().toISOString();
  var apiPath = '/api/account/self' + path;
  var signature = generateHMACSignature(timestamp, {
    method: 'GET',
    host: canvasDataApiHost,
    path: apiPath
  });
  var url = util.format('%s://%s%s', secureHttp ? 'https' : 'http', canvasDataApiHost, apiPath);

  log.info({canvasDataApiHost: canvasDataApiHost, apiPath: apiPath}, 'Begin request to Canvas Data API');

  request({
    url: url,
    headers: {
      // Each request needs to signed with a signature that is keyed with the
      // API key and salted and signed with the API secret
      Authorization: 'HMACAuth ' + canvasDataApiKey + ':' + signature,
      Date: timestamp
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

  _.each(sortedParams, function(paramName) {
    sortedParts.push(paramName + '=' + urlInfo.query[paramName]);
  });

  return [
    reqOpts.method.toUpperCase(),
    reqOpts.host || '',
    reqOpts.contentType || '',
    reqOpts.contentMD5 || '',
    urlInfo.pathname,
    sortedParts.join('&') || '',
    timestamp,
    canvasDataApiSecret
  ].join('\n');
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
  var hmac = crypto.createHmac(HMAC_ALG, Buffer.from(canvasDataApiSecret));

  hmac.update(message);

  return hmac.digest('base64');
};
