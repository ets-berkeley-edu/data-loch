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

/* Script to convert requests table from text files to more efficient parquet compressinon
 * Run this hive script on EMR cluster to compress to parquet using SNAPPY orc compression.
 * Alternatively, Glue can be used to run PySpark scripts to achieve the same.
 */

CREATE EXTERNAL TABLE historical_requests (
  id	STRING,
  timestamp_full	STRING,
  timestamp_year	STRING,
  timestamp_month	STRING,
  timestamp_day	STRING,
  user_id	BIGINT,
  course_id	BIGINT,
  root_account_id	BIGINT,
  course_account_id	BIGINT,
  quiz_id	BIGINT,
  discussion_id	BIGINT,
  conversation_id	BIGINT,
  assignment_id	BIGINT,
  url	STRING,
  user_agent	STRING,
  http_method	STRING,
  remote_ip	STRING,
  interaction_micros	BIGINT,
  web_application_controller	STRING,
  web_applicaiton_action	STRING,
  web_application_conSTRING_type	STRING,
  web_application_conSTRING_id	STRING,
  real_user_id	BIGINT,
  session_id	STRING,
  user_agent_id	BIGINT,
  http_status	STRING,
  http_version	STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://path/to/historical/requests/';



CREATE EXTERNAL TABLE historical_requests_parquet (
  id	STRING,
  timestamp_full	STRING,
  timestamp_year	STRING,
  timestamp_month	STRING,
  timestamp_day	STRING,
  user_id	BIGINT,
  course_id	BIGINT,
  root_account_id	BIGINT,
  course_account_id	BIGINT,
  quiz_id	BIGINT,
  discussion_id	BIGINT,
  conversation_id	BIGINT,
  assignment_id	BIGINT,
  url	STRING,
  user_agent	STRING,
  http_method	STRING,
  remote_ip	STRING,
  interaction_micros	BIGINT,
  web_application_controller	STRING,
  web_applicaiton_action	STRING,
  web_application_conSTRING_type	STRING,
  web_application_conSTRING_id	STRING,
  real_user_id	BIGINT,
  session_id	STRING,
  user_agent_id	BIGINT,
  http_status	STRING,
  http_version	STRING
)
STORED AS PARQUET
LOCATION 's3://path/to/historical/complete/requests-parquet-snappy/'
TBLPROPERTIES ("orc.compress"="SNAPPY");


INSERT OVERWRITE TABLE historical_requests_parquet SELECT * FROM historical_requests;
