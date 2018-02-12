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

DROP SCHEMA <%= suitecExternalSchema %>;

CREATE EXTERNAL SCHEMA <%= suitecExternalSchema %>
FROM data catalog
DATABASE '<%= suitecExternalSchema %>'
iam_role '<%= iamRole %>'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.mixpanel_events (
	event VARCHAR(20000)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3MixpanelLocation %>';

--
-- SuiteC dictionary to extract targetted data for researchers
--
CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.data_access_dict (
  id integer,
  canvas_course_id integer,
  research_group varchar
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '<%= suitecDictionaryLocation %>';

--
-- PostgreSQL database dump
--

--
-- Name: activities
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.activities (
  id integer,
  type varchar,
  object_id integer,
  object_type varchar,
  metadata varchar(20000),
  created_at varchar,
  updated_at varchar,
  asset_id integer,
  course_id integer,
  user_id integer,
  actor_id integer,
  reciprocal_id integer
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/activities';


--
-- Name: activity_types
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.activity_types (
  id integer,
  type varchar,
  points integer,
  enabled boolean,
  course_id integer,
  created_at varchar,
  updated_at varchar
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/activity_types';


--
-- Name: asset_users
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.asset_users (
  created_at varchar,
  updated_at varchar,
  asset_id integer,
  user_id integer
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/asset_users';


--
-- Name: asset_whiteboard_elements
--


CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.asset_whiteboard_elements (
  uid varchar,
  element varchar(50000),
  created_at varchar,
  updated_at varchar,
  asset_id integer,
  element_asset_id integer
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/asset_whiteboard_elements';

--
-- Name: assets
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.assets (
  id integer,
  type varchar,
  url varchar(255),
  download_url varchar(255),
  title varchar(255),
  canvas_assignment_id integer,
  description varchar(2000),
  preview_status varchar(255),
  thumbnail_url varchar(255),
  image_url varchar(255),
  pdf_url varchar(255),
  preview_metadata varchar,
  mime varchar(255),
  source varchar(255),
  body varchar,
  likes integer,
  dislikes integer,
  views integer,
  comment_count integer,
  created_at varchar,
  updated_at varchar,
  deleted_at varchar,
  course_id integer,
  visible boolean,
  impact_percentile integer,
  impact_score integer,
  trending_percentile integer,
  trending_score integer
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/assets';

--
-- Name: assets_categories
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.assets_categories (
  created_at varchar,
  updated_at varchar,
  category_id integer,
  asset_id integer
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/assets_categories';

--
-- Name: canvas
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.canvas (
  canvas_api_domain varchar(255),
  api_key varchar(255),
  lti_key varchar(255),
  lti_secret varchar(255),
  use_https boolean,
  name varchar(255),
  logo varchar(255),
  created_at varchar,
  updated_at varchar,
  supports_custom_messaging boolean
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/canvas';

--
-- Name: categories
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.categories (
  id integer,
  title varchar(500),
  canvas_assignment_id integer,
  canvas_assignment_name varchar(255),
  created_at varchar,
  updated_at varchar,
  course_id integer,
  deleted_at varchar,
  visible boolean
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/categories';

--
-- Name: chats
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.chats (
  id integer,
  body varchar(5000),
  created_at varchar,
  updated_at varchar,
  whiteboard_id integer,
  user_id integer
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/chats';

--
-- Name: comments
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.comments (
  id integer,
  body varchar(20000),
  created_at varchar,
  updated_at varchar,
  asset_id integer,
  user_id integer,
  parent_id integer
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/comments';

--
-- Name: courses
--


CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.courses (
  id integer,
  canvas_course_id integer,
  enable_upload boolean,
  name varchar(255),
  assetlibrary_url varchar(255),
  whiteboards_url varchar(255),
  active boolean,
  created_at varchar,
  updated_at varchar,
  canvas_api_domain varchar(255),
  engagementindex_url varchar(255),
  enable_daily_notifications boolean,
  enable_weekly_notifications boolean,
  dashboard_url varchar(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/courses';

--
-- Name: pinned_user_assets
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.pinned_user_assets (
  asset_id integer,
  user_id integer,
  created_at varchar,
  updated_at varchar
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/pinned_user_assets';

--
-- Name: users
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.users (
  id integer,
  canvas_user_id integer,
  canvas_course_role varchar(255),
  canvas_enrollment_state varchar,
  canvas_full_name varchar(255),
  canvas_image varchar(255),
  canvas_email varchar(255),
  points integer,
  share_points boolean,
  last_activity varchar,
  bookmarklet_token varchar(32),
  created_at varchar,
  updated_at varchar,
  course_id integer,
  canvas_course_sections varchar(255),
  personal_bio varchar(255),
  looking_for_collaborators boolean
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/users';


--
-- Name: whiteboard_elements
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.whiteboard_elements (
  uid integer,
  element varchar(50000),
  created_at varchar,
  updated_at varchar,
  whiteboard_id integer,
  asset_id integer
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/whiteboard_elements';

--
-- Name: whiteboard_members
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.whiteboard_members (
  created_at varchar,
  updated_at varchar,
  user_id integer,
  whiteboard_id integer
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/whiteboard_members';

--
-- Name: whiteboard_sessions
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.whiteboard_sessions (
  socket_id varchar(255),
  created_at varchar,
  updated_at varchar,
  whiteboard_id integer,
  user_id integer
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/whiteboard_sessions';

--
-- Name: whiteboards
--

CREATE EXTERNAL TABLE <%= suitecExternalSchema %>.whiteboards (
  id integer,
  title varchar(255),
  thumbnail_url varchar(255),
  image_url varchar(255),
  created_at varchar,
  updated_at varchar,
  course_id integer,
  deleted_at varchar
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= suitecS3Location %>/whiteboards';
