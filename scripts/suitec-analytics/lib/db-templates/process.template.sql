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
DROP SCHEMA IF EXISTS <%= suitecAnalyticsSchema %> CASCADE;
CREATE SCHEMA <%= suitecAnalyticsSchema %>;

DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.courses;
DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.user_enrollments;
DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.mixpanel_events;
DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.activities;
DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.ei_score_configs;
DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.assets;
DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.asset_users;
DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.asset_categories;
DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.asset_comments;
DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.whiteboards;
DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.whiteboard_members;
DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.whiteboard_chats;

CREATE TABLE <%= suitecAnalyticsSchema %>.data_access_dict AS (
  SELECT
    id,
    canvas_course_id,
    research_group
  FROM suitec_test.data_access_dict
);


CREATE TABLE <%= suitecAnalyticsSchema %>.courses AS (
  WITH
	q1 AS (
    	SELECT
      		canvas_course_id
 		FROM <%= suitecExternalSchema %>.data_access_dict
  	)
	-- courses
	SELECT
	  q2.id,
	  q2.canvas_course_id,
	  q2.name AS canvas_course_name,
	  q2.active::integer,
	  q2.canvas_api_domain,
	  q2.assetlibrary_url,
	  q2.whiteboards_url,
	  q2.engagementindex_url,
	  q2.enable_daily_notifications::integer,
	  q2.enable_weekly_notifications::integer
	FROM <%= suitecExternalSchema %>.courses q2
		INNER JOIN q1
			ON q2.canvas_course_id = q1.canvas_course_id
);


CREATE TABLE <%= suitecAnalyticsSchema %>.user_enrollments AS (
	WITH
		q1 AS (
			SELECT
				id,
	  			canvas_course_id,
	  			canvas_course_name
	  		FROM <%= suitecAnalyticsSchema %>.courses
		)
	SELECT
  		q2.id,
  		q2.canvas_user_id,
  		md5(q2.canvas_user_id) AS hashed_user_id,
  		q2.canvas_course_role,
  		q2.canvas_enrollment_state,
  		q2.canvas_full_name,
  		q2.canvas_email,
  		q2.points,
  		q2.share_points::integer,
  		q2.created_at,
  		q2.updated_at,
  		q2.course_id AS suitec_course_id,
  		q1.canvas_course_id,
  		q1.canvas_course_name
	FROM <%= suitecExternalSchema %>.users q2
		INNER JOIN q1
			ON q2.course_id = q1.id
);


CREATE TABLE <%= suitecAnalyticsSchema %>.mixpanel_events AS (
	WITH
		q1 AS (
			SELECT event
			FROM <%= suitecExternalSchema %>.mixpanel_events
		),
		q2 AS (
			SELECT
	    	json_extract_path_text(q1.event, 'event') as event,
	    	json_extract_path_text(q1.event, 'properties', 'distinct_id') AS distinct_id,
	    	json_extract_path_text(q1.event, 'properties', 'time') AS created_at,
	    	json_extract_path_text(q1.event, 'properties', 'asset_id') AS asset_id,
	    	json_extract_path_text(q1.event, 'properties', 'comment_id') AS asset_comment_id,
	    	json_extract_path_text(q1.event, 'properties', 'whiteboard_id') AS whiteboard_id,
	    	json_extract_path_text(q1.event, 'properties', 'whiteboard_element_id') AS whiteboard_element_id,
	    	json_extract_path_text(q1.event, 'properties') AS event_properties,
	    	q1.event as event_payload
			FROM q1
		),
		q3 AS (
			SELECT
				id AS distinct_id,
				hashed_user_id,
				canvas_user_id,
				canvas_course_role,
				canvas_enrollment_state,
				suitec_course_id,
				canvas_course_id,
				canvas_course_name
			FROM <%= suitecAnalyticsSchema %>.user_enrollments
		)
	SELECT
		  q2.event,
	  	q3.hashed_user_id,
		  q3.canvas_course_id,
		  q3.canvas_course_name,
	  	TIMESTAMP 'epoch' + q2.created_at::BIGINT * INTERVAL '1 second' as created_at,
	  	CASE q2.asset_id
	  		WHEN '' THEN NULL
	    	ELSE q2.asset_id
	  	END,
	  	CASE q2.asset_comment_id
	  		WHEN '' THEN NULL
	    	ELSE q2.asset_comment_id
	  	END,
	  	CASE q2.whiteboard_id
	  		WHEN '' THEN NULL
	    	ELSE q2.whiteboard_id
	  	END,
	  	CASE q2.whiteboard_element_id
	  		WHEN '' THEN NULL
	    	ELSE q2.whiteboard_element_id
	  	END,
	  	q2.event_properties
	FROM q2
		INNER JOIN q3
			ON q2.distinct_id = q3.distinct_id
);

-- suitec activities table clean up
CREATE TABLE <%= suitecAnalyticsSchema %>.activities AS (
	WITH
		q1 AS (
		  SELECT
		  	id as user_id,
		  	canvas_user_id,
		  	hashed_user_id,
		  	suitec_course_id,
		  	canvas_course_id,
		  	canvas_course_name
		  FROM <%= suitecAnalyticsSchema %>.user_enrollments
		)
		SELECT
		 	  q2.id,
		  	q2.type,
		  	q2.object_id,
		  	q2.object_type,
		  	q2.created_at,
		  	q2.updated_at,
		  	q2.asset_id,
		  	q1.canvas_course_id,
		  	q1.canvas_course_name,
		  	q1.hashed_user_id,
		  	q2.actor_id,
		  	q2.reciprocal_id
		 FROM <%= suitecExternalSchema %>.activities q2
		 	INNER JOIN q1
		 		ON q2.user_id = q1.user_id
		 			AND q2.course_id = q1.suitec_course_id
);

-- engagement index configurations data clean up (suitec activity_types table)
CREATE TABLE <%= suitecAnalyticsSchema %>.ei_score_configs AS (
	WITH
		q1 AS (
		  SELECT
		  	id,
		  	canvas_course_id,
		  	canvas_course_name
		  FROM <%= suitecAnalyticsSchema %>.courses
		)
		SELECT
			q2.id,
			q2.type,
			q2.points,
			q2.enabled::integer,
			q1.canvas_course_id,
			q1.canvas_course_name,
			q2.created_at,
			q2.updated_at
		FROM <%= suitecExternalSchema %>.activity_types q2
			INNER JOIN q1
				ON q2.course_id = q1.id
);


-- assets clean up
CREATE TABLE <%= suitecAnalyticsSchema %>.assets AS (
	WITH
		q1 AS (
			SELECT
				id as suitec_course_id,
				canvas_course_id,
				canvas_course_name,
				assetlibrary_url,
				whiteboards_url,
				engagementindex_url
			FROM <%= suitecAnalyticsSchema %>.courses
		),
		q2 AS (
			SELECT
				id as asset_id,
	 			type,
	 			title,
	 			description,
	 			mime,
	 			likes,
	 			dislikes,
	 			views,
	 			comment_count,
	 			created_at,
	 			course_id AS suitec_course_id,
	 			canvas_assignment_id
	 		FROM <%= suitecExternalSchema %>.assets
		),
		q3 AS (
			SELECT asset_id, count(*) as re_uses
			FROM <%= suitecExternalSchema %>.whiteboard_elements
			WHERE asset_id IS NOT NULL
			GROUP BY asset_id
		)
	SELECT
		q2.asset_id,
		q2.type AS file_type,
		q2.title,
		q2.description,
		q2.mime,
		q2.likes,
		q2.dislikes,
		q2.views,
		q2.comment_count,
		q3.re_uses,
		q2.created_at,
		q1.canvas_course_id,
		q1.canvas_course_name,
		q2.canvas_assignment_id,
		q1.assetlibrary_url + '#col_asset=' + q2.asset_id::varchar AS asset_link
	FROM q2
		INNER JOIN q1 ON
			q2.suitec_course_id = q1.suitec_course_id
		LEFT JOIN q3 ON
			q2.asset_id = q3.asset_id
);

-- asset users clean up
CREATE TABLE <%= suitecAnalyticsSchema %>.asset_users AS (
	WITH
		q1 AS (
		    SELECT
			  	id as user_id,
			  	canvas_user_id,
			  	hashed_user_id,
			  	suitec_course_id,
			  	canvas_course_id,
			  	canvas_course_name
		    FROM <%= suitecAnalyticsSchema %>.user_enrollments
		),
		q2 AS (
			SELECT
				asset_id
			FROM <%= suitecAnalyticsSchema %>.assets
		)

		SELECT
			q3.created_at,
  			q3.updated_at,
  			q3.asset_id,
  			q1.hashed_user_id
  		FROM <%= suitecExternalSchema %>.asset_users q3
  			INNER JOIN q2
  				ON q3.asset_id = q2.asset_id
  			INNER JOIN q1
  				ON q3.user_id = q1.user_id
);

-- asset categories clean up
CREATE TABLE <%= suitecAnalyticsSchema %>.asset_categories AS (
	WITH
		q1 AS (
			SELECT
				asset_id
			FROM <%= suitecAnalyticsSchema %>.assets
		)
	SELECT
  		q2.created_at,
  		q2.updated_at,
  		q2.category_id,
  		q2.asset_id
  	FROM <%= suitecExternalSchema %>.assets_categories q2
  		INNER JOIN q1 ON
  			q2.asset_id = q1.asset_id
 );

 -- asset_comments clean up
 CREATE TABLE <%= suitecAnalyticsSchema %>.asset_comments AS (
 	WITH
 		q1 AS (
 		    SELECT
 			  	id as user_id,
 			  	canvas_user_id,
 			  	hashed_user_id,
 			  	suitec_course_id,
 			  	canvas_course_id,
 			  	canvas_course_name
 		    FROM <%= suitecAnalyticsSchema %>.user_enrollments
 		),
 		-- Requires filtered assets table to be created before running this query or it will fail.
 		q2 AS (
 			SELECT
 				asset_id
 			FROM <%= suitecAnalyticsSchema %>.assets
 		)

 	SELECT
 		q3.id as comment_id,
 	  	q3.body,
 	  	q3.created_at,
 	  	q3.updated_at,
 	  	q3.asset_id,
 	  	q1.hashed_user_id,
   		q3.parent_id
   	FROM <%= suitecExternalSchema %>.comments q3
   		INNER JOIN q2
   			ON q3.asset_id = q2.asset_id
   		INNER JOIN q1
   			ON q3.user_id = q1.user_id
  );

-- whiteboards clean up
CREATE TABLE <%= suitecAnalyticsSchema %>.whiteboards AS (
	WITH
		q1 AS (
			SELECT
				id as suitec_course_id,
				canvas_course_id,
				canvas_course_name,
				assetlibrary_url,
				whiteboards_url,
				engagementindex_url
			FROM <%= suitecAnalyticsSchema %>.courses
		),
		q2 AS (
			SELECT whiteboard_id, count(asset_id) as assets_used_count
			FROM <%= suitecExternalSchema %>.whiteboard_elements
			GROUP BY whiteboard_id
		)

	SELECT
 		  q3.id as whiteboard_id,
  		q3.title,
  		q3.created_at,
  		q3.updated_at,
  		q1.canvas_course_id,
  		q1.canvas_course_name,
  		q3.deleted_at,
  		q2.assets_used_count
  	FROM <%= suitecExternalSchema %>.whiteboards q3
  		INNER JOIN q1
  			ON q3.course_id = q1.suitec_course_id
  		INNER JOIN q2
  			ON q3.id = q2.whiteboard_id
);

-- whiteboard members clean up
CREATE TABLE <%= suitecAnalyticsSchema %>.whiteboard_members AS (
	WITH
		q1 AS (
		    SELECT
			  	id as user_id,
			  	canvas_user_id,
			  	hashed_user_id,
			  	suitec_course_id,
			  	canvas_course_id,
			  	canvas_course_name
		    FROM <%= suitecAnalyticsSchema %>.user_enrollments
		),
		q2 AS (
			SELECT
				whiteboard_id
			FROM <%= suitecAnalyticsSchema %>.whiteboards
		)

		SELECT
			q3.created_at,
  			q3.updated_at,
  			q3.whiteboard_id,
  			q1.hashed_user_id
  		FROM <%= suitecExternalSchema %>.whiteboard_members q3
  			INNER JOIN q2
  				ON q3.whiteboard_id = q2.whiteboard_id
  			INNER JOIN q1
  				ON q3.user_id = q1.user_id
);

-- chats clean up
CREATE TABLE <%= suitecAnalyticsSchema %>.whiteboard_chats AS (
	WITH
		q1 AS (
		    SELECT
			  	id as user_id,
			  	canvas_user_id,
			  	hashed_user_id,
			  	suitec_course_id,
			  	canvas_course_id,
			  	canvas_course_name
		    FROM <%= suitecAnalyticsSchema %>.user_enrollments
		),
		-- Requires filtered whiteboards table to be created before running this query or it will fail.
		q2 AS (
			SELECT
				whiteboard_id
			FROM <%= suitecAnalyticsSchema %>.whiteboards
		)

	SELECT
		q3.id as chat_id,
	  	q3.body,
	  	q3.created_at,
	  	q3.updated_at,
	  	q3.whiteboard_id,
	  	q1.hashed_user_id
  	FROM <%= suitecExternalSchema %>.chats q3
  		INNER JOIN q2
  			ON q3.whiteboard_id = q2.whiteboard_id
  		INNER JOIN q1
  			ON q3.user_id = q1.user_id
 );
