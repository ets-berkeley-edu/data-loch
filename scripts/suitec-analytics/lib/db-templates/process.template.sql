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

/**
 * SuiteC/mixpanel related data-loch queries for suitec researchers
 *
 */

DROP SCHEMA IF EXISTS <%= suitecAnalyticsSchema %> CASCADE;
CREATE SCHEMA <%= suitecAnalyticsSchema %>;

-- Boilerplate queries for mixpanel/suitec data for researchers
DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.mixpanel_events;

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
				id as distinct_id,
				course_id as suitec_course_id,
				canvas_user_id,
				canvas_full_name
			FROM <%= suitecExternalSchema %>.users
		),
		q4 AS (
			SELECT
				id as suitec_course_id,
				canvas_course_id,
				name as canvas_course_name
			FROM <%= suitecExternalSchema %>.courses
		)

	SELECT
		q2.event,
	  q2.distinct_id,
	  q3.canvas_user_id,
	  md5(q3.canvas_user_id) as user_id_hash,
		q3.canvas_full_name,
	  q3.suitec_course_id,
		q4.canvas_course_id,
		q4.canvas_course_name,
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
	  q2.event_properties,
	  q2.event_payload
	FROM q2
		LEFT JOIN q3
			ON q2.distinct_id = q3.distinct_id
		LEFT JOIN q4
			ON q3.suitec_course_id = q4.suitec_course_id
);

DROP TABLE IF EXISTS <%= suitecAnalyticsSchema %>.assets_view;

CREATE TABLE <%= suitecAnalyticsSchema %>.assets_view AS (
	WITH
		q1 AS (
			SELECT
				id as suitec_course_id,
				canvas_course_id,
				name as canvas_course_name,
				assetlibrary_url,
				whiteboards_url,
				engagementindex_url
			FROM <%= suitecExternalSchema %>.courses
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
		q2.suitec_course_id,
		q1.canvas_course_id,
		q1.canvas_course_name,
		q2.canvas_assignment_id,
		q1.assetlibrary_url + '#col_asset=' + q2.asset_id::varchar AS asset_link
	FROM q2
		LEFT JOIN q1 ON
			q2.suitec_course_id = q1.suitec_course_id
		LEFT JOIN q3 ON
			q2.asset_id = q3.asset_id
);
