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

--------------------------------------------------------------------
-- DROP and CREATE EXTERNAL SCHEMA
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS <%= externalSchema %> CASCADE;

CREATE EXTERNAL SCHEMA <%= externalSchema %>
FROM data catalog
DATABASE '<%= externalSchema %>'
IAM_ROLE '<%= iamRole %>'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- user_dim
CREATE EXTERNAL TABLE <%= externalSchema %>.user_dim (
    id BIGINT,
    canvas_id BIGINT,
    root_account_id BIGINT,
    name VARCHAR,
    time_zone VARCHAR,
    created_at TIMESTAMP,
    visibility VARCHAR,
    school_name VARCHAR,
    school_position VARCHAR,
    gender VARCHAR,
    locale VARCHAR,
    public VARCHAR,
    birthdate TIMESTAMP,
    country_code VARCHAR,
    workflow_state VARCHAR,
    sortable_name VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/user_dim';

-- pseudonym_dim
CREATE EXTERNAL TABLE <%= externalSchema %>.pseudonym_dim (
    id BIGINT,
    canvas_id BIGINT,
    user_id BIGINT,
    account_id BIGINT,
    workflow_state VARCHAR,
    last_request_at TIMESTAMP,
    last_login_at TIMESTAMP,
    current_login_at TIMESTAMP,
    last_login_ip VARCHAR,
    current_login_ip VARCHAR,
    position INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    password_auto_generated BOOLEAN,
    deleted_at TIMESTAMP,
    sis_user_id VARCHAR,
    unique_name VARCHAR,
    integration_id VARCHAR,
    authentication_provider_id	BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/pseudonym_dim';

-- course_dim
CREATE EXTERNAL TABLE <%= externalSchema %>.course_dim (
    id BIGINT,
    canvas_id BIGINT,
    root_account_id BIGINT,
    account_id BIGINT,
    enrollment_term_id BIGINT,
    name VARCHAR,
    code VARCHAR,
    type VARCHAR,
    created_at TIMESTAMP,
    start_at TIMESTAMP,
    conclude_at TIMESTAMP,
    publicly_visible BOOLEAN,
    sis_source_id VARCHAR,
    workflow_state VARCHAR,
    wiki_id	BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/course_dim';

-- course_section_dim
CREATE EXTERNAL TABLE <%= externalSchema %>.course_section_dim (
    id BIGINT,
    canvas_id BIGINT,
    name VARCHAR,
    course_id BIGINT,
    enrollment_term_id BIGINT,
    default_section BOOLEAN,
    accepting_enrollments BOOLEAN,
    can_manually_enroll BOOLEAN,
    start_at TIMESTAMP,
    end_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    workflow_state VARCHAR,
    restrict_enrollments_to_section_dates BOOLEAN,
    nonxlist_course_id BIGINT,
    sis_source_id VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/course_section_dim';

-- enrollment_fact
CREATE EXTERNAL TABLE <%= externalSchema %>.enrollment_fact(
    enrollment_id BIGINT,
    user_id BIGINT,
    course_id BIGINT,
    enrollment_term_id BIGINT,
    course_account_id BIGINT,
    course_section_id BIGINT,
    computed_final_score	DOUBLE PRECISION,
    computed_current_score	DOUBLE PRECISION
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/enrollment_fact';

-- enrollment_dim
CREATE EXTERNAL TABLE <%= externalSchema %>.enrollment_dim(
    id BIGINT,
    canvas_id BIGINT,
    root_account_id BIGINT,
    course_section_id BIGINT,
    role_id BIGINT,
    type VARCHAR,
    workflow_state VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    start_at TIMESTAMP,
    end_at TIMESTAMP,
    completed_at TIMESTAMP,
    self_enrolled BOOLEAN,
    sis_source_id VARCHAR,
    course_id BIGINT,
    user_id	BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/enrollment_dim';

-- assignment_fact
CREATE EXTERNAL TABLE <%= externalSchema %>.assignment_fact(
    assignment_id BIGINT,
    course_id BIGINT,
    course_account_id VARCHAR,
    enrollment_term_id VARCHAR,
    points_possible TIMESTAMP,
    peer_review_count TIMESTAMP,
    assignment_group_id BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/assignment_fact';

-- assignment_dim
CREATE EXTERNAL TABLE <%= externalSchema %>.assignment_dim(
    id BIGINT,
    canvas_id BIGINT,
    course_id BIGINT,
    title VARCHAR,
    description VARCHAR,
    due_at TIMESTAMP,
    unlock_at TIMESTAMP,
    lock_at TIMESTAMP,
    points_possible	DOUBLE PRECISION,
    grading_type VARCHAR,
    submission_types VARCHAR,
    workflow_state VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    peer_review_count INT,
    peer_reviews_due_at TIMESTAMP,
    peer_reviews_assigned BOOLEAN,
    peer_reviews BOOLEAN,
    automatic_peer_reviews BOOLEAN,
    all_day BOOLEAN,
    all_day_date TIMESTAMP,
    could_be_locked BOOLEAN,
    grade_group_students_individually BOOLEAN,
    anonymous_peer_reviews BOOLEAN,
    muted BOOLEAN,
    assignment_group_id BIGINT,
    position INT,
    visibility VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/assignment_dim';

-- assignment_override_fact
CREATE EXTERNAL TABLE <%= externalSchema %>.assignment_override_fact(
  assignment_override_id BIGINT,
  account_id BIGINT,
  assignment_id BIGINT,
  assignment_group_id BIGINT,
  course_id BIGINT,
  course_section_id BIGINT,
  enrollment_term_id BIGINT,
  group_id BIGINT,
  group_category_id BIGINT,
  group_parent_account_id BIGINT,
  nonxlist_course_id BIGINT,
  quiz_id BIGINT,
  group_wiki_id BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/assignment_override_fact';

-- assignment_override_dim
CREATE EXTERNAL TABLE <%= externalSchema %>.assignment_override_dim(
  id BIGINT,
  canvas_id BIGINT,
  assignment_id BIGINT,
  course_section_id BIGINT,
  group_id BIGINT,
  quiz_id BIGINT,
  all_day VARCHAR,
  all_day_date TIMESTAMP,
  assignment_version INT,
  created_at TIMESTAMP,
  due_at TIMESTAMP,
  due_at_overridden VARCHAR,
  lock_at TIMESTAMP,
  lock_at_overridden VARCHAR,
  set_type VARCHAR,
  title VARCHAR,
  unlock_at TIMESTAMP,
  unlock_at_overridden VARCHAR,
  updated_at TIMESTAMP,
  quiz_version INT,
  workflow_state VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/assignment_override_dim';

-- assignment_override_user_fact
CREATE EXTERNAL TABLE <%= externalSchema %>.assignment_override_user_fact(
  assignment_override_user_id BIGINT,
  account_id BIGINT,
  assignment_group_id BIGINT,
  assignment_id BIGINT,
  assignment_override_id BIGINT,
  course_id BIGINT,
  enrollment_term_id BIGINT,
  quiz_id BIGINT,
  user_id BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/assignment_override_user_fact';

-- assignment_override_user_dim
CREATE EXTERNAL TABLE <%= externalSchema %>.assignment_override_user_dim(
  id BIGINT,
  canvas_id BIGINT,
  assignment_id BIGINT,
  assignment_override_id BIGINT,
  quiz_id BIGINT,
  user_id BIGINT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/assignment_override_user_dim';

-- assignment_override_user_rollup_fact
CREATE EXTERNAL TABLE <%= externalSchema %>.assignment_override_user_rollup_fact(
  assignment_id BIGINT,
  assignment_override_id BIGINT,
  assignment_override_user_adhoc_id BIGINT,
  assignment_group_id BIGINT,
  course_id BIGINT,
  course_account_id BIGINT,
  course_section_id BIGINT,
  enrollment_id BIGINT,
  enrollment_term_id BIGINT,
  group_category_id BIGINT,
  group_id BIGINT,
  group_parent_account_id BIGINT,
  group_wiki_id BIGINT,
  nonxlist_course_id BIGINT,
  quiz_id BIGINT,
  user_id BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/assignment_override_user_rollup_fact';

-- discussion_entry_dim
CREATE EXTERNAL TABLE <%= externalSchema %>.discussion_entry_dim(
    id BIGINT,
    canvas_id BIGINT,
    message VARCHAR,
    workflow_state VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP,
    depth	INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/discussion_entry_dim';

-- discussion_entry_fact
CREATE EXTERNAL TABLE <%= externalSchema %>.discussion_entry_fact(
    discussion_entry_id BIGINT,
    parent_discussion_entry_id BIGINT,
    user_id BIGINT,
    topic_id BIGINT,
    course_id BIGINT,
    enrollment_term_id BIGINT,
    course_account_id BIGINT,
    topic_user_id BIGINT,
    topic_assignment_id BIGINT,
    topic_editor_id BIGINT,
    enrollment_rollup_id BIGINT,
    message_length	INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/discussion_entry_fact';

-- discussion_topic_dim
CREATE EXTERNAL TABLE <%= externalSchema %>.discussion_topic_dim(
    id BIGINT,
    canvas_id BIGINT,
    title VARCHAR,
    message VARCHAR,
    type VARCHAR,
    workflow_state VARCHAR,
    last_reply_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    delayed_post_at TIMESTAMP,
    posted_at TIMESTAMP,
    deleted_at TIMESTAMP,
    discussion_type VARCHAR,
    pinned BOOLEAN,
    locked BOOLEAN,
    course_id BIGINT,
    group_id	BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/discussion_topic_dim';

-- discussion_topic_fact
CREATE EXTERNAL TABLE <%= externalSchema %>.discussion_topic_fact(
    discussion_topic_id BIGINT,
    course_id BIGINT,
    enrollment_term_id BIGINT,
    course_account_id BIGINT,
    user_id BIGINT,
    assignment_id BIGINT,
    editor_id BIGINT,
    enrollment_rollup_id BIGINT,
    message_length INT,
    group_id BIGINT,
    group_parent_course_id BIGINT,
    group_parent_account_id BIGINT,
    group_parent_course_account_id	BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/discussion_topic_fact';

-- submission_fact
CREATE EXTERNAL TABLE <%= externalSchema %>.submission_fact(
    submission_id BIGINT,
    assignment_id BIGINT,
    course_id BIGINT,
    enrollment_term_id BIGINT,
    user_id BIGINT,
    grader_id BIGINT,
    course_account_id BIGINT,
    enrollment_rollup_id BIGINT,
    score	DOUBLE PRECISION,
    published_score	DOUBLE PRECISION,
    what_if_score	DOUBLE PRECISION,
    submission_comments_count INT,
    account_id BIGINT,
    assignment_group_id BIGINT,
    group_id BIGINT,
    quiz_id BIGINT,
    quiz_submission_id BIGINT,
    wiki_id	BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/submission_fact';

-- submission_dim
CREATE EXTERNAL TABLE <%= externalSchema %>.submission_dim(
    id BIGINT,
    canvas_id BIGINT,
    body TEXT,
    url VARCHAR,
    grade VARCHAR,
    submitted_at TIMESTAMP,
    submission_type VARCHAR,
    workflow_state VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    processed BOOLEAN,
    process_attempts INT,
    grade_matches_current_submission BOOLEAN,
    graded_at TIMESTAMP,
    has_rubric_assessment BOOLEAN,
    attempt INT,
    has_admin_comment BOOLEAN,
    assignment_id BIGINT,
    excused VARCHAR,
    graded_anonymously VARCHAR,
    grader_id BIGINT,
    group_id BIGINT,
    quiz_submission_id BIGINT,
    user_id BIGINT,
    grade_state VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/submission_dim';

-- submission_comment_fact
CREATE EXTERNAL TABLE <%= externalSchema %>.submission_comment_fact(
    submission_comment_id BIGINT,
    submission_id BIGINT,
    recipient_id BIGINT,
    author_id BIGINT,
    assignment_id BIGINT,
    course_id BIGINT,
    enrollment_term_id BIGINT,
    course_account_id BIGINT,
    message_size_bytes INT,
    message_character_count INT,
    message_word_count INT,
    message_line_count	INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/submission_comment_fact';

-- submission_comment_participant_fact
CREATE EXTERNAL TABLE <%= externalSchema %>.submission_comment_participant_fact(
    submission_comment_participant_id BIGINT,
    submission_comment_id BIGINT,
    user_id BIGINT,
    submission_id BIGINT,
    assignment_id BIGINT,
    course_id BIGINT,
    enrollment_term_id BIGINT,
    course_account_id BIGINT,
    enrollment_rollup_id	BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/submission_comment_participant_fact';

-- submission_comment_dim
CREATE EXTERNAL TABLE <%= externalSchema %>.submission_comment_dim(
    id BIGINT,
    canvas_id BIGINT,
    submission_id BIGINT,
    recipient_id BIGINT,
    author_id BIGINT,
    assessment_request_id BIGINT,
    group_comment_id VARCHAR,
    comment VARCHAR,
    author_name VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    anonymous BOOLEAN,
    teacher_only_comment BOOLEAN,
    hidden	BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3DailyLocation %>/submission_comment_dim';

-- requests
CREATE EXTERNAL TABLE <%= externalSchema %>.requests(
    id VARCHAR,
    timestamp TIMESTAMP,
    timestamp_year VARCHAR,
    timestamp_month VARCHAR,
    timestamp_day VARCHAR,
    user_id BIGINT,
    course_id BIGINT,
    root_account_id BIGINT,
    course_account_id BIGINT,
    quiz_id BIGINT,
    discussion_id BIGINT,
    conversation_id BIGINT,
    assignment_id BIGINT,
    url VARCHAR,
    user_agent VARCHAR,
    http_method VARCHAR,
    remote_ip VARCHAR,
    interaction_micros BIGINT,
    web_application_controller VARCHAR,
    web_application_action VARCHAR,
    web_application_context_type VARCHAR,
    web_application_context_id VARCHAR,
    real_user_id BIGINT,
    session_id VARCHAR,
    user_agent_id BIGINT,
    http_status VARCHAR,
    http_version VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3RequestsTermLocation %>/requests';

-- historical requests in gzip format
CREATE EXTERNAL TABLE <%= externalSchema %>.historical_requests(
    id VARCHAR,
    timestamp TIMESTAMP,
    timestamp_year VARCHAR,
    timestamp_month VARCHAR,
    timestamp_day VARCHAR,
    user_id BIGINT,
    course_id BIGINT,
    root_account_id BIGINT,
    course_account_id BIGINT,
    quiz_id BIGINT,
    discussion_id BIGINT,
    conversation_id BIGINT,
    assignment_id BIGINT,
    url VARCHAR,
    user_agent VARCHAR,
    http_method VARCHAR,
    remote_ip VARCHAR,
    interaction_micros BIGINT,
    web_application_controller VARCHAR,
    web_application_action VARCHAR,
    web_application_context_type VARCHAR,
    web_application_context_id VARCHAR,
    real_user_id BIGINT,
    session_id VARCHAR,
    user_agent_id BIGINT,
    http_status VARCHAR,
    http_version VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3RequestsHistoricalLocation %>/requests';

-- historical requests in parquet compression format
CREATE EXTERNAL TABLE <%= externalSchema %>.historical_requests_parquet(
    id VARCHAR,
    timestamp TIMESTAMP,
    timestamp_year VARCHAR,
    timestamp_month VARCHAR,
    timestamp_day VARCHAR,
    user_id BIGINT,
    course_id BIGINT,
    root_account_id BIGINT,
    course_account_id BIGINT,
    quiz_id BIGINT,
    discussion_id BIGINT,
    conversation_id BIGINT,
    assignment_id BIGINT,
    url VARCHAR,
    user_agent VARCHAR,
    http_method VARCHAR,
    remote_ip VARCHAR,
    interaction_micros BIGINT,
    web_application_controller VARCHAR,
    web_application_action VARCHAR,
    web_application_context_type VARCHAR,
    web_application_context_id VARCHAR,
    real_user_id BIGINT,
    session_id VARCHAR,
    user_agent_id BIGINT,
    http_status VARCHAR,
    http_version VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3RequestsHistoricalLocation %>/requests-parquet-snappy';

--------------------------------------------------------------------
-- (Pseudo) Materialized Views
--------------------------------------------------------------------

-- View used by BOAC
-- DROP SCHEMA <%= boacSchema %> CASCADE;

CREATE SCHEMA IF NOT EXISTS <%= boacSchema %>;

DROP TABLE IF EXISTS <%= boacSchema %>.berkeley_enrollments_view;
CREATE TABLE IF NOT EXISTS <%= boacSchema %>.berkeley_enrollments_view(
    year INTEGER NOT NULL,
    term VARCHAR(8) NOT NULL,
    canvas_course_id INTEGER NOT NULL,
    canvas_user_id INTEGER NOT NULL,
    course_name VARCHAR(255) NOT NULL,
    course_code VARCHAR(255),
    user_uid INTEGER NOT NULL,
    sis_user_id INTEGER,
    user_full_name VARCHAR(255) NOT NULL,
    canvas_page_views INTEGER,
    primary key(canvas_course_id, canvas_user_id)
);
