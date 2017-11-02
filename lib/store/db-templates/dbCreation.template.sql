--------------------------------------------------------------------
-- DROP and CREATE
--------------------------------------------------------------------

DROP SCHEMA <%= externalDatabase %>;

CREATE EXTERNAL SCHEMA <%= externalDatabase %>
FROM data catalog
DATABASE '<%= externalDatabase %>'
IAM_ROLE '<%= iamRole %>'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- user_dim
DROP TABLE <%= externalDatabase %>.user_dim;

CREATE EXTERNAL TABLE <%= externalDatabase %>.user_dim (
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
LOCATION '<%= s3Location %>/user_dim';

-- pseudonym_dim
DROP TABLE <%= externalDatabase %>.pseudonym_dim;

CREATE EXTERNAL TABLE <%= externalDatabase %>.pseudonym_dim (
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
LOCATION '<%= s3Location %>/pseudonym_dim';

-- course_dim
DROP TABLE <%= externalDatabase %>.course_dim;

CREATE EXTERNAL TABLE <%= externalDatabase %>.course_dim (
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
LOCATION '<%= s3Location %>/course_dim';

-- course_section_dim
DROP TABLE <%= externalDatabase %>.course_section_dim;

CREATE EXTERNAL TABLE <%= externalDatabase %>.course_section_dim (
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
LOCATION '<%= s3Location %>/course_section_dim';

-- enrollment_fact
DROP TABLE <%= externalDatabase %>.enrollment_fact;

CREATE EXTERNAL TABLE <%= externalDatabase %>.enrollment_fact(
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
LOCATION '<%= s3Location %>/enrollment_fact';

-- enrollment_dim
DROP TABLE <%= externalDatabase %>.enrollment_dim;

CREATE EXTERNAL TABLE <%= externalDatabase %>.enrollment_dim(
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
LOCATION '<%= s3Location %>/enrollment_dim';

-- assignment_fact
DROP TABLE <%= externalDatabase %>.assignment_fact;

CREATE EXTERNAL TABLE <%= externalDatabase %>.assignment_fact(
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
LOCATION '<%= s3Location %>/assignment_fact';

-- assignment_dim
DROP TABLE <%= externalDatabase %>.assignment_dim;

CREATE EXTERNAL TABLE <%= externalDatabase %>.assignment_dim(
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
LOCATION '<%= s3Location %>/assignment_dim';

-- discussion_entry_dim
DROP TABLE <%= externalDatabase %>.discussion_entry_dim;

CREATE EXTERNAL TABLE <%= externalDatabase %>.discussion_entry_dim(
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
LOCATION '<%= s3Location %>/discussion_entry_dim';

-- discussion_entry_fact
DROP TABLE <%= externalDatabase %>.discussion_entry_fact;

CREATE EXTERNAL TABLE <%= externalDatabase %>.discussion_entry_fact(
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
LOCATION '<%= s3Location %>/discussion_entry_fact';

-- discussion_topic_dim
DROP TABLE <%= externalDatabase %>.discussion_topic_dim;

CREATE EXTERNAL TABLE <%= externalDatabase %>.discussion_topic_dim(
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
LOCATION '<%= s3Location %>/discussion_topic_dim';

-- discussion_topic_fact
DROP TABLE <%= externalDatabase %>.discussion_topic_fact;

CREATE EXTERNAL TABLE <%= externalDatabase %>.discussion_topic_fact(
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
LOCATION '<%= s3Location %>/discussion_topic_fact';

-- submission_fact
DROP TABLE <%= externalDatabase %>.submission_fact;

CREATE EXTERNAL TABLE <%= externalDatabase %>.submission_fact(
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
LOCATION '<%= s3Location %>/submission_fact';

-- submission_dim
DROP TABLE <%= externalDatabase %>.submission_dim;

CREATE EXTERNAL TABLE <%= externalDatabase %>.submission_dim(
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
LOCATION '<%= s3Location %>/submission_dim';

-- submission_comment_fact
DROP TABLE <%= externalDatabase %>.submission_comment_fact;

CREATE EXTERNAL TABLE <%= externalDatabase %>.submission_comment_fact(
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
LOCATION '<%= s3Location %>/submission_comment_fact';

-- submission_comment_participant_fact
DROP TABLE <%= externalDatabase %>.submission_comment_participant_fact;

CREATE EXTERNAL TABLE <%= externalDatabase %>.submission_comment_participant_fact(
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
LOCATION '<%= s3Location %>/submission_comment_participant_fact';

-- submission_comment_dim
DROP TABLE <%= externalDatabase %>.submission_comment_dim;

CREATE EXTERNAL TABLE <%= externalDatabase %>.submission_comment_dim(
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
LOCATION '<%= s3Location %>/submission_comment_dim';

-- requests
DROP TABLE <%= externalDatabase %>.requests;

CREATE EXTERNAL TABLE <%= externalDatabase %>.requests(
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
  web_applicaiton_action VARCHAR,
  web_application_conVARCHAR_type VARCHAR,
  web_application_conVARCHAR_id VARCHAR,
  real_user_id BIGINT,
  session_id VARCHAR,
  user_agent_id BIGINT,
  http_status VARCHAR,
  http_version VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '<%= s3Location %>/requests';

--------------------------------------------------------------------
-- (Pseudo) Materialized Views
--------------------------------------------------------------------

-- View used by BOAC
DROP TABLE berkeley_enrollments_view;

CREATE TABLE berkeley_enrollments_view(
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
