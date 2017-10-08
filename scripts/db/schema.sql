\set ON_ERROR_ROLLBACK on

BEGIN;

--
-- Set up database
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--

CREATE TYPE context_type AS ENUM ('canvas'); -- e.g., ('canvas', 'turnitin')

ALTER TYPE context_type OWNER TO data_processor;

--

CREATE TABLE context (
  api_domain VARCHAR(255) PRIMARY KEY, -- e.g., 'bcourses.berkeley.edu'
  type context_type NOT NULL
);

ALTER TABLE context OWNER TO data_processor;

--

CREATE TABLE course (
  api_domain VARCHAR(255) NOT NULL,
  course_id INTEGER NOT NULL, -- e.g., value of Canvas course_id
  sis_course_id INTEGER,
  name VARCHAR(255) NOT NULL,
  short_name VARCHAR(255),

  PRIMARY KEY (api_domain, course_id),
  FOREIGN KEY (api_domain) REFERENCES context (api_domain) ON DELETE cascade
);

ALTER TABLE course OWNER TO data_processor;

--

CREATE TABLE person (
  api_domain VARCHAR(255) NOT NULL,
  context_user_id INTEGER NOT NULL, -- e.g., value of Canvas user_id
  sis_user_id INTEGER, -- e.g., student_id

  PRIMARY KEY (api_domain, context_user_id),
  FOREIGN KEY (api_domain) REFERENCES context (api_domain) ON DELETE cascade
);

ALTER TABLE person OWNER TO data_processor;

--

CREATE TABLE course_students (
  api_domain VARCHAR(255) NOT NULL,
  course_id INTEGER NOT NULL,
  context_user_id INTEGER NOT NULL,
  page_views INTEGER NOT NULL,
  participations INTEGER NOT NULL,
  tardiness_on_time INTEGER,

  PRIMARY KEY (api_domain, course_id, context_user_id),
  FOREIGN KEY (api_domain, course_id) REFERENCES course (api_domain, course_id) ON DELETE cascade,
  FOREIGN KEY (api_domain, context_user_id) REFERENCES person (api_domain, context_user_id) ON DELETE cascade
);

ALTER TABLE course_students OWNER TO data_processor;

--
-- Database creation is complete
--

END;
