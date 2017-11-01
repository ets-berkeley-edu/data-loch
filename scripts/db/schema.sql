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
  id INTEGER NOT NULL, -- e.g., value of Canvas course_id
  name VARCHAR(255) NOT NULL,
  code VARCHAR(255),

  PRIMARY KEY (api_domain, id),
  FOREIGN KEY (api_domain) REFERENCES context (api_domain) ON DELETE cascade
);

ALTER TABLE course OWNER TO data_processor;

--

CREATE TABLE person (
  api_domain VARCHAR(255) NOT NULL,
  uid INTEGER NOT NULL, -- e.g., LDAP
  name VARCHAR(255) NOT NULL,

  PRIMARY KEY (api_domain, uid),
  FOREIGN KEY (api_domain) REFERENCES context (api_domain) ON DELETE cascade
);

ALTER TABLE person OWNER TO data_processor;

--

CREATE TABLE enrollment (
  api_domain VARCHAR(255) NOT NULL,
  course_id INTEGER NOT NULL,
  uid INTEGER NOT NULL,
  sis_section_id INTEGER,
  year INTEGER NOT NULL,
  term VARCHAR(8) NOT NULL,

  PRIMARY KEY (api_domain, course_id, uid),
  FOREIGN KEY (api_domain, course_id) REFERENCES course (api_domain, id) ON DELETE cascade,
  FOREIGN KEY (api_domain, uid) REFERENCES person (api_domain, uid) ON DELETE cascade
);

ALTER TABLE enrollment OWNER TO data_processor;

--
-- Database creation is complete
--

END;
