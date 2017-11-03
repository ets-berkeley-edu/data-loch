-- Repoints External Tables to new S3 location

-- Users
ALTER TABLE <%= externalDatabase %>.user_dim
  SET LOCATION '<%= s3Location %>/user_dim';

-- Pseudonym
ALTER TABLE <%= externalDatabase %>.pseudonym_dim
  SET LOCATION '<%= s3Location %>/pseudonym_dim';

-- Courses
ALTER TABLE <%= externalDatabase %>.course_dim
  SET LOCATION '<%= s3Location %>/course_dim';

-- Course Section Dimensions
ALTER TABLE <%= externalDatabase %>.course_section_dim
  SET LOCATION '<%= s3Location %>/course_section_dim';

-- enrollment_fact;
ALTER TABLE <%= externalDatabase %>.enrollment_fact
  SET LOCATION '<%= s3Location %>/enrollment_fact';

-- enrollment_dim;
ALTER TABLE <%= externalDatabase %>.enrollment_dim
  SET LOCATION '<%= s3Location %>/enrollment_dim';

-- Assignments Fact
ALTER TABLE <%= externalDatabase %>.assignment_fact
    SET LOCATION '<%= s3Location %>/assignment_fact';

-- Assignment Dimension table;
ALTER TABLE <%= externalDatabase %>.assignment_dim
  SET LOCATION '<%= s3Location %>/assignment_dim';

-- Discussion Entry dimension
ALTER TABLE  <%= externalDatabase %>.discussion_entry_dim
  SET LOCATION '<%= s3Location %>/discussion_entry_dim';

-- Discussion entry fact;
ALTER TABLE <%= externalDatabase %>.discussion_entry_fact
  SET LOCATION '<%= s3Location %>/discussion_entry_fact';

-- Discussion topic dim;
ALTER TABLE <%= externalDatabase %>.discussion_topic_dim
  SET LOCATION '<%= s3Location %>/discussion_topic_dim';

-- Discussion topic fact;
ALTER TABLE <%= externalDatabase %>.discussion_topic_fact
  SET LOCATION '<%= s3Location %>/discussion_topic_fact';

-- Submission_fact;
ALTER TABLE <%= externalDatabase %>.submission_fact
  SET LOCATION '<%= s3Location %>/submission_fact';

-- Submission_dim;
ALTER TABLE <%= externalDatabase %>.submission_dim
  SET LOCATION '<%= s3Location %>/submission_dim';

-- Submissions comments fact
ALTER TABLE <%= externalDatabase %>.submission_comment_fact
  SET LOCATION '<%= s3Location %>/submission_comment_fact';

-- Submission_comment_participant_fact;
ALTER TABLE <%= externalDatabase %>.submission_comment_participant_fact
  SET LOCATION '<%= s3Location %>/submission_comment_participant_fact';

-- Submission_comment_dim;
 ALTER TABLE <%= externalDatabase %>.submission_comment_dim
  SET LOCATION '<%= s3Location %>/submission_comment_dim';

-- Requests
ALTER TABLE <%= externalDatabase %>.requests
  SET LOCATION '<%= s3Location %>/requests';
