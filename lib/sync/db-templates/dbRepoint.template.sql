-- Repoints External Tables to new S3 location

-- Users
ALTER TABLE <%= externalSchema %>.user_dim
  SET LOCATION '<%= s3DailyLocation %>/user_dim';

-- Pseudonym
ALTER TABLE <%= externalSchema %>.pseudonym_dim
  SET LOCATION '<%= s3DailyLocation %>/pseudonym_dim';

-- Courses
ALTER TABLE <%= externalSchema %>.course_dim
  SET LOCATION '<%= s3DailyLocation %>/course_dim';

-- Course Section Dimensions
ALTER TABLE <%= externalSchema %>.course_section_dim
  SET LOCATION '<%= s3DailyLocation %>/course_section_dim';

-- enrollment_fact;
ALTER TABLE <%= externalSchema %>.enrollment_fact
  SET LOCATION '<%= s3DailyLocation %>/enrollment_fact';

-- enrollment_dim;
ALTER TABLE <%= externalSchema %>.enrollment_dim
  SET LOCATION '<%= s3DailyLocation %>/enrollment_dim';

-- Assignments Fact
ALTER TABLE <%= externalSchema %>.assignment_fact
    SET LOCATION '<%= s3DailyLocation %>/assignment_fact';

-- Assignment Dimension table;
ALTER TABLE <%= externalSchema %>.assignment_dim
  SET LOCATION '<%= s3DailyLocation %>/assignment_dim';

-- Discussion Entry dimension
ALTER TABLE  <%= externalSchema %>.discussion_entry_dim
  SET LOCATION '<%= s3DailyLocation %>/discussion_entry_dim';

-- Discussion entry fact;
ALTER TABLE <%= externalSchema %>.discussion_entry_fact
  SET LOCATION '<%= s3DailyLocation %>/discussion_entry_fact';

-- Discussion topic dim;
ALTER TABLE <%= externalSchema %>.discussion_topic_dim
  SET LOCATION '<%= s3DailyLocation %>/discussion_topic_dim';

-- Discussion topic fact;
ALTER TABLE <%= externalSchema %>.discussion_topic_fact
  SET LOCATION '<%= s3DailyLocation %>/discussion_topic_fact';

-- Submission_fact;
ALTER TABLE <%= externalSchema %>.submission_fact
  SET LOCATION '<%= s3DailyLocation %>/submission_fact';

-- Submission_dim;
ALTER TABLE <%= externalSchema %>.submission_dim
  SET LOCATION '<%= s3DailyLocation %>/submission_dim';

-- Submissions comments fact
ALTER TABLE <%= externalSchema %>.submission_comment_fact
  SET LOCATION '<%= s3DailyLocation %>/submission_comment_fact';

-- Submission_comment_participant_fact;
ALTER TABLE <%= externalSchema %>.submission_comment_participant_fact
  SET LOCATION '<%= s3DailyLocation %>/submission_comment_participant_fact';

-- Submission_comment_dim;
 ALTER TABLE <%= externalSchema %>.submission_comment_dim
  SET LOCATION '<%= s3DailyLocation %>/submission_comment_dim';

-- Requests
ALTER TABLE <%= externalSchema %>.requests
  SET LOCATION '<%= s3RequestsTermLocation %>/requests';
