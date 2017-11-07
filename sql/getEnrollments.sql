SELECT
  DISTINCT(p.unique_name, c.canvas_id),
  p.unique_name AS uid,
  p.sis_user_id,
  u.name person_name,
  c.canvas_id AS canvas_course_id,
  c.name AS course_name,
  c.sis_source_id as course_code
FROM user_dim u
  JOIN enrollment_fact e ON e.user_id = u.id
  JOIN course_dim c ON c.id = e.course_id
  JOIN pseudonym_dim p ON p.user_id = u.id
WHERE
  e.enrollment_term_id = :enrollmentTermId
  AND
  p.unique_name IN (:uids)
ORDER BY
  p.unique_name,
  c.canvas_id
