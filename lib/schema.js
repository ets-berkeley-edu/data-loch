/**
 * Copyright ©2017. The Regents of the University of California (Regents). All Rights Reserved.
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

module.exports = {
  users: [
    {
      name: 'user_dim',
      mapping: [
        {id: true},
        {canvas_id: true},
        {root_account_id: false},
        {name: true},
        {time_zone: false},
        {created_at: true},
        {visibility: false},
        {school_name: false},
        {school_position: false},
        {gender: false},
        {locale: false},
        {public: false},
        {birthdate: false},
        {country_code: false},
        {workflow_state: false}
      ]
    },
    {
      name: 'pseudonym_dim',
      mapping: [
        {pseudonym_id: true},
        {pseudonym_canvas_id: false},
        {id: false},
        {pseudonym_account_id: false},
        {pseudonym_workflow_state: false},
        {last_request_at: false},
        {last_login_at: false},
        {current_login_at: false},
        {last_login_ip: false},
        {current_login_ip: false},
        {pseudonym_position: false},
        {created_at: false},
        {updated_at: true},
        {password_auto_generated: false},
        {deleted_at: false},
        {sis_user_id: true},
        {unique_name: false}
      ]
    }
  ],
  courses: [
    {
      name: 'course_dim',
      mapping: [
        {id: true},
        {canvas_id: true},
        {root_account_id: false},
        {account_id: false},
        {enrollment_term_id: true},
        {name: true},
        {code: true},
        {type: false},
        {created_at: true},
        {start_at: true},
        {conclude_at: true},
        {publicly_visible: false},
        {sis_source_id: true},
        {workflow_state: false},
        {wiki_id: false}
      ]
    }
  ],
  course_sections: [
    {
      name: 'course_section_dim',
      mapping: [
        {id: true},
        {canvas_id: true},
        {name: true},
        {course_id: true},
        {enrollment_term_id: true},
        {default_section: false},
        {accepting_enrollments: false},
        {can_manually_enroll: false},
        {start_at: true},
        {end_at: true},
        {created_at: true},
        {updated_at: false},
        {workflow_state: false},
        {restrict_enrollments_to_section_dates: false},
        {nonxlist_course_id: false},
        {sis_source_id: true}
      ]
    }
  ],
  enrollments: [
    {
      name: 'enrollment_dim',
      mapping: [
        {id: true},
        {canvas_id: true},
        {root_account_id: false},
        {course_section_id: true},
        {role_id: true},
        {type: true},
        {workflow_state: false},
        {created_at: true},
        {updated_at: false},
        {start_at: true},
        {end_at: true},
        {completed_at: true},
        {self_enrolled: false},
        {sis_source_id: true},
        {course_id: true},
        {user_id: true}
      ]
    },
    {
      name: 'enrollment_fact',
      mapping: [
        {id: false},
        {user_id: false},
        {course_id: false},
        {enrollment_term_id: true},
        {course_account_id: false},
        {course_section_id: false},
        {computed_final_score: true},
        {computed_current_score: true}
      ]
    }
  ],
  discussion_topics: [
    {
      name: 'discussion_topic_dim',
      mapping: [
        {id: true},
        {canvas_id: true},
        {title: true},
        {message: true},
        {type: true},
        {workflow_state: false},
        {last_reply_at: false},
        {created_at: true},
        {updated_at: false},
        {delayed_post_at: false},
        {posted_at: false},
        {deleted_at: false},
        {discussion_type: true},
        {pinned: false},
        {locked: false}
      ]
    },
    {
      name: 'discussion_topic_fact',
      mapping: [
        {id: false},
        {course_id: true},
        {enrollment_term_id: true},
        {course_account_id: false},
        {user_id: true},
        {assignment_id: true},
        {editor_id: false},
        {enrollment_rollup_id: false},
        {message_length: false}
      ]
    }
  ],
  discussion_entries: [
    {
      name: 'discussion_entry_dim',
      mapping: [
        {id: true},
        {canvas_id: true},
        {message: true},
        {workflow_state: false},
        {created_at: true},
        {updated_at: false},
        {deleted_at: false},
        {depth: false}
      ]
    },
    {
      name: 'discussion_entry_fact',
      mapping: [
        {id: false},
        {parent_discussion_entry_id: true},
        {user_id: true},
        {topic_id: true},
        {course_id: true},
        {enrollment_term_id: true},
        {course_account_id: false},
        {topic_user_id: false},
        {topic_assignment_id: false},
        {topic_editor_id: false},
        {enrollment_rollup_id: false},
        {message_length: false}
      ]
    }
  ],
  assignments: [
    {
      name: 'assignment_dim',
      mapping: [
        {id: true},
        {canvas_id: true},
        {course_id: true},
        {title: true},
        {description: true},
        {due_at: true},
        {unlock_at: false},
        {lock_at: false},
        {points_possible: true},
        {grading_type: true},
        {submission_types: true},
        {workflow_state: false},
        {created_at: true},
        {updated_at: false},
        {peer_review_count: false},
        {peer_reviews_due_at: false},
        {peer_reviews_assigned: false},
        {peer_reviews: true},
        {automatic_peer_reviews: false},
        {all_day: false},
        {all_day_date: false},
        {could_be_locked: false},
        {grade_group_students_individually: false},
        {anonymous_peer_reviews: false},
        {muted: false}
      ]
    },
    {
      name: 'assignment_fact',
      mapping: [
        {id: false},
        {course_id: false},
        {course_account_id: false},
        {enrollment_term_id: true}
      ]
    }
  ],
  submissions: [
    {
      name: 'submission_dim',
      mapping: [
        {id: true},
        {canvas_id: true},
        {body: true},
        {url: true},
        {grade: true},
        {submitted_at: true},
        {submission_type: true},
        {workflow_state: false},
        {created_at: true},
        {updated_at: false},
        {processed: false},
        {process_attempts: false},
        {grade_matches_current_submission: false},
        {published_grade: false},
        {graded_at: true},
        {has_rubric_assessment: true},
        {attempt: false},
        {has_admin_comment: false},
        {assignment_id: true}
      ]
    },
    {
      name: 'submission_fact',
      mapping: [
        {id: false},
        {assignment_id: false},
        {course_id: true},
        {enrollment_term_id: true},
        {user_id: true},
        {grader_id: true},
        {course_account_id: false},
        {enrollment_rollup_id: false},
        {score: true},
        {published_score: true},
        {what_if_score: false},
        {submission_comments_count: false}
      ]
    }
  ],
  submission_comments: [
    {
      name: 'submission_comment_dim',
      mapping: [
        {id: true},
        {canvas_id: true},
        {submission_id: true},
        {recipient_id: true},
        {author_id: true},
        {assessment_request_id: false},
        {group_comment_id: false},
        {comment: true},
        {author_name: false},
        {created_at: true},
        {updated_at: false},
        {anonymous: false},
        {teacher_only_comment: false},
        {hidden: false}
      ]
    },
    {
      name: 'submission_comment_fact',
      mapping: [
        {id: false},
        {submission_id: false},
        {recipient_id: false},
        {author_id: false},
        {assignment_id: true},
        {course_id: true},
        {enrollment_term_id: false},
        {course_account_id: false},
        {message_size_bytes: false},
        {message_character_count: false},
        {message_word_count: false},
        {message_line_count: false}
      ]
    },
    {
      name: 'submission_comment_participant_fact',
      mapping: [{commenter_id: false}, {id: false}, {commenter_user_id: true}]
    }
  ],
  requests: [
    {
      name: 'requests',
      mapping: [
        {id: true},
        {timestamp: true},
        {timestamp_year: true},
        {timestamp_month: true},
        {timestamp_day: true},
        {user_id: true},
        {course_id: true},
        {root_account_id: true},
        {course_account_id: true},
        {quiz_id: true},
        {discussion_id: true},
        {conversation_id: true},
        {assignment_id: true},
        {url: true},
        {user_agent: true},
        {http_method: true},
        {remote_ip: true},
        {interaction_micros: true},
        {web_application_controller: true},
        {web_application_action: true},
        {web_application_context_type: true},
        {web_application_context_id: true}
      ]
    }
  ]
};
