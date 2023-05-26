from udemy_api import get_popular_courses

excluded_words = ['Exam', 'Intermediate', 'Guide', 'Project', 'Level', 'Functional', 'Content', 'Support', 'Professional', 'by', 'Streams', 'Framework', 'Portal', 'Language', '10', '2019', '2020', '2021', '2022', '2023', 'Programming', 'Application', '&', 'Using', 'Working', 'Environment', 'The', 'Way', 'Build', 'Part', 'a', 'an', 'and', 'of', 'the', 'to',
                  'with', 'With', 'Building', 'A', 'Applications', 'App', 'Market', 'Software', 'Advanced', 'Parallel', 'Coding', 'Web', '1', '2', '3', 'Learning', 'Introduction', 'Development', 'in', '-', 'Server', 'Studio', 'Practice', 'your', 'for', 'using', 'from', ' ', '', 'on', 'us', 'Visualization', 'Master', 'Performance']

def calculate_certification_counts(certifications):
    cert_match_count = {}

    udemy_courses = get_popular_courses()

    for ucourse in udemy_courses[0]:
        ucourse_words = ucourse.split(' ')
        for certification in certifications:
            certification_words = certification.split(' ')
            for word in certification_words:
                if word not in excluded_words and word in ucourse_words:
                    if certification in cert_match_count:
                        cert_match_count[certification] += 1
                    else:
                        cert_match_count[certification] = 1

    return cert_match_count
