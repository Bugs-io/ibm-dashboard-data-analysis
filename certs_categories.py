import re
from fastapi import status
from fastapi.exceptions import HTTPException

categories = {
    "Software Development": ["developer", "db", "dba", "containers", "database", "databases", "koder", "testing", "tester", "development", "microservices", "blockchain", "software development", "web development", "mobile development", "user experience", "ui", "ux", "software engineering", "linux", "software architecture", "agile development", "version control", "software testing", "html", "django", "javascript", "node.js", "sun", "react", "angular", "java", "api"],
    "Data Analytics": ["analytics", "pytorch", "neural network", "neural networks", "process mining", "watson", "tensorflow", "automation", "data science", "ai", "big data", "cognos", "data analysis", "data mining", "data visualization", "statistics", "machine learning", "artificial intelligence", "predictive modeling", "data", "analysis", "r", "deep learning"],
    "Project Management": ["pmp", "mentor", "itil", "executive", "scrum", "project planning", "project control", "project management", "time management", "risk management", "resource management", "agile methodologies", "project lifecycle", "agile", "stakeholder management", "scrum Master", "product owner", "project manager"],
    "Cybersecurity": ["hacker", "hack", "cybersecurity", "network", "security", "cryptography", "data protection", "security auditing", "incident response", "ethical hacking", "information security", "penetration testing"],
    "Cloud Computing": ["aws", "azure", "cloud computing", "cloudant", "infrastructure as a service", "virtualization", "platform as a service", "cloud architecture", "cloud", "cloud deployment", "kubernetes", "devops"],
    "Mainframe and Systems": ["mainframe", "systems administrator", "systems administration", "performance optimization", "legacy systems", "server virtualization", "system architecture", "system maintenance", "troubleshooting"]
}

def get_categories(certifications):
    results = {}
    for certification in certifications:
        matched = False
        for category, keywords in categories.items():
            pattern = r"\b(" + "|".join(keywords) + r")\b"
            if re.search(pattern, certification, flags=re.IGNORECASE):
                if category not in results:
                    results[category] = 1
                else:
                    results[category] += 1
                matched = True
                break
        if not matched:
            if "Other" not in results:
                results["Other"] = 1
            else:
                results["Other"] += 1
    return results

def get_certifications(data, uid):
    certifications = []
    for index, row in data.iterrows():
        if row['uid'] == uid:
            certifications.append(row['certification'])
    if len(certifications) == 0:
        raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid uid"
            )
            
    return certifications

def get_certifications_data(certifications, uid):
    categories_data = get_categories(certifications)
    data = []
    for category in categories:
        count = categories_data.get(category, 0)
        data.append({
            "uid": uid,
            "category": category,
            "certifications": count
        })
    return data