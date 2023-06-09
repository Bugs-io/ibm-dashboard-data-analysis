import re
import io
import pandas as pd
import difflib
from fastapi import status, HTTPException
import openpyxl
from pandas import DataFrame

from domain import Certification
from utils import CATEGORIES


def clean_dataset(df):
    df = df.iloc[1:]
    columns = ['uid', 'org', 'work_location', 'certification', 'issue_date', 'type']

    if columns != list(df.columns):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File not supported"
        )

    if df.shape[0] <= 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Data should have at least one row of data"
        )

    df = df.copy()

    df.loc[df.type == 'external certification', 'issue_date'] = df['certification'].str.split('(').str[::-1].str[0].str.split(')').str[0]
    df.loc[df.type == 'external certification', 'certification'] = df['certification'].str.rsplit('(', n=1).str[0].str.strip()
    df.loc[df.type == 'external certification', 'issue_date'] = df['issue_date'].str.split(' ').str[2] + '-' + df['issue_date'].str.split(' ').str[1] + '-' + df['issue_date'].str.split(' ').str[0]

    external_cert = df.loc[df.type == 'external certification']
    external_cert.loc[:, 'issue_date'] = pd.to_datetime(external_cert['issue_date'], format='%Y-%b-%d').dt.date
    df.loc[df.type == 'external certification', 'issue_date'] = external_cert['issue_date']

    df['work_location'] = df['work_location'].fillna('Not specified')

    return df


def get_most_attended_certifications(df, top=5, since_years=None):
    df = df.copy()

    if since_years is not None:
        df = filter_since_years(df, "issue_date", since_years)

    certification_attendance_count = df["certification"].value_counts()
    certification_attendance_count = certification_attendance_count.head(top)

    top_attended_certifications = []

    for name, attendees in certification_attendance_count.items():
        certification = Certification(name=name, total_attendees=attendees)
        top_attended_certifications.append(certification)

    return top_attended_certifications


def filter_since_years(df, date_column_name, since_years) -> pd.DataFrame:
    current_date = pd.to_datetime("today")
    start_date = current_date - pd.DateOffset(years=since_years)
    date_column = pd.to_datetime(df[date_column_name])

    return df.loc[date_column.between(start_date, current_date)]


def convert_df_to_csv_string(df):
    stream = io.StringIO()
    df.to_csv(stream, index=False)

    return stream.getvalue()


def read_excel_file(content):
    wb = openpyxl.load_workbook(io.BytesIO(content))
    ws = wb.active
    df = DataFrame(ws.values)

    for i, col in enumerate(df.iloc[0]):
        df = df.rename(columns={i: col})

    return df


SIMILARITY_THRESHOLD = 0.5


def calculate_similarity_score(certification: str, udemy_courses: list, threshold: float) -> (str, float):
    matches = difflib.get_close_matches(certification, udemy_courses, n=1, cutoff=threshold)
    if matches:
        similarity_score = difflib.SequenceMatcher(None, certification, matches[0]).ratio()
        return (matches[0], similarity_score)
    else:
        return ("", 0.0)


def calculate_similarity_scores(certifications: list, udemy_courses: list, threshold: float) -> pd.DataFrame:
    similarity_scores = []
    for certification in certifications:
        matched_course, similarity_score = calculate_similarity_score(
                certification,
                udemy_courses,
                threshold
                )

        if similarity_score >= threshold:
            similarity_scores.append((certification, matched_course, similarity_score))

    similarity_scores_df = pd.DataFrame(
            similarity_scores,
            columns=['ibm_certification', 'udemy_course', 'similarity_score']
            )

    return similarity_scores_df


def get_matched_certifications(certifications_df: pd.DataFrame, udemy_courses_df: pd.DataFrame):
    certifications = certifications_df["certification"]
    certifications = certifications.unique()
    certifications = certifications.tolist()

    udemy_courses = udemy_courses_df["title"]
    udemy_courses = udemy_courses.unique()
    udemy_courses = udemy_courses.tolist()

    similarity_scores_df = calculate_similarity_scores(
            certifications,
            udemy_courses,
            SIMILARITY_THRESHOLD
            )

    sorted_df = similarity_scores_df.sort_values(by='similarity_score', ascending=False)
    sorted_df = sorted_df.reset_index(drop=True)

    return sorted_df


def get_categories(certifications):
    results = {}
    for certification in certifications:
        matched = False
        for category, keywords in CATEGORIES.items():
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
    return certifications


def get_certifications_data(certifications, uid="IBM"):
    categories_data = get_categories(certifications)
    data = []
    for category in CATEGORIES:
        count = categories_data.get(category, 0)
        data.append({
            "uid": uid,
            "category": category,
            "certifications": count
        })
    return data


def get_certifications_distribution(certifications):
    categories = get_categories(certifications)
    data = []

    for name, count in categories.items():
        data.append({
            "uid": "IBM",
            "category": name,
            "certifications": count
        })

    return data
