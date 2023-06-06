import io
import pandas as pd
from fastapi import status, HTTPException
import openpyxl
from pandas import DataFrame

from domain import Certification


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
