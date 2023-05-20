import io
import pandas as pd
from fastapi import status, HTTPException
import openpyxl
from pandas import DataFrame

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
