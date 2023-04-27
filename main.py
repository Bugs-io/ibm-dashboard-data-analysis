import openpyxl
from fastapi.exceptions import HTTPException
import magic
from fastapi import FastAPI, UploadFile, status, HTTPException
import io
import pandas as pd
from pandas import DataFrame
import openpyxl
from fastapi.responses import StreamingResponse

app = FastAPI()

EXCEL_MIME_TYPE = [
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ]

@app.post("/clean-internal-dataset")
async def upload(file: UploadFile | None = None):
    if not file:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file was uploaded"
        )

    content = await file.read()

    file_type = magic.from_buffer(content, mime=True)

    if file_type not in EXCEL_MIME_TYPE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File type is not supported: {file_type}"
        )
    

    wb = openpyxl.load_workbook(io.BytesIO(content))
    ws = wb.active
    df = DataFrame(ws.values)


    for i, col in enumerate(df.iloc[0]):
        df = df.rename(columns={i: col})
    df = df.iloc[1:]

        
    df.loc[df.type == 'external certification', 'issue_date'] = df['certification'].str.split(
    '(').str[::-1].str[0].str.split(')').str[0]
    
    df.loc[df.type == 'external certification', 'certification'] = df['certification'].str.rsplit('(', n=1).str[0].str.strip()

    df.loc[df.type == 'external certification', 'issue_date'] = df['issue_date'].str.split(
    ' ').str[2] + '-' + df['issue_date'].str.split(' ').str[1] + '-' + df['issue_date'].str.split(' ').str[0]


    external_cert = df.loc[df.type == 'external certification']

    external_cert.loc[:, 'issue_date'] = pd.to_datetime(external_cert['issue_date'], format='%Y-%b-%d').dt.date

    df.loc[df.type == 'external certification', 'issue_date'] = external_cert['issue_date']


    df['work_location'] = df['work_location'].fillna('Not specified')

    stream = io.StringIO()
    df.to_csv(stream, index=False)


    response = StreamingResponse(
        iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    
    return response
