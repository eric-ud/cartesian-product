from typing import Union

from fastapi import FastAPI, Request, File, UploadFile, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

import uvicorn

import pandas as pd
import numpy as np
import openpyxl
import io
from functools import reduce
from datetime import datetime

app = FastAPI()

templates = Jinja2Templates("src/templates")


@app.get("/", response_class=HTMLResponse)
def read_root(request: Request) -> object:
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/uploader")
def create_upload_file(
    request: Request, excel: UploadFile = File(...)
) -> StreamingResponse:
    client_host = request.client.host

    wb = io.BytesIO(excel.file.read())

    openpyxl_reader = openpyxl.load_workbook(wb)
    if np.prod([ws.max_row for ws in openpyxl_reader.worksheets]) > 3_000_000:
        raise HTTPException(
            status_code=422,
            detail="Слишком много строк. Максимум 3 миллиона после перемножения.",
        )

    with pd.ExcelFile(wb, engine="openpyxl") as reader:
        if len(reader.sheet_names) < 2:
            raise HTTPException(
                status_code=422,
                detail="Слишком мало листов. Требуется минимум 2 листа.",
            )
        sheets = [pd.read_excel(reader, n, decimal=",") for n in reader.sheet_names]

    result_df = reduce(lambda x, y: x.merge(y, how="cross"), sheets)

    def chunker(seq: pd.DataFrame, size: int) -> pd.DataFrame:
        for pos in range(0, len(seq), size):
            yield seq.iloc[pos : pos + size]

    headers = {
        "Content-Disposition": f'attachment; filename="{client_host}@{datetime.now().strftime("%Y-%m-%d_%H%M%S")}.xlsx"'
    }

    output_xlsx = io.BytesIO()
    with pd.ExcelWriter(output_xlsx) as writer:
        for index, chunk in enumerate(chunker(result_df, 1_000_000)):
            chunk.to_excel(writer, sheet_name=str(index).zfill(2))
    output_xlsx.seek(0)
    return StreamingResponse(output_xlsx, headers=headers)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0")  # reload=True,
