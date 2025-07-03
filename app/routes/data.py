from fastapi import APIRouter, HTTPException, Request, status
from app.services import dataservice

router = APIRouter()


@router.get("/get-data")
async def get_data(request: Request):
    get_params = dict(request.query_params)
    x_columns = None
    y_columns = None
    if get_params['x-axis'] == 'Bp':    
        x_columns = dataservice.extract_columns(get_params['x-axis'])
    if get_params['y-axis'] == 'Time':
        y_columns = dataservice.get_time()
    return {
        "x_columns": x_columns,
        "y_columns": y_columns,
        "message": "Data retrieved successfully"
    }

@router.get("/get-col-names")
async def get_col_names():
    columns = dataservice.extract_col_names()
    return {
        "columns": columns,
        "message": "Column names retrieved successfully"
    }