from fastapi import FastAPI
from pydantic import BaseModel, validator
from app.model.model import predict_volume
from app.model.model import __version__ as model_version
from typing import Tuple

app = FastAPI()

class InputModel(BaseModel):
    input: Tuple[float, float]

    @validator('input')
    def validate_my_tuple(cls, value):
        if len(value) != 2:
            raise ValueError('Input should contain exactly 2 elements')
        return value

    @validator('my_tuple', each_item=True)
    def validate_tuple_elements(cls, value):
        if not isinstance(value, float):
            raise ValueError('Input elements should be of type float')
        return value

class Volume(BaseModel):
    volume: float


@app.get("/")
def home():
   return {"health_cherk": "OK", "model_vesion": model_version}


@app.post("/precdict", response_model=Volume)
def predict(payload: InputModel):
    volume= predict_volume(payload.txt)
    return {"volume": volume}

