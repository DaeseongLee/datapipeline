from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/hello")
def hello():
    return {"Hello": 'Lee'}

@app.get("/hello2")
def hello2():
    return {"Hello": 'Daeseong'}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str | None = None):
    return {"item_id": item_id, "q": q}
