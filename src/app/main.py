from fastapi import FastAPI

app = FastAPI(
    title="Green Earth API",
    description="An API server for handling bluesky content recommendation requests",
    version="0.1.0"
)


@app.get("/")
async def root():
    return {"message": "Green Earth API"}
