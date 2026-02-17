from fastapi import FastAPI, Response

app = FastAPI(title="TWF V3 Tile Server", version="0.0.0")

@app.get("/tiles/v3/health")
def health():
    return {"ok": True}

@app.get("/tiles/v3/{path:path}")
def catchall(path: str):
    return Response(status_code=404)
