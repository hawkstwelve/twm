from fastapi import FastAPI

app = FastAPI(title="TWF V3 API", version="0.0.0")

@app.get("/api/v3/health")
def health():
    return {"ok": True}

@app.get("/api/v3")
def root():
    return {"service": "twf-v3-api", "status": "stub"}
