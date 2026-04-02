"""공사명칭 검토기 — FastAPI 서버"""
import os, shutil, tempfile, time, base64
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from engine import ReviewEngine, NameMatcher, load_master_names, save_excel_report, generate_highlight_snapshots

app = FastAPI(title="공사명칭 검토기", version="1.0")

from fastapi.responses import FileResponse

@app.get("/app_icon.ico")
async def favicon_ico():
    return FileResponse("/opt/name-checker/app_icon.ico", media_type="image/x-icon")

@app.get("/app_icon.png")
async def favicon_png():
    return FileResponse("/opt/name-checker/app_icon.png", media_type="image/png")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

raw = load_master_names()
if isinstance(raw, tuple):
    master_list = raw[0]
else:
    master_list = raw

matcher = NameMatcher(master_list)
engine = ReviewEngine(matcher)

ALLOWED_EXT = {".pdf", ".xlsx", ".xls", ".docx", ".hwp", ".hwpx", ".csv"}

@app.get("/")
async def root():
    return FileResponse("static/index.html")

@app.get("/api/health")
async def health():
    return {"status": "ok", "master_count": len(master_list)}

@app.post("/api/review")
async def review_file(file: UploadFile = File(...)):
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_EXT:
        raise HTTPException(400, f"지원하지 않는 형식: {ext}")

    with tempfile.NamedTemporaryFile(delete=False, suffix=ext, dir="/tmp") as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    try:
        start = time.time()
        result = engine.review_file(tmp_path)
        result["elapsed"] = round(time.time() - start, 1)
        result["filename"] = file.filename

        snapshots_b64 = []
        if ext == ".pdf":
            ng_items = [d for d in result.get("details", []) if d.get("status") == "불일치"]
            if ng_items:
                try:
                    snapshots = generate_highlight_snapshots(tmp_path, ng_items, all_details=result.get("details", []))
                    for page_num, png_bytes in snapshots:
                        snapshots_b64.append({
                            "page": page_num,
                            "image": base64.b64encode(png_bytes).decode("ascii")
                        })
                except Exception:
                    pass
        result["snapshots"] = snapshots_b64

        return JSONResponse(result)
    finally:
        os.unlink(tmp_path)

@app.post("/api/review/excel")
async def review_to_excel(file: UploadFile = File(...)):
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_EXT:
        raise HTTPException(400, f"지원하지 않는 형식: {ext}")

    with tempfile.NamedTemporaryFile(delete=False, suffix=ext, dir="/tmp") as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    report_path = tmp_path + "_report.xlsx"
    try:
        result = engine.review_file(tmp_path)
        save_excel_report(result, report_path)
        return FileResponse(
            report_path,
            filename=f"검토결과_{file.filename}.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    finally:
        os.unlink(tmp_path)
        if os.path.exists(report_path):
            os.unlink(report_path)

os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
