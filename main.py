import os
import uuid
import logging
import tempfile
from pathlib import Path

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# pdf2docx is the best Python library for PDF → DOCX conversion
from pdf2docx import Converter

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pdf-python-service")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="PDF → DOCX Microservice",
    description="Converts PDF files to Word (.docx) documents using pdf2docx.",
    version="1.0.0",
)

# Allow requests from NestJS and React (local + production)
_ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5173",
    "http://localhost:3001",
]
# Add production URLs from environment variable (comma-separated)
_extra = os.environ.get("ALLOWED_ORIGINS", "")
if _extra:
    _ALLOWED_ORIGINS.extend([o.strip() for o in _extra.split(",") if o.strip()])

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    """Simple liveness probe."""
    return {"status": "ok", "service": "pdf-python-service"}


# ---------------------------------------------------------------------------
# Main conversion endpoint
# ---------------------------------------------------------------------------
@app.post("/convert-pdf")
async def convert_pdf(file: UploadFile = File(...)):
    """
    Accept a PDF file (multipart/form-data, field name = 'file').
    Returns the converted .docx file as a binary download.

    NestJS sends the PDF here via an internal HTTP POST.
    """
    # ── Validate input ──────────────────────────────────────────────────────
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided.")

    original_name = Path(file.filename).stem  # filename without extension
    content_type = file.content_type or ""

    if not file.filename.lower().endswith(".pdf") and "pdf" not in content_type:
        raise HTTPException(
            status_code=400,
            detail="Only PDF files are accepted. Received: " + file.filename,
        )

    # ── Write upload to a temp PDF file ─────────────────────────────────────
    pdf_bytes = await file.read()
    if len(pdf_bytes) == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    tmp_dir = Path(tempfile.mkdtemp(prefix="pdf_svc_"))
    pdf_path  = tmp_dir / f"{uuid.uuid4().hex}.pdf"
    docx_path = tmp_dir / f"{original_name}.docx"

    try:
        pdf_path.write_bytes(pdf_bytes)
        logger.info(
            "Converting %s (%d bytes) → %s",
            file.filename,
            len(pdf_bytes),
            docx_path.name,
        )

        # ── pdf2docx conversion ──────────────────────────────────────────────
        cv = Converter(str(pdf_path))
        cv.convert(str(docx_path), start=0, end=None)
        cv.close()

        if not docx_path.exists() or docx_path.stat().st_size == 0:
            raise RuntimeError("Conversion produced an empty or missing file.")

        logger.info("Conversion successful → %d bytes", docx_path.stat().st_size)

        # ── Stream the docx back ─────────────────────────────────────────────
        return FileResponse(
            path=str(docx_path),
            media_type=(
                "application/vnd.openxmlformats-officedocument"
                ".wordprocessingml.document"
            ),
            filename=f"{original_name}.docx",
            background=None,  # File will be sent synchronously; cleanup below
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Conversion failed for %s", file.filename)
        raise HTTPException(
            status_code=500,
            detail=f"Conversion failed: {str(exc)}",
        )
    finally:
        # Clean up the input PDF; leave docx until AFTER the response is sent.
        # For a simple synchronous FileResponse FastAPI reads the file before
        # returning, so we can clean up in the finally block safely.
        try:
            pdf_path.unlink(missing_ok=True)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Entry point (python main.py)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
