import os
import uuid
import logging
import tempfile
from pathlib import Path

import shutil
import zipfile
from fastapi import FastAPI, File, UploadFile, HTTPException, Form, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# pdf2docx is the best Python library for PDF → DOCX conversion
from pdf2docx import Converter

# pypdf for Merge/Split
from pypdf import PdfReader, PdfWriter
from typing import List

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
# Helpers
# ---------------------------------------------------------------------------
def cleanup_temp_dir(path: Path):
    """Deletes a directory and its contents."""
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
        logger.info("Cleaned up temp directory: %s", path)

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
# Word -> PDF conversion endpoint
# ---------------------------------------------------------------------------
import subprocess

@app.post("/convert-word")
async def convert_word(file: UploadFile = File(...)):
    """
    Accept a Word doc/docx file. Returns a PDF.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided.")

    original_name = Path(file.filename).stem
    content_type = file.content_type or ""

    if not (file.filename.lower().endswith((".doc", ".docx")) or "word" in content_type):
        raise HTTPException(status_code=400, detail="Only Word files (.doc, .docx) are accepted.")

    doc_bytes = await file.read()
    if len(doc_bytes) == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    tmp_dir = Path(tempfile.mkdtemp(prefix="doc_svc_"))
    
    # Needs to retain correct extension for LibreOffice
    extension = Path(file.filename).suffix.lower()
    doc_path = tmp_dir / f"{uuid.uuid4().hex}{extension}"
    pdf_path = tmp_dir / f"{doc_path.stem}.pdf"

    try:
        doc_path.write_bytes(doc_bytes)
        logger.info(
            "Converting %s (%d bytes) → %s via libreoffice",
            file.filename,
            len(doc_bytes),
            pdf_path.name,
        )

        # Build libreoffice command
        # --env:UserInstallation is to prevent collisions if run concurrently
        profile_dir = tmp_dir / "lo_profile"
        cmd = [
            "libreoffice",
            f"-env:UserInstallation=file://{profile_dir}",
            "--headless",
            "--convert-to", "pdf",
            "--outdir", str(tmp_dir),
            str(doc_path)
        ]
        
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode != 0:
            raise RuntimeError(f"LibreOffice returned {proc.returncode}. stderr={proc.stderr.decode('utf-8', errors='ignore')}")

        if not pdf_path.exists() or pdf_path.stat().st_size == 0:
            raise RuntimeError("Conversion produced empty or missing file.")

        logger.info("Conversion successful → %d bytes", pdf_path.stat().st_size)

        return FileResponse(
            path=str(pdf_path),
            media_type="application/pdf",
            filename=f"{original_name}.pdf",
            background=None,
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Conversion failed for %s", file.filename)
        raise HTTPException(status_code=500, detail=f"Conversion failed: {str(exc)}")
    finally:
        try:
            doc_path.unlink(missing_ok=True)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Image -> PDF conversion endpoint
# ---------------------------------------------------------------------------
from PIL import Image
import io

@app.post("/convert-image-to-pdf")
async def convert_image_to_pdf(file: UploadFile = File(...)):
    """
    Accept an Image file (jpg, png, etc). Returns a PDF.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided.")

    original_name = Path(file.filename).stem

    try:
        img_bytes = await file.read()
        if len(img_bytes) == 0:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")

        logger.info("Converting Image %s (%d bytes) → PDF", file.filename, len(img_bytes))

        # Open image using Pillow
        image = Image.open(io.BytesIO(img_bytes))
        
        # Convert to RGB if it's RGBA or P (to avoid errors when saving as PDF)
        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")
            
        # Save directly to bytes buffer
        pdf_bytes_io = io.BytesIO()
        image.save(pdf_bytes_io, "PDF", resolution=100.0)
        pdf_bytes_io.seek(0)
        
        pdf_bytes = pdf_bytes_io.read()
        logger.info("Conversion successful → %d bytes", len(pdf_bytes))

        # Return the bytes directly
        return JSONResponse(
            content=None, # Not used since we're returning raw bytes, but needed for proper syntax
        )
        
    except Exception as exc:
        logger.exception("Conversion failed for %s", file.filename)
        raise HTTPException(status_code=500, detail=f"Conversion failed: {str(exc)}")
        
@app.post("/convert-image-to-pdf-file")
async def convert_image_to_pdf_file(file: UploadFile = File(...)):
    """
    Accept an Image file (jpg, png, etc). Returns a PDF as file stream.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided.")

    original_name = Path(file.filename).stem

    try:
        img_bytes = await file.read()
        if len(img_bytes) == 0:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")

        tmp_dir = Path(tempfile.mkdtemp(prefix="img_svc_"))
        pdf_path = tmp_dir / f"{uuid.uuid4().hex}.pdf"

        logger.info("Converting Image %s (%d bytes) → %s", file.filename, len(img_bytes), pdf_path.name)

        # Open image using Pillow
        image = Image.open(io.BytesIO(img_bytes))
        
        # Convert to RGB if it's RGBA or P (to avoid errors when saving as PDF)
        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")
            
        # Save to file
        image.save(pdf_path, "PDF", resolution=100.0)
        
        if not pdf_path.exists() or pdf_path.stat().st_size == 0:
            raise RuntimeError("Conversion produced empty or missing file.")

        logger.info("Conversion successful → %d bytes", pdf_path.stat().st_size)

        return FileResponse(
            path=str(pdf_path),
            media_type="application/pdf",
            filename=f"{original_name}.pdf",
            background=None,
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Conversion failed for %s", file.filename)
        raise HTTPException(status_code=500, detail=f"Conversion failed: {str(exc)}")


# ---------------------------------------------------------------------------
# PDF Merge / Split endpoints
# ---------------------------------------------------------------------------

@app.post("/merge-pdfs")
async def merge_pdfs(background_tasks: BackgroundTasks, files: List[UploadFile] = File(...)):
    """
    Accept multiple PDF files. Returns a single merged PDF.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files provided for merging.")

    tmp_dir = Path(tempfile.mkdtemp(prefix="merge_svc_"))
    merged_path = tmp_dir / "merged_document.pdf"

    writer = PdfWriter()
    temp_files = []

    try:
        for file in files:
            if not file.filename.lower().endswith(".pdf"):
                continue
            
            content = await file.read()
            if not content:
                continue
                
            # Need to save to temp file because PdfReader often needs seekable stream
            t_path = tmp_dir / f"{uuid.uuid4().hex}.pdf"
            t_path.write_bytes(content)
            
            reader = PdfReader(str(t_path))
            for page in reader.pages:
                writer.add_page(page)

        if len(writer.pages) == 0:
            raise HTTPException(status_code=400, detail="No valid PDF pages were found to merge.")

        with open(merged_path, "wb") as f:
            writer.write(f)

        logger.info("Merged %d files into %s", len(temp_files), merged_path.name)

        background_tasks.add_task(cleanup_temp_dir, tmp_dir)

        return FileResponse(
            path=str(merged_path),
            media_type="application/pdf",
            filename="merged_document.pdf"
        )

    except Exception as exc:
        cleanup_temp_dir(tmp_dir)
        logger.exception("Merge failed")
        raise HTTPException(status_code=500, detail=f"Merge failed: {str(exc)}")

@app.post("/split-pdf")
async def split_pdf(background_tasks: BackgroundTasks, file: UploadFile = File(...), pages: str = Form("all")):
    """
    Accept a single PDF and a page range (e.g. '1-3, 5').
    If one range, returns a single PDF.
    If multiple ranges (comma-separated), returns a ZIP.
    """
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files can be split.")

    content = await file.read()
    tmp_dir = Path(tempfile.mkdtemp(prefix="split_svc_"))
    input_path = tmp_dir / "input.pdf"
    input_path.write_bytes(content)

    try:
        reader = PdfReader(str(input_path))
        total_pages = len(reader.pages)
        
        # Parse ranges
        range_strings = [p.strip() for p in pages.split(",") if p.strip()]
        if not range_strings:
            range_strings = ["all"]

        outputs = []
        
        for idx, r_str in enumerate(range_strings):
            writer = PdfWriter()
            pages_to_keep = []
            
            if r_str.lower() == "all":
                pages_to_keep = list(range(total_pages))
            else:
                if "-" in r_str:
                    start, end = r_str.split("-")
                    s_idx = int(start) - 1
                    e_idx = total_pages if end.lower() == "end" else int(end)
                    pages_to_keep = list(range(max(0, s_idx), min(e_idx, total_pages)))
                else:
                    p_num = int(r_str) - 1
                    if 0 <= p_num < total_pages:
                        pages_to_keep = [p_num]
            
            if pages_to_keep:
                for p in pages_to_keep:
                    writer.add_page(reader.pages[p])
                
                out_name = f"split_part_{idx+1}.pdf"
                out_path = tmp_dir / out_name
                with open(out_path, "wb") as f:
                    writer.write(f)
                outputs.append((out_path, out_name))

        if not outputs:
            raise HTTPException(status_code=400, detail="No valid pages selected.")

        background_tasks.add_task(cleanup_temp_dir, tmp_dir)

        if len(outputs) == 1:
            # Single range -> Return PDF directly
            return FileResponse(
                path=str(outputs[0][0]),
                media_type="application/pdf",
                filename=f"split_{file.filename}"
            )
        else:
            # Multiple ranges -> Return ZIP
            zip_path = tmp_dir / "split_parts.zip"
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for out_path, out_name in outputs:
                    zipf.write(out_path, out_name)
            
            return FileResponse(
                path=str(zip_path),
                media_type="application/zip",
                filename=f"split_{Path(file.filename).stem}.zip"
            )

    except ValueError:
        cleanup_temp_dir(tmp_dir)
        raise HTTPException(status_code=400, detail="Invalid page range format. Use e.g. '1-3, 5'")
    except Exception as exc:
        cleanup_temp_dir(tmp_dir)
        logger.exception("Split failed")
        raise HTTPException(status_code=500, detail=f"Split failed: {str(exc)}")
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
