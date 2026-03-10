# PDF Python Microservice

A lightweight **FastAPI** microservice that converts PDF files to Word (`.docx`) using the [`pdf2docx`](https://github.com/dothinking/pdf2docx) library — the best pure-Python solution for preserving layout, tables, fonts, and images during conversion.

---

## Port
`8000`

## Endpoint
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness probe |
| `POST` | `/convert-pdf` | Upload a PDF, receive a `.docx` |

---

## Setup

### 1. Create a virtual environment
```bash
cd pdf-python-service
python -m venv venv
```

### 2. Activate it
- **Windows**: `venv\Scripts\activate`
- **macOS/Linux**: `source venv/bin/activate`

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the server
```bash
# Development (auto-reload on file change)
uvicorn main:app --reload --port 8000

# Or simply:
python main.py
```

The interactive API docs will be available at **http://localhost:8000/docs**.

---

## Testing with Postman (Layer 1)

1. Create a new **POST** request to `http://localhost:8000/convert-pdf`
2. Set Body → **form-data**
3. Add a key `file` (type = **File**), attach any PDF
4. Send — you should receive a `.docx` download

---

## Full stack startup sequence

| Terminal | Command | Port |
|----------|---------|------|
| 1 – Python | `uvicorn main:app --reload --port 8000` | 8000 |
| 2 – NestJS | `npm run start:dev` (inside `pdf-backend/`) | 3000 |
| 3 – React | `npm run dev` (inside `pdf-frontend/`) | 5173 |

---

## How it integrates with NestJS

NestJS receives the PDF from React, forwards it internally to this service at  
`http://localhost:8000/convert-pdf`, streams the `.docx` response back to the browser.

See `pdf-backend/src/modules/pdf-to-word/service/pdf-to-word.service.ts` for the bridging logic.
