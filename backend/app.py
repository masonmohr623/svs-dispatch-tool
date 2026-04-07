from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import sqlite3
import requests
import os

app = FastAPI(title="SVS Dispatch Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "svs_dispatch.db")
CLICKUP_WEBHOOK_URL = os.environ.get(
    "CLICKUP_WEBHOOK_URL",
    "https://hook.us1.make.com/tzham3njl79ucri6lmsd9imvecnft9xq"
)


# -----------------------------
# DATABASE
# -----------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS technicians (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            phone TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client TEXT,
            site TEXT,
            address TEXT,
            technician TEXT,
            technician_phone TEXT,
            contact_name TEXT,
            contact_email TEXT,
            contact_phone TEXT,
            proposal TEXT,
            task_id TEXT,
            template_type TEXT,
            project_type TEXT,
            date TEXT,
            site_date TEXT,
            time TEXT,
            window_start TEXT,
            window_end TEXT,
            displayTime TEXT,
            site_display_time TEXT,
            site_timezone TEXT,
            site_timezone_label TEXT,
            arrival_type TEXT,
            checked_in INTEGER DEFAULT 0,
            checked_out INTEGER DEFAULT 0,
            check_in_time TEXT,
            check_out_time TEXT
        )
    """)

    conn.commit()
    conn.close()


init_db()


# -----------------------------
# MODELS
# -----------------------------
class TechnicianIn(BaseModel):
    name: str
    phone: str


class TechnicianOut(BaseModel):
    id: int
    name: str
    phone: str


class EventIn(BaseModel):
    client: Optional[str] = ""
    site: Optional[str] = ""
    address: Optional[str] = ""
    technician: Optional[str] = ""
    technician_phone: Optional[str] = ""
    contact_name: Optional[str] = ""
    contact_email: Optional[str] = ""
    contact_phone: Optional[str] = ""
    proposal: Optional[str] = ""
    task_id: Optional[str] = ""
    template_type: Optional[str] = ""
    project_type: Optional[str] = ""
    date: Optional[str] = ""
    site_date: Optional[str] = ""
    time: Optional[str] = ""
    window_start: Optional[str] = ""
    window_end: Optional[str] = ""
    displayTime: Optional[str] = ""
    site_display_time: Optional[str] = ""
    site_timezone: Optional[str] = ""
    site_timezone_label: Optional[str] = ""
    arrival_type: Optional[str] = "exact"
    checked_in: bool = False
    checked_out: bool = False
    check_in_time: Optional[str] = ""
    check_out_time: Optional[str] = ""


class EventOut(EventIn):
    id: int


class GenerateTemplateIn(BaseModel):
    arrival_type: str = "exact"
    date: str = ""
    time: str = ""
    window_start: str = ""
    window_end: str = ""
    project_type: str = "ip_camera"
    tech_name: str = ""
    tech_phone: str = ""
    contact_name: str = ""
    contact_email: str = ""
    contact_phone: str = ""
    client: str = ""
    site: str = ""
    address: str = ""
    proposal: Optional[str] = ""


class CheckinPayload(BaseModel):
    task_id: str
    client: Optional[str] = ""
    site: Optional[str] = ""
    technician: Optional[str] = ""
    check_in_time: Optional[str] = ""


class CheckoutPayload(BaseModel):
    task_id: str
    client: Optional[str] = ""
    site: Optional[str] = ""
    technician: Optional[str] = ""
    check_out_time: Optional[str] = ""


# -----------------------------
# HELPERS
# -----------------------------
def row_to_dict(row):
    return dict(row)


def normalize_event_row(row):
    data = dict(row)
    data["checked_in"] = bool(data["checked_in"])
    data["checked_out"] = bool(data["checked_out"])
    return data


def pretty_date(date_str: str) -> str:
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        day = dt.day
        if 10 <= day % 100 <= 20:
            suffix = "th"
        else:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
        return f"{dt.strftime('%A')}, {dt.strftime('%B')} {day}{suffix}"
    except Exception:
        return date_str


def project_scope_label(project_type: str, mode: str) -> str:
    pt = (project_type or "ip_camera").strip().lower()

    if mode == "survey":
        if pt == "access_control":
            return "Site Survey for Access Control Installation"
        if pt == "both":
            return "Site Survey for IP Camera and Access Control Installation"
        return "Site Survey for IP Camera Installation"

    if mode == "service":
        if pt == "access_control":
            return "Service Access Control System"
        if pt == "both":
            return "Service IP Camera and Access Control System"
        return "Service IP Camera System"

    if mode == "installation":
        if pt == "access_control":
            return "Access Control Installation"
        if pt == "both":
            return "IP Camera and Access Control Installation"
        return "IP Camera Installation"

    return ""


def subject_prefix(project_type: str, mode: str) -> str:
    pt = (project_type or "ip_camera").strip().lower()

    if mode == "dispatch":
        if pt == "access_control":
            return "SCW Access Control Site Survey Walkthrough Confirmed"
        if pt == "both":
            return "SCW IP Camera / Access Control Site Survey Walkthrough Confirmed"
        return "SCW IP Camera Site Survey Walkthrough Confirmed"

    if mode == "service":
        if pt == "access_control":
            return "SCW Access Control Service Confirmed"
        if pt == "both":
            return "SCW IP Camera / Access Control Service Confirmed"
        return "SCW IP Camera Service Confirmed"

    if mode == "installation":
        if pt == "access_control":
            return "SCW Access Control Installation Confirmed"
        if pt == "both":
            return "SCW IP Camera / Access Control Installation Confirmed"
        return "SCW IP Camera Installation Confirmed"

    return "SCW Confirmation"


def time_phrase(data: GenerateTemplateIn) -> str:
    if data.arrival_type == "window":
        return f"with an Arrival Window of {data.window_start} - {data.window_end} (Local Time)"
    return f"at {data.time} (Local Time)"


def greeting_for_time(data: GenerateTemplateIn) -> str:
    sample = (data.time or data.window_start or "").upper()
    if "AM" in sample:
        return "morning"
    return "afternoon"


def build_subject(data: GenerateTemplateIn, mode: str) -> str:
    return f"{subject_prefix(data.project_type, mode)} for {pretty_date(data.date)} {time_phrase(data)}"


def build_dispatch_message(data: GenerateTemplateIn) -> str:
    return f"""Good {greeting_for_time(data)} everyone,

The technician is confirmed for {pretty_date(data.date)} {time_phrase(data)} for arrival on site.

Please have a dedicated Point of Contact available once we arrive on-site. If at that time you have any concerns with blind spots covered by our proposal, our technician can make note of it and your SCW account executive will reach out to discuss additional coverage options.

Technician information
{data.tech_name}
Phone: {data.tech_phone}

Project Scope
{project_scope_label(data.project_type, "survey")}

Site Contact Information
Name: {data.contact_name}
Email: {data.contact_email}
Phone: {data.contact_phone}

Client: {data.client}
Site: {data.site}
Project Address: {data.address}

Proposal #: {data.proposal}

If any of the information above needs correction, please email me as soon as possible.

Thank you,"""


def build_service_message(data: GenerateTemplateIn) -> str:
    return f"""Good {greeting_for_time(data)} everyone,

The technician is confirmed for {pretty_date(data.date)} {time_phrase(data)} for arrival on site.

Technician information
{data.tech_name}
Phone: {data.tech_phone}

Project Scope
{project_scope_label(data.project_type, "service")}

Site Contact Information
Name: {data.contact_name}
Email: {data.contact_email}
Phone: {data.contact_phone}

Client: {data.client}
Site: {data.site}
Project Address: {data.address}

If any of the information above needs correction, please email me as soon as possible.

Thank you,"""


def build_installation_message(data: GenerateTemplateIn) -> str:
    return f"""Good {greeting_for_time(data)} everyone,

The technician is confirmed for {pretty_date(data.date)} {time_phrase(data)} for arrival on site.

Please have a dedicated Point of Contact available on-site. We ask that the Point of Contact walk the site with the technician prior to work beginning so all final camera or device locations can be confirmed before installation starts. Any requested changes should be discussed before work begins.

Technician information
{data.tech_name}
Phone: {data.tech_phone}

Project Scope
{project_scope_label(data.project_type, "installation")}

Site Contact Information
Name: {data.contact_name}
Email: {data.contact_email}
Phone: {data.contact_phone}

Client: {data.client}
Site: {data.site}
Project Address: {data.address}

Proposal #: {data.proposal}

If any of the information above needs correction, please email me as soon as possible.

Thank you,"""


# -----------------------------
# HEALTH / ROOT
# -----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


# -----------------------------
# TECHNICIANS
# -----------------------------
@app.get("/technicians", response_model=List[TechnicianOut])
def get_technicians():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM technicians ORDER BY name COLLATE NOCASE").fetchall()
    conn.close()
    return [row_to_dict(r) for r in rows]


@app.post("/technicians", response_model=TechnicianOut)
def add_technician(tech: TechnicianIn):
    conn = get_conn()
    cur = conn.cursor()

    existing = cur.execute(
        "SELECT * FROM technicians WHERE LOWER(name) = LOWER(?)",
        (tech.name.strip(),)
    ).fetchone()

    if existing:
        conn.close()
        return row_to_dict(existing)

    cur.execute(
        "INSERT INTO technicians (name, phone) VALUES (?, ?)",
        (tech.name.strip(), tech.phone.strip())
    )
    conn.commit()

    row = cur.execute("SELECT * FROM technicians WHERE id = ?", (cur.lastrowid,)).fetchone()
    conn.close()
    return row_to_dict(row)


@app.put("/technicians/{tech_id}", response_model=TechnicianOut)
def update_technician(tech_id: int, tech: TechnicianIn):
    conn = get_conn()
    cur = conn.cursor()

    existing = cur.execute("SELECT * FROM technicians WHERE id = ?", (tech_id,)).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="Technician not found")

    cur.execute(
        "UPDATE technicians SET name = ?, phone = ? WHERE id = ?",
        (tech.name.strip(), tech.phone.strip(), tech_id)
    )
    conn.commit()

    row = cur.execute("SELECT * FROM technicians WHERE id = ?", (tech_id,)).fetchone()
    conn.close()
    return row_to_dict(row)


@app.delete("/technicians/{tech_id}")
def delete_technician(tech_id: int):
    conn = get_conn()
    cur = conn.cursor()

    existing = cur.execute("SELECT * FROM technicians WHERE id = ?", (tech_id,)).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="Technician not found")

    cur.execute("DELETE FROM technicians WHERE id = ?", (tech_id,))
    conn.commit()
    conn.close()

    return {"status": "deleted"}


# -----------------------------
# EVENTS
# -----------------------------
@app.get("/events", response_model=List[EventOut])
def get_events():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM events ORDER BY date, COALESCE(time, ''), COALESCE(displayTime, '')").fetchall()
    conn.close()
    return [normalize_event_row(r) for r in rows]


@app.post("/events", response_model=EventOut)
def create_event(event: EventIn):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO events (
            client, site, address, technician, technician_phone,
            contact_name, contact_email, contact_phone, proposal, task_id,
            template_type, project_type, date, site_date, time,
            window_start, window_end, displayTime, site_display_time,
            site_timezone, site_timezone_label, arrival_type,
            checked_in, checked_out, check_in_time, check_out_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        event.client,
        event.site,
        event.address,
        event.technician,
        event.technician_phone,
        event.contact_name,
        event.contact_email,
        event.contact_phone,
        event.proposal,
        event.task_id,
        event.template_type,
        event.project_type,
        event.date,
        event.site_date,
        event.time,
        event.window_start,
        event.window_end,
        event.displayTime,
        event.site_display_time,
        event.site_timezone,
        event.site_timezone_label,
        event.arrival_type,
        int(bool(event.checked_in)),
        int(bool(event.checked_out)),
        event.check_in_time,
        event.check_out_time
    ))

    conn.commit()
    row = cur.execute("SELECT * FROM events WHERE id = ?", (cur.lastrowid,)).fetchone()
    conn.close()
    return normalize_event_row(row)


@app.put("/events/{event_id}", response_model=EventOut)
def update_event(event_id: int, event: EventIn):
    conn = get_conn()
    cur = conn.cursor()

    existing = cur.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="Event not found")

    cur.execute("""
        UPDATE events SET
            client = ?, site = ?, address = ?, technician = ?, technician_phone = ?,
            contact_name = ?, contact_email = ?, contact_phone = ?, proposal = ?, task_id = ?,
            template_type = ?, project_type = ?, date = ?, site_date = ?, time = ?,
            window_start = ?, window_end = ?, displayTime = ?, site_display_time = ?,
            site_timezone = ?, site_timezone_label = ?, arrival_type = ?,
            checked_in = ?, checked_out = ?, check_in_time = ?, check_out_time = ?
        WHERE id = ?
    """, (
        event.client,
        event.site,
        event.address,
        event.technician,
        event.technician_phone,
        event.contact_name,
        event.contact_email,
        event.contact_phone,
        event.proposal,
        event.task_id,
        event.template_type,
        event.project_type,
        event.date,
        event.site_date,
        event.time,
        event.window_start,
        event.window_end,
        event.displayTime,
        event.site_display_time,
        event.site_timezone,
        event.site_timezone_label,
        event.arrival_type,
        int(bool(event.checked_in)),
        int(bool(event.checked_out)),
        event.check_in_time,
        event.check_out_time,
        event_id
    ))

    conn.commit()
    row = cur.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
    conn.close()
    return normalize_event_row(row)


@app.delete("/events/{event_id}")
def delete_event(event_id: int):
    conn = get_conn()
    cur = conn.cursor()

    existing = cur.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="Event not found")

    cur.execute("DELETE FROM events WHERE id = ?", (event_id,))
    conn.commit()
    conn.close()

    return {"status": "deleted"}


# -----------------------------
# TEMPLATE GENERATION
# -----------------------------
@app.post("/generate-dispatch")
def generate_dispatch(data: GenerateTemplateIn):
    return {
        "subject": build_subject(data, "dispatch"),
        "message": build_dispatch_message(data)
    }


@app.post("/generate-service")
def generate_service(data: GenerateTemplateIn):
    return {
        "subject": build_subject(data, "service"),
        "message": build_service_message(data)
    }


@app.post("/generate-installation")
def generate_installation(data: GenerateTemplateIn):
    return {
        "subject": build_subject(data, "installation"),
        "message": build_installation_message(data)
    }


# -----------------------------
# CHECK-IN / CHECK-OUT WEBHOOKS
# -----------------------------
@app.post("/send-checkin")
def send_checkin(payload: CheckinPayload):
    comment = f"Status Update: The technician is on-site and checked in at {payload.check_in_time} (Local Time)."

    body = {
        "task_id": payload.task_id,
        "type": "checkin",
        "comment": comment,
        "client": payload.client,
        "site": payload.site,
        "technician": payload.technician,
        "check_in_time": payload.check_in_time
    }

    try:
        r = requests.post(CLICKUP_WEBHOOK_URL, json=body, timeout=20)
        return {
            "status": "sent",
            "status_code": r.status_code,
            "response_text": r.text[:1000]
        }
    except Exception as e:
        return {
            "status": "error",
            "status_code": 500,
            "response_text": str(e)
        }


@app.post("/send-checkout")
def send_checkout(payload: CheckoutPayload):
    comment = (
        f"Status update: The technician is off-site and checked out at "
        f"{payload.check_out_time} (Local Time). Survey documents will be available once they have been processed."
    )

    body = {
        "task_id": payload.task_id,
        "type": "checkout",
        "comment": comment,
        "client": payload.client,
        "site": payload.site,
        "technician": payload.technician,
        "check_out_time": payload.check_out_time
    }

    try:
        r = requests.post(CLICKUP_WEBHOOK_URL, json=body, timeout=20)
        return {
            "status": "sent",
            "status_code": r.status_code,
            "response_text": r.text[:1000]
        }
    except Exception as e:
        return {
            "status": "error",
            "status_code": 500,
            "response_text": str(e)
        }


# -----------------------------
# FRONTEND FILES
# -----------------------------
@app.get("/index.html")
def serve_index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


@app.get("/dispatch.html")
def serve_dispatch():
    return FileResponse(os.path.join(FRONTEND_DIR, "dispatch.html"))


@app.get("/service.html")
def serve_service():
    return FileResponse(os.path.join(FRONTEND_DIR, "service.html"))


@app.get("/installation.html")
def serve_installation():
    return FileResponse(os.path.join(FRONTEND_DIR, "installation.html"))


@app.get("/calendar.html")
def serve_calendar():
    return FileResponse(os.path.join(FRONTEND_DIR, "calendar.html"))


@app.get("/checkin.html")
def serve_checkin():
    return FileResponse(os.path.join(FRONTEND_DIR, "checkin.html"))


@app.get("/technicians.html")
def serve_technicians():
    return FileResponse(os.path.join(FRONTEND_DIR, "technicians.html"))


@app.get("/")
def serve_home():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))