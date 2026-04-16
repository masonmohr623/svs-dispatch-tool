from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, List, Tuple
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
import sqlite3
import requests
import os
import uuid
import re

app = FastAPI(title="SVS Dispatch Backend")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "svs_dispatch.db")

# -----------------------------
# CONFIG
# -----------------------------
ASANA_TOKEN = os.environ.get("ASANA_TOKEN", "")
ASANA_PROJECT_ID = "1206280340344209"
ASANA_SECTION_ID = "1206281947668069"

CLICKUP_API_TOKEN = os.environ.get("CLICKUP_API_TOKEN", "")

GOOGLE_DRIVE_PARENT_FOLDER_ID = os.environ.get(
    "GOOGLE_DRIVE_PARENT_FOLDER_ID",
    "15ZdGKmUFoIQZ2RKnfYsvz8btoc-dOK_P"
)
GOOGLE_SERVICE_ACCOUNT_FILE = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "google-service-account.json")
)
GOOGLE_DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

if os.path.isdir(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


# -----------------------------
# DATABASE
# -----------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(cur, table_name: str, column_name: str, column_sql: str):
    cols = cur.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing = [c["name"] if isinstance(c, sqlite3.Row) else c[1] for c in cols]
    if column_name not in existing:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


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

    ensure_column(cur, "events", "ip_camera_clickup_id", "TEXT DEFAULT ''")
    ensure_column(cur, "events", "access_control_clickup_id", "TEXT DEFAULT ''")
    ensure_column(cur, "events", "ip_camera_proposal", "TEXT DEFAULT ''")
    ensure_column(cur, "events", "access_control_proposal", "TEXT DEFAULT ''")
    ensure_column(cur, "events", "ip_checkin_comment_id", "TEXT DEFAULT ''")
    ensure_column(cur, "events", "ac_checkin_comment_id", "TEXT DEFAULT ''")
    ensure_column(cur, "events", "ip_checkout_comment_id", "TEXT DEFAULT ''")
    ensure_column(cur, "events", "ac_checkout_comment_id", "TEXT DEFAULT ''")

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
    ip_camera_clickup_id: Optional[str] = ""
    access_control_clickup_id: Optional[str] = ""
    ip_camera_proposal: Optional[str] = ""
    access_control_proposal: Optional[str] = ""
    ip_checkin_comment_id: Optional[str] = ""
    ac_checkin_comment_id: Optional[str] = ""
    ip_checkout_comment_id: Optional[str] = ""
    ac_checkout_comment_id: Optional[str] = ""


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
    ip_camera_proposal: Optional[str] = ""
    access_control_proposal: Optional[str] = ""


class CheckinPayload(BaseModel):
    event_id: int
    task_id: str = ""
    client: Optional[str] = ""
    site: Optional[str] = ""
    technician: Optional[str] = ""
    check_in_time: Optional[str] = ""


class CheckoutPayload(BaseModel):
    event_id: int
    task_id: str = ""
    client: Optional[str] = ""
    site: Optional[str] = ""
    technician: Optional[str] = ""
    check_out_time: Optional[str] = ""


class ResetBotCommentsPayload(BaseModel):
    event_id: int


class ProjectNameRequest(BaseModel):
    request_type: str
    site_name: str
    city: str
    state: str


class CreateDriveFolderRequest(BaseModel):
    folder_name: str
    is_survey: bool = False


class CreateAsanaTaskRequest(BaseModel):
    task_name: str


# -----------------------------
# HELPERS
# -----------------------------
def row_to_dict(row):
    return dict(row)


def normalize_event_row(row):
    data = dict(row)
    data["checked_in"] = bool(data.get("checked_in", 0))
    data["checked_out"] = bool(data.get("checked_out", 0))
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


def proposal_block(data: GenerateTemplateIn, mode: str) -> str:
    pt = (data.project_type or "").strip().lower()

    if mode == "service":
        return ""

    if pt == "both":
        return (
            f"IP Camera Proposal #: {data.ip_camera_proposal}\n"
            f"Access Control Proposal #: {data.access_control_proposal}"
        )

    return f"Proposal #: {data.proposal}"


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

{proposal_block(data, "survey")}

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

{proposal_block(data, "installation")}

If any of the information above needs correction, please email me as soon as possible.

Thank you,"""


# -----------------------------
# CLICKUP API HELPERS
# -----------------------------
def clickup_headers():
    return {
        "Authorization": CLICKUP_API_TOKEN,
        "Content-Type": "application/json"
    }


def send_clickup_comment(task_id: str, comment_text: str):
    if not task_id:
        return None, ""

    url = f"https://api.clickup.com/api/v2/task/{task_id}/comment"
    payload = {
        "comment_text": comment_text,
        "notify_all": False
    }

    response = requests.post(url, headers=clickup_headers(), json=payload, timeout=30)

    try:
        data = response.json()
    except Exception:
        data = {}

    comment_id = ""
    if isinstance(data, dict):
        if data.get("id"):
            comment_id = str(data.get("id"))
        elif isinstance(data.get("comment"), dict) and data["comment"].get("id"):
            comment_id = str(data["comment"]["id"])

    return response, comment_id


def delete_clickup_comment(comment_id: str):
    if not comment_id:
        return None

    url = f"https://api.clickup.com/api/v2/comment/{comment_id}"
    return requests.delete(url, headers=clickup_headers(), timeout=30)


# -----------------------------
# GOOGLE DRIVE HELPERS
# -----------------------------
def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_SERVICE_ACCOUNT_FILE,
        scopes=GOOGLE_DRIVE_SCOPES
    )
    return build("drive", "v3", credentials=creds)


def list_active_project_folders():
    service = get_drive_service()
    results = service.files().list(
        q=(
            f"'{GOOGLE_DRIVE_PARENT_FOLDER_ID}' in parents "
            f"and mimeType = 'application/vnd.google-apps.folder' "
            f"and trashed = false"
        ),
        fields="files(id, name, createdTime)",
        pageSize=1000
    ).execute()
    return results.get("files", [])


def get_next_project_number():
    folders = list_active_project_folders()
    now = datetime.now()
    prefix = now.strftime("%y%m")
    highest = 0

    for folder in folders:
        name = folder.get("name", "").strip()
        match = re.match(rf"^{prefix}(\d{{3}})\b", name)
        if match:
            number = int(match.group(1))
            if number > highest:
                highest = number

    next_number = highest + 1 if highest > 0 else 1
    return f"{prefix}{next_number:03d}"


def create_drive_folder(name: str, parent_id: str):
    service = get_drive_service()
    file_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id]
    }
    folder = service.files().create(
        body=file_metadata,
        fields="id, name, webViewLink"
    ).execute()
    return folder


def create_scw_provided_documents_subfolder(parent_folder_id: str):
    return create_drive_folder("SCW Provided Documents", parent_folder_id)


# -----------------------------
# ASANA HELPERS
# -----------------------------
def create_asana_task(name: str):
    url = "https://app.asana.com/api/1.0/tasks"
    headers = {
        "Authorization": f"Bearer {ASANA_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "data": {
            "name": name,
            "projects": [ASANA_PROJECT_ID],
            "memberships": [
                {
                    "project": ASANA_PROJECT_ID,
                    "section": ASANA_SECTION_ID
                }
            ]
        }
    }

    response = requests.post(url, headers=headers, json=payload, timeout=30)

    if not response.ok:
        raise Exception(f"Asana error: {response.text}")

    return response.json()


# -----------------------------
# ICS HELPERS
# -----------------------------
def ics_escape(value: str) -> str:
    value = str(value or "")
    value = value.replace("\\", "\\\\")
    value = value.replace(";", r"\;")
    value = value.replace(",", r"\,")
    value = value.replace("\n", r"\n")
    return value


def parse_time_12h(time_str: str) -> Optional[Tuple[int, int]]:
    if not time_str:
        return None
    match = re.match(r"^\s*(\d{1,2}):(\d{2})\s*(AM|PM)\s*$", time_str, flags=re.I)
    if not match:
        return None

    hour = int(match.group(1))
    minute = int(match.group(2))
    meridiem = match.group(3).upper()

    if meridiem == "AM" and hour == 12:
        hour = 0
    elif meridiem == "PM" and hour != 12:
        hour += 12

    return hour, minute


def build_event_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    parsed = parse_time_12h(time_str)
    if not date_str or not parsed:
        return None
    year, month, day = [int(x) for x in date_str.split("-")]
    return datetime(year, month, day, parsed[0], parsed[1])


def event_start_end_for_ics(event: dict) -> Tuple[Optional[datetime], Optional[datetime]]:
    date_str = event.get("site_date") or event.get("date") or ""
    arrival_type = (event.get("arrival_type") or "exact").lower()

    if arrival_type == "window":
        start_dt = build_event_datetime(date_str, event.get("window_start") or "")
        end_dt = build_event_datetime(date_str, event.get("window_end") or "")
        if start_dt and end_dt and end_dt > start_dt:
            return start_dt, end_dt
        if start_dt:
            return start_dt, start_dt + timedelta(hours=1)
        return None, None

    start_dt = build_event_datetime(date_str, event.get("time") or "")
    if start_dt:
        return start_dt, start_dt + timedelta(hours=1)

    return None, None


def build_ics_text(events: List[dict], host: str) -> str:
    now_utc = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//SVS Dispatch//Calendar Feed//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:SVS Dispatch Calendar",
        "X-WR-CALDESC:Secure Vision Solutions Dispatch Calendar",
    ]

    for event in events:
        start_dt, end_dt = event_start_end_for_ics(event)
        if not start_dt or not end_dt:
            continue

        uid = f"svs-event-{event.get('id', uuid.uuid4())}@{host}"
        summary_parts = [event.get("client") or "SVS Job"]
        if event.get("site"):
            summary_parts.append(event.get("site"))
        summary = " - ".join(summary_parts)

        description_lines = [
            f"Template Type: {event.get('template_type') or ''}",
            f"Project Type: {event.get('project_type') or ''}",
            f"Technician: {event.get('technician') or ''}",
            f"Technician Phone: {event.get('technician_phone') or ''}",
            f"Site Contact: {event.get('contact_name') or ''}",
            f"Site Contact Phone: {event.get('contact_phone') or ''}",
            f"Site Contact Email: {event.get('contact_email') or ''}",
            f"Display Time: {event.get('displayTime') or event.get('site_display_time') or ''}",
        ]

        if event.get("project_type") == "both":
            description_lines.append(f"IP Camera Proposal: {event.get('ip_camera_proposal') or ''}")
            description_lines.append(f"Access Control Proposal: {event.get('access_control_proposal') or ''}")
        else:
            description_lines.append(f"Proposal: {event.get('proposal') or ''}")

        if event.get("task_id"):
            description_lines.append(f"ClickUp ID: {event.get('task_id')}")
        if event.get("ip_camera_clickup_id"):
            description_lines.append(f"IP Camera ClickUp ID: {event.get('ip_camera_clickup_id')}")
        if event.get("access_control_clickup_id"):
            description_lines.append(f"Access Control ClickUp ID: {event.get('access_control_clickup_id')}")

        description = "\n".join(description_lines)

        lines.extend([
            "BEGIN:VEVENT",
            f"UID:{ics_escape(uid)}",
            f"DTSTAMP:{now_utc}",
            f"DTSTART:{start_dt.strftime('%Y%m%dT%H%M%S')}",
            f"DTEND:{end_dt.strftime('%Y%m%dT%H%M%S')}",
            f"SUMMARY:{ics_escape(summary)}",
            f"DESCRIPTION:{ics_escape(description)}",
            f"LOCATION:{ics_escape(event.get('address') or '')}",
            "BEGIN:VALARM",
            "ACTION:DISPLAY",
            "DESCRIPTION:SVS job reminder",
            "TRIGGER:-PT30M",
            "END:VALARM",
            "END:VEVENT",
        ])

    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"


# -----------------------------
# HEALTH / ROOT
# -----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


# -----------------------------
# TEST ROUTES
# -----------------------------
@app.get("/test-drive")
def test_drive():
    try:
        folders = list_active_project_folders()
        next_number = get_next_project_number()
        return {
            "status": "ok",
            "folder_count": len(folders),
            "next_project_number": next_number,
            "sample_folders": folders[:10]
        }
    except Exception as e:
        return {
            "status": "error",
            "folder_id_being_used": GOOGLE_DRIVE_PARENT_FOLDER_ID,
            "error": str(e)
        }


@app.get("/test-asana")
def test_asana():
    try:
        test_name = "TEST DELETE ME"
        response = create_asana_task(test_name)
        return {
            "status": "ok",
            "response": response
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/test-clickup")
def test_clickup():
    url = "https://api.clickup.com/api/v2/team"
    response = requests.get(url, headers=clickup_headers(), timeout=30)
    try:
        return {
            "status_code": response.status_code,
            "response": response.json()
        }
    except Exception:
        return {
            "status_code": response.status_code,
            "response_text": response.text
        }


@app.get("/test-clickup-comment/{task_id}")
def test_clickup_comment(task_id: str):
    response, comment_id = send_clickup_comment(task_id, "Test comment from SVS bot.")
    try:
        data = response.json() if response is not None else None
    except Exception:
        data = {"raw_text": response.text if response is not None else ""}

    return {
        "status_code": response.status_code if response else None,
        "comment_id": comment_id,
        "response": data
    }


# -----------------------------
# SCW JOB REQUEST ROUTES
# -----------------------------
@app.post("/scw/next-name")
def scw_next_name(data: ProjectNameRequest):
    try:
        next_number = get_next_project_number()
        request_type_clean = data.request_type.strip().title()
        full_name = f"{next_number} {request_type_clean} - {data.site_name.strip()}, {data.city.strip()} {data.state.strip()}"

        return {
            "status": "ok",
            "next_project_number": next_number,
            "full_name": full_name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/scw/create-drive-folder")
def scw_create_drive_folder(data: CreateDriveFolderRequest):
    try:
        main_folder = create_drive_folder(data.folder_name, GOOGLE_DRIVE_PARENT_FOLDER_ID)

        subfolder = None
        if data.is_survey:
            subfolder = create_scw_provided_documents_subfolder(main_folder["id"])

        return {
            "status": "ok",
            "main_folder": main_folder,
            "scw_provided_documents": subfolder
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/scw/create-asana-task")
def scw_create_asana_task(data: CreateAsanaTaskRequest):
    try:
        task = create_asana_task(data.task_name)
        return {
            "status": "ok",
            "task": task
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
    rows = conn.execute(
        "SELECT * FROM events ORDER BY date, COALESCE(time, ''), COALESCE(displayTime, '')"
    ).fetchall()
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
            checked_in, checked_out, check_in_time, check_out_time,
            ip_camera_clickup_id, access_control_clickup_id,
            ip_camera_proposal, access_control_proposal,
            ip_checkin_comment_id, ac_checkin_comment_id,
            ip_checkout_comment_id, ac_checkout_comment_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        event.ip_camera_clickup_id or "",
        event.access_control_clickup_id or "",
        event.ip_camera_proposal or "",
        event.access_control_proposal or "",
        event.ip_checkin_comment_id or "",
        event.ac_checkin_comment_id or "",
        event.ip_checkout_comment_id or "",
        event.ac_checkout_comment_id or "",
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
            checked_in = ?, checked_out = ?, check_in_time = ?, check_out_time = ?,
            ip_camera_clickup_id = ?, access_control_clickup_id = ?,
            ip_camera_proposal = ?, access_control_proposal = ?,
            ip_checkin_comment_id = ?, ac_checkin_comment_id = ?,
            ip_checkout_comment_id = ?, ac_checkout_comment_id = ?
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
        event.ip_camera_clickup_id or "",
        event.access_control_clickup_id or "",
        event.ip_camera_proposal or "",
        event.access_control_proposal or "",
        event.ip_checkin_comment_id or "",
        event.ac_checkin_comment_id or "",
        event.ip_checkout_comment_id or "",
        event.ac_checkout_comment_id or "",
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
@app.options("/generate-dispatch")
def options_generate_dispatch():
    return {"ok": True}


@app.post("/generate-dispatch")
def generate_dispatch(data: GenerateTemplateIn):
    return {
        "subject": build_subject(data, "dispatch"),
        "message": build_dispatch_message(data)
    }


@app.options("/generate-service")
def options_generate_service():
    return {"ok": True}


@app.post("/generate-service")
def generate_service(data: GenerateTemplateIn):
    return {
        "subject": build_subject(data, "service"),
        "message": build_service_message(data)
    }


@app.options("/generate-installation")
def options_generate_installation():
    return {"ok": True}


@app.post("/generate-installation")
def generate_installation(data: GenerateTemplateIn):
    return {
        "subject": build_subject(data, "installation"),
        "message": build_installation_message(data)
    }


# -----------------------------
# CHECK-IN / CHECK-OUT
# -----------------------------
@app.post("/send-checkin")
def send_checkin(payload: CheckinPayload):
    conn = get_conn()
    cur = conn.cursor()

    event = cur.execute("SELECT * FROM events WHERE id = ?", (payload.event_id,)).fetchone()
    if not event:
        conn.close()
        raise HTTPException(status_code=404, detail="Event not found")

    event_data = dict(event)
    comment = f"Status Update: The technician is on-site and checked in at {payload.check_in_time} (Local Time)."

    try:
        sent_to = []

        ip_checkin_comment_id = event_data.get("ip_checkin_comment_id", "") or ""
        ac_checkin_comment_id = event_data.get("ac_checkin_comment_id", "") or ""

        project_type = (event_data.get("project_type") or "").lower()

        if project_type == "both":
            ip_id = event_data.get("ip_camera_clickup_id") or ""
            ac_id = event_data.get("access_control_clickup_id") or ""

            if ip_id:
                _, ip_comment_id = send_clickup_comment(ip_id, comment)
                ip_checkin_comment_id = ip_comment_id or ""
                sent_to.append(f"IP Camera ({ip_id})")

            if ac_id:
                _, ac_comment_id = send_clickup_comment(ac_id, comment)
                ac_checkin_comment_id = ac_comment_id or ""
                sent_to.append(f"Access Control ({ac_id})")
        else:
            task_id = payload.task_id or event_data.get("task_id") or ""
            if task_id:
                _, single_comment_id = send_clickup_comment(task_id, comment)
                ip_checkin_comment_id = single_comment_id or ""
                sent_to.append(task_id)

        cur.execute("""
            UPDATE events
            SET checked_in = ?,
                checked_out = ?,
                check_in_time = ?,
                check_out_time = ?,
                ip_checkin_comment_id = ?,
                ac_checkin_comment_id = ?
            WHERE id = ?
        """, (
            1,
            0,
            payload.check_in_time or "",
            "",
            ip_checkin_comment_id,
            ac_checkin_comment_id,
            payload.event_id
        ))

        conn.commit()
        row = cur.execute("SELECT * FROM events WHERE id = ?", (payload.event_id,)).fetchone()
        conn.close()

        return {
            "status": "sent",
            "sent_to": sent_to,
            "event": normalize_event_row(row)
        }
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/send-checkout")
def send_checkout(payload: CheckoutPayload):
    conn = get_conn()
    cur = conn.cursor()

    event = cur.execute("SELECT * FROM events WHERE id = ?", (payload.event_id,)).fetchone()
    if not event:
        conn.close()
        raise HTTPException(status_code=404, detail="Event not found")

    event_data = dict(event)
    comment = (
        f"Status update: The technician is off-site and checked out at "
        f"{payload.check_out_time} (Local Time). Survey documents will be available once they have been processed."
    )

    try:
        sent_to = []

        ip_checkout_comment_id = event_data.get("ip_checkout_comment_id", "") or ""
        ac_checkout_comment_id = event_data.get("ac_checkout_comment_id", "") or ""

        project_type = (event_data.get("project_type") or "").lower()

        if project_type == "both":
            ip_id = event_data.get("ip_camera_clickup_id") or ""
            ac_id = event_data.get("access_control_clickup_id") or ""

            if ip_id:
                _, ip_comment_id = send_clickup_comment(ip_id, comment)
                ip_checkout_comment_id = ip_comment_id or ""
                sent_to.append(f"IP Camera ({ip_id})")

            if ac_id:
                _, ac_comment_id = send_clickup_comment(ac_id, comment)
                ac_checkout_comment_id = ac_comment_id or ""
                sent_to.append(f"Access Control ({ac_id})")
        else:
            task_id = payload.task_id or event_data.get("task_id") or ""
            if task_id:
                _, single_comment_id = send_clickup_comment(task_id, comment)
                ip_checkout_comment_id = single_comment_id or ""
                sent_to.append(task_id)

        cur.execute("""
            UPDATE events
            SET checked_in = ?,
                checked_out = ?,
                check_out_time = ?,
                ip_checkout_comment_id = ?,
                ac_checkout_comment_id = ?
            WHERE id = ?
        """, (
            1,
            1,
            payload.check_out_time or "",
            ip_checkout_comment_id,
            ac_checkout_comment_id,
            payload.event_id
        ))

        conn.commit()
        row = cur.execute("SELECT * FROM events WHERE id = ?", (payload.event_id,)).fetchone()
        conn.close()

        return {
            "status": "sent",
            "sent_to": sent_to,
            "event": normalize_event_row(row)
        }
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reset-bot-comments")
def reset_bot_comments(payload: ResetBotCommentsPayload):
    conn = get_conn()
    cur = conn.cursor()

    event = cur.execute("SELECT * FROM events WHERE id = ?", (payload.event_id,)).fetchone()
    if not event:
        conn.close()
        raise HTTPException(status_code=404, detail="Event not found")

    event_data = dict(event)
    deleted = []
    skipped = []

    try:
        project_type = (event_data.get("project_type") or "").lower()

        if project_type == "both":
            if event_data.get("ip_checkin_comment_id"):
                delete_clickup_comment(event_data.get("ip_checkin_comment_id"))
                deleted.append("IP check-in")
            else:
                skipped.append("IP check-in")

            if event_data.get("ac_checkin_comment_id"):
                delete_clickup_comment(event_data.get("ac_checkin_comment_id"))
                deleted.append("AC check-in")
            else:
                skipped.append("AC check-in")

            if event_data.get("ip_checkout_comment_id"):
                delete_clickup_comment(event_data.get("ip_checkout_comment_id"))
                deleted.append("IP check-out")
            else:
                skipped.append("IP check-out")

            if event_data.get("ac_checkout_comment_id"):
                delete_clickup_comment(event_data.get("ac_checkout_comment_id"))
                deleted.append("AC check-out")
            else:
                skipped.append("AC check-out")
        else:
            if event_data.get("ip_checkin_comment_id"):
                delete_clickup_comment(event_data.get("ip_checkin_comment_id"))
                deleted.append("check-in")
            else:
                skipped.append("check-in")

            if event_data.get("ip_checkout_comment_id"):
                delete_clickup_comment(event_data.get("ip_checkout_comment_id"))
                deleted.append("check-out")
            else:
                skipped.append("check-out")

        cur.execute("""
            UPDATE events
            SET checked_in = 0,
                checked_out = 0,
                check_in_time = '',
                check_out_time = '',
                ip_checkin_comment_id = '',
                ac_checkin_comment_id = '',
                ip_checkout_comment_id = '',
                ac_checkout_comment_id = ''
            WHERE id = ?
        """, (payload.event_id,))
        conn.commit()

        row = cur.execute("SELECT * FROM events WHERE id = ?", (payload.event_id,)).fetchone()
        conn.close()

        return {
            "status": "reset",
            "deleted": deleted,
            "skipped": skipped,
            "event": normalize_event_row(row)
        }
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# CALENDAR SUBSCRIPTION FEED
# -----------------------------
@app.get("/calendar.ics")
def calendar_ics():
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM events ORDER BY date, COALESCE(time, ''), COALESCE(window_start, '')"
    ).fetchall()
    conn.close()

    events = [normalize_event_row(r) for r in rows]
    host = os.environ.get("PUBLIC_HOSTNAME", "svs-dispatch.local")
    ics_text = build_ics_text(events, host)

    return Response(
        content=ics_text,
        media_type="text/calendar; charset=utf-8",
        headers={
            "Content-Disposition": "inline; filename=svs-calendar.ics",
            "Cache-Control": "no-store"
        }
    )


# -----------------------------
# FRONTEND FILES
# -----------------------------
@app.get("/svs-logo.png")
def serve_logo():
    return FileResponse(os.path.join(FRONTEND_DIR, "svs-logo.png"))


@app.get("/favicon.ico")
def serve_favicon():
    favicon_path = os.path.join(FRONTEND_DIR, "favicon.ico")
    if os.path.exists(favicon_path):
        return FileResponse(favicon_path)
    raise HTTPException(status_code=404, detail="favicon.ico not found")


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


@app.get("/scw-requests.html")
def serve_scw_requests():
    return FileResponse(os.path.join(FRONTEND_DIR, "scw-requests.html"))


@app.get("/")
def serve_home():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=True)