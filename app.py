"""
app.py — TIGIE Tariff Scraper (Streamlit UI)
Uses requests + BeautifulSoup to scrape tigie.com.mx and export to Excel.
"""

from datetime import datetime
import io
import time
from typing import List, Dict, Any, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# -------------------------
# Config / Session
# -------------------------
_SESSION = requests.Session()
_SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
)


# -------------------------
# Scraping helpers
# -------------------------
def _fetch(url: str, retries: int = 3, timeout: int = 20) -> Optional[BeautifulSoup]:
    """GET a URL and return BeautifulSoup or None."""
    for attempt in range(retries):
        try:
            r = _SESSION.get(url, timeout=timeout)
            r.raise_for_status()
            return BeautifulSoup(r.text, "lxml")
        except Exception:
            if attempt < retries - 1:
                time.sleep(1 + attempt)  # backoff
            else:
                return None


def _parse_historic(soup: Optional[BeautifulSoup]) -> Dict[str, str]:
    """Parse the Historic Record table and return the most recent published row as a dict."""
    empty = dict(
        hist_hs_code="",
        hist_description="",
        hist_valid_since="",
        hist_published="",
        hist_ad_valorem="",
        hist_unit="",
        hist_m3_unit_id="",
    )
    if not soup:
        return empty

    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]
        # Basic heuristic: must contain 'ad-valorem' and 'published'
        if "ad-valorem" not in " ".join(headers) or "published" not in " ".join(headers):
            continue

        data_rows = table.find_all("tr")[1:]
        best_cells = None
        best_date = None

        for row in data_rows:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            # find index of Published-like column
            try:
                pi = next(i for i, h in enumerate(headers) if "publish" in h)
                pv = cells[pi].get_text(strip=True) if pi < len(cells) else ""
                parsed = None
                for fmt in ("%b/%d/%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%b-%Y"):
                    try:
                        parsed = datetime.strptime(pv, fmt)
                        break
                    except Exception:
                        pass
                if parsed and (best_date is None or parsed > best_date):
                    best_date = parsed
                    best_cells = cells
            except StopIteration:
                continue

        if best_cells is None and data_rows:
            best_cells = data_rows[-1].find_all(["td", "th"])

        if not best_cells:
            continue

        def get_col(col_name: str) -> str:
            try:
                i = next(j for j, h in enumerate(headers) if col_name in h)
                return best_cells[i].get_text(strip=True) if i < len(best_cells) else ""
            except StopIteration:
                return ""

        return dict(
            hist_hs_code=get_col("hs code"),
            hist_description=get_col("description"),
            hist_valid_since=get_col("valid since"),
            hist_published=get_col("published"),
            hist_ad_valorem=get_col("ad-valorem"),
            hist_unit=get_col("unit"),
            hist_m3_unit_id=get_col("m3"),
        )

    return empty


def _extract_aladi_table(soup: Optional[BeautifulSoup]) -> List[Dict[str, str]]:
    """Extract every row from the ALADI table and return a list of dicts."""
    rows = []
    if not soup:
        return rows

    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]
        # Heuristic: ALADI tables contain 'country' or 'code'
        if "country" not in " ".join(headers) and "code" not in " ".join(headers):
            continue

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue

            def gc(col: str) -> str:
                try:
                    i = next(j for j, h in enumerate(headers) if col in h)
                    return cells[i].get_text(strip=True) if i < len(cells) else ""
                except StopIteration:
                    return ""

            entry = dict(
                aladi_country=gc("country"),
                aladi_ad_valorem=gc("ad-valorem"),
                aladi_only=gc("only"),
                aladi_published=gc("published"),
                aladi_valid_since=gc("valid since"),
                aladi_valid_until=gc("valid until"),
                aladi_code=gc("code"),
                aladi_quotas=gc("quotas"),
            )
            if any(entry.values()):
                rows.append(entry)
    return rows


def scrape_code(code: str, date_param: str, retries: int = 3) -> Dict[str, Any]:
    """Scrape one HS code — historic + aladi."""
    url = f"https://tigie.com.mx/?hs={code}&date={date_param}"
    soup = _fetch(url, retries=retries)
    historic = _parse_historic(soup)
    aladi = _extract_aladi_table(soup)
    # Sometimes ALADI is on a separate ALADI-specific URL — try fallback patterns if empty
    if not aladi:
        for alt in (
            f"https://tigie.com.mx/?hs={code}&date={date_param}&tab=aladi",
            f"https://tigie.com.mx/aladi?hs={code}&date={date_param}",
        ):
            alt_soup = _fetch(alt, retries=retries)
            aladi = _extract_aladi_table(alt_soup)
            if aladi:
                break

    return {"code": code, "historic": historic, "aladi": aladi, "status": "OK"}


# -------------------------
# Excel builder
# -------------------------
def build_excel(results: List[Dict[str, Any]]) -> bytes:
    """Construct a 3-sheet styled Excel workbook and return bytes."""
    wb = Workbook()
    C_DARK = "1a1a2e"
    C_NAVY = "0f3460"
    C_YELLOW = "e2b714"
    C_ALT = "f1f5f9"

    FILL_DARK = PatternFill("solid", start_color=C_DARK)
    FILL_NAVY = PatternFill("solid", start_color=C_NAVY)
    FILL_YELLOW = PatternFill("solid", start_color=C_YELLOW)
    FILL_ALT = PatternFill("solid", start_color=C_ALT)

    F_HDR = Font(name="Arial", bold=True, color="FFFFFF", size=10)
    F_BODY = Font(name="Arial", size=10)
    F_KEY = Font(name="Arial", bold=True, size=10, color="000000")
    F_OK = Font(name="Arial", size=10, color="16a34a")
    F_ERR = Font(name="Arial", size=10, color="dc2626")

    side = Side(style="thin", color="cbd5e1")
    border = Border(left=side, right=side, top=side, bottom=side)

    def style_header(cell, fill=FILL_DARK):
        cell.font = F_HDR
        cell.fill = fill
        cell.border = border
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    def style_body(cell, fill=None, key=False, status=None):
        cell.font = (F_KEY if key else F_BODY)
        cell.border = border
        cell.alignment = Alignment(vertical="center", wrap_text=True)
        if fill:
            cell.fill = fill
        if key:
            cell.fill = FILL_YELLOW
        if status == "OK":
            cell.font = F_OK
        if status == "ERROR":
            cell.font = F_ERR

    # Sheet 1: Historic Record
    ws1 = wb.active
    ws1.title = "Historic Record"
    headers1 = [
        "HS Code", "Hist HS Code", "Description",
        "Valid Since", "Published", "Ad-Valorem", "Unit", "M3 Unit Id",
        "ALADI Rows", "Status",
    ]
    ws1.append(headers1)
    ws1.row_dimensions[1].height = 28
    for ci in range(1, len(headers1) + 1):
        style_header(ws1.cell(1, ci))

    for ri, r in enumerate(results, start=2):
        h = r["historic"]
        row = [
            r["code"],
            h.get("hist_hs_code", ""),
            h.get("hist_description", ""),
            h.get("hist_valid_since", ""),
            h.get("hist_published", ""),
            h.get("hist_ad_valorem", ""),
            h.get("hist_unit", ""),
            h.get("hist_m3_unit_id", ""),
            len(r["aladi"]),
            r.get("status", "OK"),
        ]
        ws1.append(row)
        fill = FILL_ALT if ri % 2 == 0 else None
        for ci, _ in enumerate(row, 1):
            style_body(ws1.cell(ri, ci), fill=fill, key=(ci == 1), status=("OK" if ci == 10 and r.get("status","OK") == "OK" else None))

    widths = [15, 15, 62, 15, 15, 14, 10, 13, 12, 12]
    for ci, w in enumerate(widths, 1):
        ws1.column_dimensions[get_column_letter(ci)].width = w
    ws1.freeze_panes = "A2"
    ws1.auto_filter.ref = f"A1:{get_column_letter(len(headers1))}1"

    # Sheet 2: ALADI
    ws2 = wb.create_sheet("ALADI")
    headers2 = ["HS Code", "Country", "Ad-Valorem", "Only", "Published", "Valid Since", "Valid Until", "ALADI Code", "Quotas"]
    ws2.append(headers2)
    ws2.row_dimensions[1].height = 28
    for ci in range(1, len(headers2) + 1):
        style_header(ws2.cell(1, ci), fill=FILL_NAVY)

    rn = 2
    for r in results:
        for a in r["aladi"]:
            row = [
                r["code"],
                a.get("aladi_country", ""),
                a.get("aladi_ad_valorem", ""),
                a.get("aladi_only", ""),
                a.get("aladi_published", ""),
                a.get("aladi_valid_since", ""),
                a.get("aladi_valid_until", ""),
                a.get("aladi_code", ""),
                a.get("aladi_quotas", ""),
            ]
            ws2.append(row)
            fill = FILL_ALT if rn % 2 == 0 else None
            for ci, _ in enumerate(row, 1):
                style_body(ws2.cell(rn, ci), fill=fill, key=(ci == 1))
            rn += 1

    widths2 = [15, 20, 16, 62, 15, 15, 15, 13, 12]
    for ci, w in enumerate(widths2, 1):
        ws2.column_dimensions[get_column_letter(ci)].width = w
    ws2.freeze_panes = "A2"
    ws2.auto_filter.ref = f"A1:{get_column_letter(len(headers2))}1"

    # Sheet 3: Summary
    ws3 = wb.create_sheet("Summary")
    ws3["A1"] = "TIGIE Scraper — Run Summary"
    ws3["A1"].font = Font(name="Arial", bold=True, size=14, color=C_DARK)

    stats = [
        ("Total codes processed", len(results)),
        ("Codes with Historic data", sum(1 for r in results if r["historic"].get("hist_description"))),
        ("Codes with ALADI data", sum(1 for r in results if r["aladi"])),
        ("Total ALADI rows", sum(len(r["aladi"]) for r in results)),
        ("Errors", sum(1 for r in results if r.get("status", "OK") != "OK")),
        ("Run date", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    ]
    for i, (k, v) in enumerate(stats, start=3):
        ws3.cell(i, 1).value = k
        ws3.cell(i, 1).font = Font(name="Arial", bold=True, size=11)
        ws3.cell(i, 2).value = v
        ws3.cell(i, 2).font = Font(name="Arial", size=11)

    ws3.column_dimensions["A"].width = 34
    ws3.column_dimensions["B"].width = 25

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# -------------------------
# Streamlit UI
# -------------------------
st.set_page_config(page_title="TIGIE Scraper", page_icon="📦", layout="wide")

st.markdown(
    """
    <style>
    .header{background:linear-gradient(135deg,#1a1a2e,#0f3460);padding:18px;border-radius:12px;color:#fff;text-align:center;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="header"><h1>📦 TIGIE Tariff Scraper</h1><p>Historic Record & ALADI extraction</p></div>', unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### ⚙️ Settings")
    delay = st.slider("Delay between requests (seconds)", min_value=0.0, max_value=5.0, value=1.5, step=0.1)
    retries = st.number_input("Retries per request", min_value=1, max_value=5, value=3)
    date_param = st.text_input("Date parameter (used in URL)", value=datetime.now().strftime("%-m/%-d/%Y") if hasattr(datetime.now(), "strftime") else "1/25/2026")
    st.markdown("---")
    st.markdown("Input file must contain a `Code` column with HS codes (strings). Optional `Priority` column supported for filtering.")

uploaded = st.file_uploader("Upload Excel (has 'Code' column)", type=["xlsx", "xls"])

if uploaded is None:
    st.info("Upload a file to begin. Use small batches (100–200 rows) for reliability.")
    st.stop()

try:
    df_in = pd.read_excel(uploaded, dtype=str)
except Exception as e:
    st.error(f"Failed to read Excel: {e}")
    st.stop()

df_in.columns = [c.strip() for c in df_in.columns]

if "Code" not in df_in.columns:
    st.error("No `Code` column found in uploaded file.")
    st.stop()

df_in["Code"] = df_in["Code"].astype(str).str.strip()
total_codes = len(df_in)

# Optional priority filter
if "Priority" in df_in.columns:
    priorities = sorted(df_in["Priority"].dropna().unique().tolist())
    sel = st.multiselect("Filter by Priority", priorities, default=priorities)
    if sel:
        df_in = df_in[df_in["Priority"].isin(sel)].reset_index(drop=True)

st.markdown(f"**Total codes in file:** {total_codes:,} — **Selected:** {len(df_in):,}")

# Row range selector
c1, c2 = st.columns(2)
with c1:
    start_row = st.number_input("Start row (1-indexed)", min_value=1, max_value=max(1, len(df_in)), value=1)
with c2:
    end_row = st.number_input("End row", min_value=1, max_value=max(1, len(df_in)), value=min(100, len(df_in)))

df_batch = df_in.iloc[start_row - 1 : end_row].reset_index(drop=True)
st.markdown(f"Processing rows {start_row} → {end_row} (count: {len(df_batch)})")

if st.button("🚀 Start Scraping", type="primary"):
    codes = df_batch["Code"].astype(str).tolist()
    n = len(codes)
    prog = st.progress(0)
    statusbox = st.empty()
    results = []
    done = n_hist = n_aladi = n_err = 0

    for i, code in enumerate(codes, start=1):
        statusbox.info(f"⏳ ({i}/{n}) Scraping {code} …")
        try:
            data = scrape_code(code, date_param, retries=int(retries))
            if data["historic"].get("hist_description"):
                n_hist += 1
            if data["aladi"]:
                n_aladi += 1
        except Exception as exc:
            data = {"code": code, "historic": {}, "aladi": [], "status": f"ERROR: {str(exc)[:120]}"}
            n_err += 1

        results.append(data)
        done += 1
        prog.progress(done / n)
        time.sleep(float(delay))

    statusbox.success("✅ Scraping finished!")

    # Build excel and provide download button
    excel_bytes = build_excel(results)
    fname = f"tigie_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    st.download_button(label="⬇️ Download Excel", data=excel_bytes, file_name=fname, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # Summaries / small table
    st.markdown("### Run summary")
    st.write({
        "Total processed": len(results),
        "Historic rows found": n_hist,
        "ALADI rows found (codes)": n_aladi,
        "Errors": n_err,
    })

    # Small preview of results
    preview = []
    for r in results[:20]:
        preview.append({
            "Code": r["code"],
            "Description": (r["historic"].get("hist_description") or "")[:80],
            "Published": r["historic"].get("hist_published", ""),
            "ALADI rows": len(r["aladi"]),
            "Status": r.get("status", "OK"),
        })
    st.dataframe(pd.DataFrame(preview), use_container_width=True)
else:
    st.info("Configure settings on the left and click Start Scraping.")
