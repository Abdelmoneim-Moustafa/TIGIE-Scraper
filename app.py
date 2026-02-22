"""
╔══════════════════════════════════════════════════════════════════╗
║        TIGIE.com.mx Tariff Scraper — v3                          ║
╚══════════════════════════════════════════════════════════════════╝

HOW TO RUN:
  pip install streamlit requests beautifulsoup4 lxml openpyxl pandas
  streamlit run tigie_scraper.py

COMMAND-LINE:
  python tigie_scraper.py --cli codes.xlsx
  python tigie_scraper.py --cli codes.xlsx --limit 50
  python tigie_scraper.py --cli codes.xlsx --debug   <- shows raw HTML to diagnose blank data

ABOUT THE DATE PARAMETER:
  The URL uses date=M/D/YYYY (US format), e.g. 1/25/2026 = January 25 2026.
  This is the TARIFF LOOKUP DATE — it controls which published tariff version is shown.
  Your Excel already has the correct links with this date baked in, so this setting
  is IGNORED when a Link column is present. You only need to change it if you want
  a different snapshot date AND your Excel has no Link column.

IF YOU GET "Could not reach tigie.com.mx":
  The site may only be accessible from Mexico or may block automated requests.
  Solutions: (1) Use a Mexican VPN, (2) run --debug to see what the server returns,
  (3) open the URL in your browser to confirm the site is up.
"""

import sys, io, json, os, time, argparse
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ═══════════════════════════════════════════════════════════════════
#  HTTP SESSION
# ═══════════════════════════════════════════════════════════════════

_BASE_URL = "https://tigie.com.mx"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

_session = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(_HEADERS)
        # Warm up: visit homepage to get cookies (helps avoid bot detection)
        try:
            _session.get(_BASE_URL, timeout=20)
            time.sleep(1.0)
        except Exception:
            pass
    return _session


def _fetch(url: str, retries: int = 3, debug: bool = False):
    """GET a URL -> BeautifulSoup or None."""
    session = _get_session()
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=30)
            if r.status_code in (403, 429, 503):
                if debug:
                    print(f"  HTTP {r.status_code} — site may be blocking. Try a Mexican VPN.")
                time.sleep(5 * (attempt + 1))
                continue
            r.raise_for_status()
            if debug:
                print(f"\n{'─'*60}")
                print(f"URL: {url}")
                print(f"Status: {r.status_code}  Length: {len(r.text)} chars")
                print("First 4000 chars:")
                print(r.text[:4000])
                print(f"{'─'*60}\n")
            return BeautifulSoup(r.text, "lxml")
        except requests.exceptions.ConnectionError:
            if attempt == retries - 1:
                if debug:
                    print(f"  Connection failed: {url}")
                    print("  -> Site unreachable. Check internet or use a Mexican VPN.")
            time.sleep(3)
        except Exception as exc:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


# ═══════════════════════════════════════════════════════════════════
#  PARSING
# ═══════════════════════════════════════════════════════════════════

def _parse_historic(soup, debug=False) -> dict:
    empty = dict(hist_hs_code="", hist_description="", hist_valid_since="",
                 hist_published="", hist_ad_valorem="", hist_unit="", hist_m3_unit_id="")
    if not soup:
        return empty

    tables = soup.find_all("table")
    if debug:
        print(f"  Tables found: {len(tables)}")
        for i, t in enumerate(tables):
            hrow = t.find("tr")
            if hrow:
                hdrs = [th.get_text(strip=True) for th in hrow.find_all(["th","td"])]
                print(f"  Table {i}: {hdrs}")

    for table in tables:
        hrow = table.find("tr")
        if not hrow:
            continue
        hdrs = [th.get_text(strip=True).lower() for th in hrow.find_all(["th","td"])]
        has_adval = any("ad" in h and "valor" in h for h in hdrs)
        has_date  = any(k in h for h in hdrs for k in ["publish","public","valid","vigent"])
        has_desc  = any("desc" in h for h in hdrs)
        if not (has_adval and (has_date or has_desc)):
            continue

        data_rows = table.find_all("tr")[1:]
        best_cells, best_date = None, None
        for row in data_rows:
            cells = row.find_all(["td","th"])
            if len(cells) < 3:
                continue
            pv = ""
            for i, h in enumerate(hdrs):
                if ("publish" in h or "public" in h) and i < len(cells):
                    pv = cells[i].get_text(strip=True)
                    break
            d = None
            for fmt in ["%b/%d/%Y","%m/%d/%Y","%d/%m/%Y","%Y-%m-%d","%B/%d/%Y","%d-%m-%Y"]:
                try:
                    d = datetime.strptime(pv, fmt); break
                except ValueError:
                    pass
            if d and (best_date is None or d > best_date):
                best_date, best_cells = d, cells
            elif best_cells is None:
                best_cells = cells

        if best_cells is None and data_rows:
            best_cells = data_rows[-1].find_all(["td","th"])
        if not best_cells:
            continue

        def gc(*names):
            for name in names:
                try:
                    i = next(j for j, h in enumerate(hdrs) if name in h)
                    return best_cells[i].get_text(strip=True) if i < len(best_cells) else ""
                except StopIteration:
                    pass
            return ""

        return dict(
            hist_hs_code     = gc("hs code","código","fraccion"),
            hist_description = gc("description","descripción","descripcion"),
            hist_valid_since = gc("valid since","vigente desde","vigencia"),
            hist_published   = gc("published","publicado","publicación"),
            hist_ad_valorem  = gc("ad-valorem","ad valorem","arancel"),
            hist_unit        = gc("unit","unidad"),
            hist_m3_unit_id  = gc("m3","unit id"),
        )
    return empty


def _extract_aladi(soup) -> list:
    rows = []
    if not soup:
        return rows
    for table in soup.find_all("table"):
        hrow = table.find("tr")
        if not hrow:
            continue
        hdrs = [th.get_text(strip=True).lower() for th in hrow.find_all(["th","td"])]
        if not any("country" in h or "país" in h or "pais" in h for h in hdrs):
            continue
        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td","th"])
            if len(cells) < 3:
                continue
            def gc(*names):
                for name in names:
                    try:
                        i = next(j for j, h in enumerate(hdrs) if name in h)
                        return cells[i].get_text(strip=True) if i < len(cells) else ""
                    except StopIteration:
                        pass
                return ""
            entry = dict(
                aladi_country     = gc("country","país","pais"),
                aladi_ad_valorem  = gc("ad-valorem","ad valorem"),
                aladi_only        = gc("only","únicamente","solo"),
                aladi_published   = gc("published","publicado"),
                aladi_valid_since = gc("valid since","vigente desde"),
                aladi_valid_until = gc("valid until","vigente hasta"),
                aladi_code        = gc("code","código","acuerdo"),
                aladi_quotas      = gc("quotas","cuotas"),
            )
            if any(entry.values()):
                rows.append(entry)
    return rows


# ═══════════════════════════════════════════════════════════════════
#  MAIN SCRAPE
# ═══════════════════════════════════════════════════════════════════

def scrape_code(code: str, link: str = None, date_param: str = "1/25/2026",
                retries: int = 3, debug: bool = False) -> dict:
    """
    Scrape one HS code.
    Uses 'link' from Excel if provided (recommended — date already embedded).
    Falls back to building URL from code + date_param.
    """
    url = link if link else f"{_BASE_URL}/?hs={code}&date={date_param}"
    soup = _fetch(url, retries, debug=debug)
    historic = _parse_historic(soup, debug=debug)

    aladi = []
    if soup:
        aladi = _extract_aladi(soup)
    if not aladi:
        aladi_url = (url + "&tab=aladi") if "?" in url else (url + "?tab=aladi")
        aladi_soup = _fetch(aladi_url, retries)
        if aladi_soup:
            aladi = _extract_aladi(aladi_soup)

    if soup is None:
        status = "ERROR: Could not reach tigie.com.mx — check internet / use Mexican VPN"
    elif not historic.get("hist_description") and not aladi:
        status = "EMPTY: Page loaded but no table data found"
    else:
        status = "OK"

    return {"code": code, "link": url, "historic": historic, "aladi": aladi, "status": status}


# ═══════════════════════════════════════════════════════════════════
#  PROGRESS AUTO-SAVE / RESUME
# ═══════════════════════════════════════════════════════════════════

def _load_progress(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"  Resumed: {len(data)} codes already done from {path}")
            return data
        except Exception:
            pass
    return {}


def _save_progress(path: str, done: dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(done, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ═══════════════════════════════════════════════════════════════════
#  EXCEL BUILDER
# ═══════════════════════════════════════════════════════════════════

def build_excel(results: list) -> bytes:
    wb = Workbook()
    FILL_DARK   = PatternFill("solid", start_color="1a1a2e")
    FILL_NAVY   = PatternFill("solid", start_color="0f3460")
    FILL_YELLOW = PatternFill("solid", start_color="e2b714")
    FILL_ALT    = PatternFill("solid", start_color="f1f5f9")
    _side   = Side(style="thin", color="cbd5e1")
    _border = Border(left=_side, right=_side, top=_side, bottom=_side)

    def sh(cell, fill=FILL_DARK):
        cell.font = Font(name="Arial", bold=True, color="FFFFFF", size=10)
        cell.fill = fill; cell.border = _border
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    def sb(cell, fill=None, key=False, st=None):
        color = ("000000" if key else
                 ("16a34a" if st == "OK" else
                  ("dc2626" if st and "ERROR" in st else
                   ("d97706" if st and "EMPTY" in st else "000000"))))
        cell.font = Font(name="Arial", bold=key, size=10, color=color)
        cell.border = _border
        cell.alignment = Alignment(vertical="center", wrap_text=True)
        if key: cell.fill = FILL_YELLOW
        elif fill: cell.fill = fill

    # Sheet 1 — Historic Record
    ws1 = wb.active
    ws1.title = "Historic Record"
    H1 = ["HS Code","Description","Valid Since","Published","Ad-Valorem","Unit","ALADI Rows","Status","Source URL"]
    ws1.append(H1)
    ws1.row_dimensions[1].height = 30
    for ci in range(1, len(H1)+1): sh(ws1.cell(1, ci))

    for ri, r in enumerate(results, 2):
        h  = r["historic"]
        st = r.get("status","OK")
        row = [r["code"], h.get("hist_description",""), h.get("hist_valid_since",""),
               h.get("hist_published",""), h.get("hist_ad_valorem",""), h.get("hist_unit",""),
               len(r["aladi"]), st, r.get("link","")]
        ws1.append(row)
        fill = FILL_ALT if ri % 2 == 0 else None
        for ci, _ in enumerate(row, 1):
            sb(ws1.cell(ri, ci), fill=fill, key=(ci==1), st=(st if ci==8 else None))

    for ci, w in enumerate([15,62,15,15,14,10,12,25,55], 1):
        ws1.column_dimensions[get_column_letter(ci)].width = w
    ws1.freeze_panes = "A2"
    ws1.auto_filter.ref = f"A1:{get_column_letter(len(H1))}1"

    # Sheet 2 — ALADI
    ws2 = wb.create_sheet("ALADI")
    H2 = ["HS Code","Country","Ad-Valorem","Only","Published","Valid Since","Valid Until","ALADI Code","Quotas"]
    ws2.append(H2)
    ws2.row_dimensions[1].height = 30
    for ci in range(1, len(H2)+1): sh(ws2.cell(1, ci), fill=FILL_NAVY)

    rn = 2
    for r in results:
        for a in r["aladi"]:
            row = [r["code"], a.get("aladi_country",""), a.get("aladi_ad_valorem",""),
                   a.get("aladi_only",""), a.get("aladi_published",""),
                   a.get("aladi_valid_since",""), a.get("aladi_valid_until",""),
                   a.get("aladi_code",""), a.get("aladi_quotas","")]
            ws2.append(row)
            fill = FILL_ALT if rn % 2 == 0 else None
            for ci, _ in enumerate(row, 1):
                sb(ws2.cell(rn, ci), fill=fill, key=(ci==1))
            rn += 1

    for ci, w in enumerate([15,20,16,62,15,15,15,13,12], 1):
        ws2.column_dimensions[get_column_letter(ci)].width = w
    ws2.freeze_panes = "A2"
    ws2.auto_filter.ref = f"A1:{get_column_letter(len(H2))}1"

    # Sheet 3 — Summary
    ws3 = wb.create_sheet("Summary")
    ws3["A1"] = "TIGIE Scraper — Run Summary"
    ws3["A1"].font = Font(name="Arial", bold=True, size=14)
    stats = [
        ("Total codes processed",     len(results)),
        ("OK (data found)",           sum(1 for r in results if r.get("status")=="OK")),
        ("Empty (loaded, no data)",   sum(1 for r in results if "EMPTY" in r.get("status",""))),
        ("Errors (unreachable)",      sum(1 for r in results if "ERROR" in r.get("status",""))),
        ("Codes with Historic data",  sum(1 for r in results if r["historic"].get("hist_description"))),
        ("Codes with ALADI data",     sum(1 for r in results if r["aladi"])),
        ("Total ALADI rows",          sum(len(r["aladi"]) for r in results)),
        ("Run date",                  datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    ]
    for i, (k, v) in enumerate(stats, 3):
        ws3.cell(i,1).value = k; ws3.cell(i,1).font = Font(name="Arial", bold=True, size=11)
        ws3.cell(i,2).value = v; ws3.cell(i,2).font = Font(name="Arial", size=11)
    ws3.column_dimensions["A"].width = 35
    ws3.column_dimensions["B"].width = 25

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

def _run_cli():
    p = argparse.ArgumentParser(description="TIGIE Scraper CLI")
    p.add_argument("--cli",       required=True, metavar="EXCEL_FILE")
    p.add_argument("--date",      default="1/25/2026",
                   help="Tariff date M/D/YYYY. Ignored if Excel has a Link column.")
    p.add_argument("--delay",     type=float, default=1.5)
    p.add_argument("--retries",   type=int,   default=3)
    p.add_argument("--start",     type=int,   default=1)
    p.add_argument("--limit",     type=int,   default=None)
    p.add_argument("--debug",     action="store_true",
                   help="Print raw HTML for each code — helps diagnose blank data")
    p.add_argument("--no-resume", action="store_true", help="Ignore saved progress")
    args = p.parse_args()

    df = pd.read_excel(args.cli, dtype={"Code": str})
    df.columns = [c.strip() for c in df.columns]
    if "Code" not in df.columns:
        sys.exit("No 'Code' column found.")
    df["Code"] = df["Code"].astype(str).str.strip()
    has_links = "Link" in df.columns

    rows = df.iloc[args.start - 1:]
    if args.limit:
        rows = rows.iloc[:args.limit]

    tag       = os.path.splitext(os.path.basename(args.cli))[0]
    prog_path = f"tigie_progress_{tag}.json"
    done_map  = {} if args.no_resume else _load_progress(prog_path)
    all_codes = rows["Code"].tolist()
    all_links = rows["Link"].tolist() if has_links else [""] * len(all_codes)

    remaining = [(c, l) for c, l in zip(all_codes, all_links) if c not in done_map]
    print(f"\n  {len(all_codes)} total | {len(done_map)} already done | {len(remaining)} to scrape")
    if has_links:
        print("  Using pre-built Links from Excel (date already embedded)\n")
    else:
        print(f"  No Link column — building URLs with date={args.date}\n")

    for i, (code, link) in enumerate(remaining, 1):
        data = scrape_code(code, link or None, args.date, args.retries, args.debug)
        done_map[code] = data
        _save_progress(prog_path, done_map)
        h  = data["historic"].get("hist_description","")
        st = data.get("status","OK")
        icon = "OK" if st == "OK" else ("EMPTY" if "EMPTY" in st else "ERROR")
        print(f"  [{i:>5}/{len(remaining)}]  {code}  [{icon}]  "
              f"hist={'Y' if h else 'N'}  aladi={len(data['aladi'])}")
        time.sleep(args.delay)

    results = [done_map[c] for c in all_codes if c in done_map]
    out = f"tigie_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    with open(out, "wb") as f:
        f.write(build_excel(results))

    n_ok    = sum(1 for r in results if r.get("status")=="OK")
    n_empty = sum(1 for r in results if "EMPTY" in r.get("status",""))
    n_err   = sum(1 for r in results if "ERROR" in r.get("status",""))
    print(f"\n  Done: {len(results)} codes | {n_ok} OK | {n_empty} empty | {n_err} errors")
    print(f"  Saved: {out}")
    print(f"  Progress file: {prog_path}")
    if n_err == len(results):
        print("\n  ALL FAILED. Solutions:")
        print("  1. Use a Mexican VPN and retry")
        print("  2. Run with --debug to see what the server returns")
        print("  3. Open the URL in your browser to check the site is up\n")


# ═══════════════════════════════════════════════════════════════════
#  STREAMLIT UI
# ═══════════════════════════════════════════════════════════════════

def _run_streamlit():
    import streamlit as st

    st.set_page_config(page_title="TIGIE Scraper", page_icon="📦",
                       layout="wide", initial_sidebar_state="expanded")

    st.markdown("""<style>
    .header-box{background:linear-gradient(135deg,#1a1a2e,#0f3460);
        padding:2rem 2.5rem;border-radius:14px;margin-bottom:1.5rem;text-align:center;}
    .header-box h1{color:#e2b714;margin:0;font-size:2.2rem;}
    .header-box p{color:#a0aec0;margin:.5rem 0 0;}
    .kpi{background:#1e293b;border:1px solid #334155;border-radius:10px;
         padding:1.1rem;text-align:center;}
    .kpi .v{font-size:2rem;font-weight:700;color:#e2b714;}
    .kpi .l{font-size:.8rem;color:#94a3b8;margin-top:.2rem;}
    .tip{background:#0c1a27;border-left:4px solid #e2b714;padding:.8rem 1rem;
         border-radius:0 8px 8px 0;font-size:.9rem;color:#cbd5e1;margin:.6rem 0;}
    .warn{background:#2d1f00;border-left:4px solid #f59e0b;padding:.8rem 1rem;
          border-radius:0 8px 8px 0;font-size:.9rem;color:#fcd34d;margin:.6rem 0;}
    </style>""", unsafe_allow_html=True)

    st.markdown("""<div class="header-box">
      <h1>📦 TIGIE Tariff Scraper</h1>
      <p>Historic Records &amp; ALADI · Auto-saves progress after every code · Safe to switch tabs</p>
    </div>""", unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### ⚙️ Settings")
        delay       = st.slider("Delay between requests (sec)", 0.5, 5.0, 1.5, 0.5)
        max_retries = st.number_input("Max retries per code", 1, 5, 3)
        date_param  = st.text_input("Tariff date (M/D/YYYY)", value="1/25/2026",
            help="Only used if your Excel has NO Link column. Format: Month/Day/Year (US). "
                 "1/25/2026 = January 25 2026. If your Excel has pre-built links, this is ignored.")
        st.caption("The date controls which tariff snapshot is shown on tigie.com.mx. "
                   "Your uploaded Excel already has links with the date embedded — "
                   "so this field is only a fallback.")
        st.markdown("---")
        st.markdown("**💾 Auto-save enabled**\n\nProgress saved after every code. "
                    "Switch tabs, sleep, or reload — your work is safe.")
        st.markdown("---")
        st.warning("If all codes fail, the site may block non-Mexican IPs. "
                   "Try a Mexican VPN.")

    st.markdown("### 📁 Upload your codes file")
    uploaded = st.file_uploader("Excel with Code column (Link column used automatically if present)",
                                type=["xlsx","xls"])
    if not uploaded:
        st.info("Upload your codes Excel. Must have a `Code` column. "
                "If it has a `Link` column those URLs are used directly.")
        return

    df_in = pd.read_excel(uploaded, dtype={"Code": str})
    df_in.columns = [c.strip() for c in df_in.columns]
    if "Code" not in df_in.columns:
        st.error("No `Code` column found.")
        return

    df_in["Code"] = df_in["Code"].astype(str).str.strip()
    has_links = "Link" in df_in.columns
    total = len(df_in)

    if has_links:
        st.success("✅ Link column detected — pre-built URLs will be used (date already embedded in each link)")
    else:
        st.info(f"No Link column — URLs will be built using date={date_param}")

    if "Priority" in df_in.columns:
        priorities = sorted(df_in["Priority"].dropna().unique().tolist())
        sel = st.multiselect("Filter by Priority (empty = all)", priorities, default=priorities)
        if sel:
            df_in = df_in[df_in["Priority"].isin(sel)]

    c1, c2 = st.columns(2)
    with c1:
        start_i = st.number_input("Start from row #", 1, len(df_in), 1)
    with c2:
        end_i = st.number_input("End at row #", 1, len(df_in), min(100, len(df_in)))

    df_batch = df_in.iloc[start_i - 1: end_i].reset_index(drop=True)
    codes = df_batch["Code"].tolist()
    links = df_batch["Link"].tolist() if has_links else [""] * len(codes)

    k1, k2, k3 = st.columns(3)
    for col, val, lbl in [(k1,total,"Total codes in file"),
                           (k2,len(df_in),"After priority filter"),
                           (k3,len(df_batch),"In current batch")]:
        col.markdown(f'<div class="kpi"><div class="v">{val:,}</div>'
                     f'<div class="l">{lbl}</div></div>', unsafe_allow_html=True)

    st.markdown('<div class="tip">⚡ Results auto-saved to disk after every code. '
                'Switch tabs or sleep — no data lost. Reload to resume.</div>',
                unsafe_allow_html=True)

    with st.expander("Preview batch"):
        st.dataframe(df_batch.head(20), use_container_width=True)

    # Session state
    if "results_map" not in st.session_state:
        st.session_state.results_map = {}

    prog_path = "tigie_streamlit_progress.json"
    if not st.session_state.results_map and os.path.exists(prog_path):
        st.session_state.results_map = _load_progress(prog_path)

    saved = [c for c in codes if c in st.session_state.results_map]
    if saved:
        st.markdown(f'<div class="warn">💾 Saved progress: '
                    f'<b>{len(saved)}/{len(codes)}</b> codes already done. '
                    f'Start Scraping will resume automatically.</div>',
                    unsafe_allow_html=True)
        if st.button("🗑️ Clear progress and restart"):
            st.session_state.results_map = {}
            if os.path.exists(prog_path): os.remove(prog_path)
            st.rerun()

    col1, col2 = st.columns(2)
    with col1:
        start_btn = st.button("🚀 Start Scraping", type="primary", use_container_width=True)
    with col2:
        if st.button("⏹️ Stop — Download what we have", use_container_width=True):
            partial = [st.session_state.results_map[c] for c in codes
                       if c in st.session_state.results_map]
            if partial:
                st.download_button("⬇️ Download Partial Excel",
                    data=build_excel(partial),
                    file_name=f"tigie_partial_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    if not start_btn:
        _show_results(st, codes, st.session_state.results_map)
        return

    remaining = [(c, l) for c, l in zip(codes, links)
                 if c not in st.session_state.results_map]

    prog  = st.progress(0)
    stxt  = st.empty()
    ltbl  = st.empty()
    m1, m2, m3, m4 = st.columns(4)
    mm = {k: c.empty() for k, c in zip(["done","hist","aladi","err"],[m1,m2,m3,m4])}
    n_done = len(codes) - len(remaining)

    for i, (code, link) in enumerate(remaining):
        stxt.markdown(f"⏳ Scraping **{code}** ({n_done+i+1}/{len(codes)})…")
        try:
            data = scrape_code(code, link or None, date_param, max_retries)
        except Exception as exc:
            data = {"code": code, "link": link,
                    "historic": {k: "" for k in ["hist_hs_code","hist_description",
                                 "hist_valid_since","hist_published","hist_ad_valorem",
                                 "hist_unit","hist_m3_unit_id"]},
                    "aladi": [], "status": f"ERROR: {exc}"}

        st.session_state.results_map[code] = data
        _save_progress(prog_path, st.session_state.results_map)

        done_so_far = n_done + i + 1
        prog.progress(done_so_far / len(codes))

        all_r = [st.session_state.results_map[c] for c in codes
                 if c in st.session_state.results_map]
        mm["done"].metric("✅ Done",    done_so_far)
        mm["hist"].metric("📜 Historic", sum(1 for r in all_r if r["historic"].get("hist_description")))
        mm["aladi"].metric("🌎 ALADI",   sum(1 for r in all_r if r["aladi"]))
        mm["err"].metric("⚠️ Issues",   sum(1 for r in all_r if r.get("status","OK")!="OK"))

        ltbl.dataframe(pd.DataFrame([{
            "Code":       r["code"],
            "Description":r["historic"].get("hist_description","")[:50],
            "Published":  r["historic"].get("hist_published",""),
            "Ad-valorem": r["historic"].get("hist_ad_valorem",""),
            "ALADI rows": len(r["aladi"]),
            "Status":     r.get("status","OK")[:40],
        } for r in all_r[-5:]]), use_container_width=True)

        time.sleep(delay)

    stxt.markdown("✅ **Scraping complete!**")
    _show_results(st, codes, st.session_state.results_map)


def _show_results(st, codes, results_map):
    results = [results_map[c] for c in codes if c in results_map]
    if not results:
        return
    n_ok    = sum(1 for r in results if r.get("status")=="OK")
    n_empty = sum(1 for r in results if "EMPTY" in r.get("status",""))
    n_err   = sum(1 for r in results if "ERROR" in r.get("status",""))
    st.success(f"🎉 {len(results)} codes | {n_ok} OK | {n_empty} empty | {n_err} errors")

    if n_err == len(results):
        st.error("❌ All codes failed — tigie.com.mx is unreachable.\n\n"
                 "Try: (1) Mexican VPN, (2) run CLI with --debug, "
                 "(3) open the URL in your browser to check the site is up.")

    st.download_button("⬇️ Download Excel",
        data=build_excel(results),
        file_name=f"tigie_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary", use_container_width=True)

    df_res = pd.DataFrame([{"Code":r["code"],
        "Has Historic":bool(r["historic"].get("hist_description")),
        "ALADI Rows":len(r["aladi"]), "Status":r.get("status","OK")} for r in results])
    ca, cb = st.columns(2)
    with ca:
        st.markdown("**Coverage**")
        st.bar_chart(pd.DataFrame({"Count":{
            "Has Historic":int(df_res["Has Historic"].sum()),
            "Has ALADI":int((df_res["ALADI Rows"]>0).sum()),
            "Empty/Error":int(((~df_res["Has Historic"])&(df_res["ALADI Rows"]==0)).sum()),
        }}))
    with cb:
        st.markdown("**Top 10 by ALADI rows**")
        st.dataframe(df_res.nlargest(10,"ALADI Rows")[["Code","ALADI Rows"]],
                     use_container_width=True)


# ═══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if "--cli" in sys.argv:
        _run_cli()

if "--cli" not in sys.argv:
    try:
        _run_streamlit()
    except Exception:
        pass
