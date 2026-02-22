# app_playwright.py
# Streamlit + Playwright scraper for tigie.com.mx
# - Upload Excel with columns: "Code" or "HS Code" and optional "Link"
# - Scrape Historic Record (last published date, ad-valorem, description)
# - Scrape ALADI tables (all rows)
# - Produce a single Excel: one row per code + sheet "ALADI" for detailed rows
# - Progress auto-saved to resume.json
#
# Usage:
#   pip install -r requirements.txt
#   playwright install
#   streamlit run app_playwright.py

import streamlit as st
import pandas as pd
import json, time, os, io, traceback
from datetime import datetime
from bs4 import BeautifulSoup

# Playwright (sync)
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ---------- Helpers: parsing functions ----------

def parse_historic_from_soup(soup):
    """Return dict with keys: last_published, ad_valorem, description (strings or empty)."""
    res = {"last_published": "", "ad_valorem": "", "description": ""}
    if not soup:
        return res

    # Strategy: find table that contains header cells like 'ad', 'valor', 'published', 'public', 'description'
    tables = soup.find_all("table")
    for table in tables:
        hdrs = []
        first_row = table.find("tr")
        if not first_row:
            continue
        # get headers (th/td)
        hdrs = [th.get_text(strip=True).lower() for th in first_row.find_all(["th","td"])]
        if not hdrs:
            continue
        hdr_text = " ".join(hdrs)
        if ("ad" in hdr_text and "valor" in hdr_text) or ("public" in hdr_text) or ("publicación" in hdr_text) or ("descripción" in hdr_text) or ("description" in hdr_text):
            # parse first data row (or best row)
            data_rows = table.find_all("tr")[1:]
            if not data_rows:
                continue
            # choose the latest by date if we can parse dates
            best_row = data_rows[-1]
            cells = best_row.find_all(["td","th"])
            # fallback mapping: try find index of likely headers
            def find_index(key_options):
                for k in key_options:
                    for i,h in enumerate(hdrs):
                        if k in h:
                            return i
                return None
            idx_pub = find_index(["publish","public","publicación","publicado","fecha"])
            idx_ad  = find_index(["ad-valorem","ad valorem","ad","arancel","arancelaria"])
            idx_desc = find_index(["description","descripcion","descripci","descripcion"])
            # get values if indexes found, else attempt heuristics
            try:
                if idx_pub is not None and idx_pub < len(cells):
                    res["last_published"] = cells[idx_pub].get_text(strip=True)
                if idx_ad is not None and idx_ad < len(cells):
                    res["ad_valorem"] = cells[idx_ad].get_text(strip=True)
                if idx_desc is not None and idx_desc < len(cells):
                    res["description"] = cells[idx_desc].get_text(strip=True)
            except Exception:
                pass
            # If still empty, fallback: concat all cells
            if not any(res.values()):
                txt = " | ".join([c.get_text(strip=True) for c in cells])
                res["description"] = txt
            return res
    # If no suitable table found, try to find direct labels in the page
    text = soup.get_text(" ", strip=True).lower()
    # heuristics
    # look for "ad-valorem" nearby
    if "ad-valorem" in text or "ad valorem" in text or "arancel" in text:
        res["ad_valorem"] = "found-in-text"
    return res

def extract_aladi_from_soup(soup):
    """Return list of dicts for ALADI rows. Each dict with typical keys: country, ad_valorem, published, valid_since, valid_until, code, quotas"""
    rows = []
    if not soup:
        return rows
    tables = soup.find_all("table")
    for table in tables:
        # check header for country or pais
        first_row = table.find("tr")
        if not first_row:
            continue
        hdrs = [th.get_text(strip=True).lower() for th in first_row.find_all(["th","td"])]
        hdr_text = " ".join(hdrs)
        if not ("pais" in hdr_text or "país" in hdr_text or "country" in hdr_text):
            continue
        # parse data rows
        data_rows = table.find_all("tr")[1:]
        for r in data_rows:
            cells = r.find_all(["td","th"])
            if len(cells) < 1:
                continue
            # map heuristics
            def gc(options):
                for opt in options:
                    for i,h in enumerate(hdrs):
                        if opt in h:
                            return cells[i].get_text(strip=True) if i < len(cells) else ""
                return ""
            entry = {
                "country": gc(["country","pais","país"]),
                "ad_valorem": gc(["ad-valorem","ad valorem","arancel","arancelaria"]),
                "only": gc(["only","solo","únicamente","único"]),
                "published": gc(["published","publicado","publicación","fecha"]),
                "valid_since": gc(["valid since","vigente desde","vigencia"]),
                "valid_until": gc(["valid until","vigente hasta","hasta"]),
                "code": gc(["code","código","acuerdo"]),
                "quotas": gc(["quotas","cuotas"])
            }
            rows.append(entry)
    return rows

# ---------- Playwright scraping function ----------

def scrape_with_playwright(playwright, url, wait_for_selector="table", click_try_texts=None, timeout=15000):
    """
    Use an existing playwright instance.
    - url: page url
    - click_try_texts: list of texts to click before parsing (e.g., ["ALADI","ALADI / PREFERENCIAS"])
    Returns (html_content, debug_log)
    """
    browser = playwright.chromium.launch(headless=True, args=["--no-sandbox"])  # chromium for widest compatibility
    context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36", locale="es-MX")
    page = context.new_page()
    debug_log = {"url": url, "events": []}
    try:
        page.goto(url, wait_until="networkidle", timeout=timeout)
        debug_log["events"].append("loaded")
        # try clicking tabs to reveal historic/aladi sections (language variations)
        tabs_to_try = click_try_texts or ["Historic Record", "Registro histórico", "ALADI", "ALADI / PREFERENCIAS", "ALADI / Preferencias"]
        # attempt clicking each text-based tab if present
        for t in tabs_to_try:
            try:
                loc = page.locator(f"text=\"{t}\"")
                if loc.count() > 0:
                    loc.first.click(timeout=2000)
                    debug_log["events"].append(f"clicked {t}")
                    # wait a short time for content to render
                    page.wait_for_load_state("networkidle", timeout=3000)
            except PlaywrightTimeout:
                pass
            except Exception:
                # ignore click failures
                pass

        # wait for at least one table (historic or aladi)
        try:
            page.wait_for_selector(wait_for_selector, timeout=timeout)
            debug_log["events"].append("table-found")
        except PlaywrightTimeout:
            debug_log["events"].append("no-table-found-timeout")

        html = page.content()
        return html, debug_log
    finally:
        try:
            page.close()
            context.close()
            browser.close()
        except Exception:
            pass

# ---------- Streamlit UI and orchestration ----------

st.set_page_config(page_title="TIGIE Scraper (Playwright)", layout="wide")
st.title("TIGIE Tariff Scraper — Playwright (Improved)")

st.markdown("""
Simpler instructions for end users:
- قم بتحميل ملف Excel يحتوي على عمود `Code` (أو `HS Code`) وعمود `Link` اختياري.
- اضغط **Start** لبدء السحب. ستتلقى ملف Excel واحد بعد الانتهاء.
- إن كان الموقع يقيد الوصول حسب الموقع الجغرافي، استخدم VPN / Proxy (Mexico IP).
""")

with st.sidebar:
    st.header("Settings")
    delay = st.number_input("Delay between requests (seconds)", min_value=0.0, max_value=10.0, value=1.2, step=0.1)
    timeout = st.number_input("Page load timeout (ms)", min_value=5000, max_value=60000, value=20000, step=1000)
    max_codes = st.number_input("Max codes to process (0 = all)", min_value=0, value=0, step=1)
    proxy = st.text_input("Optional Proxy (http://user:pass@host:port)", value="")
    st.caption("Use proxy if you need to route requests through Mexico.")

uploaded = st.file_uploader("Upload Excel (must include 'Code' or 'HS Code' column, 'Link' optional)", type=["xlsx","xls"])
if not uploaded:
    st.info("Upload your Excel file to start.")
    st.stop()

# read dataframe
try:
    df = pd.read_excel(uploaded, dtype=str)
except Exception as e:
    st.error(f"Cannot read Excel: {e}")
    st.stop()

# normalize columns
cols_lower = [c.strip().lower() for c in df.columns]
if "code" not in cols_lower and "hs code" not in cols_lower and "hs_code" not in cols_lower:
    st.error("Excel must have a 'Code' or 'HS Code' column.")
    st.stop()

# find code column
if "code" in cols_lower:
    code_col = df.columns[cols_lower.index("code")]
elif "hs code" in cols_lower:
    code_col = df.columns[cols_lower.index("hs code")]
else:
    code_col = df.columns[cols_lower.index("hs_code")]

link_col = None
if "link" in cols_lower:
    link_col = df.columns[cols_lower.index("link")]

# optional limit
if max_codes > 0:
    df = df.head(int(max_codes))

# replace NaN with empty string
df = df.fillna("")

st.markdown(f"Found **{len(df)}** rows. First codes preview:")
st.dataframe(df[[code_col] + ([link_col] if link_col else [])].head(10))

# progress storage
progress_path = "playwright_tigie_progress.json"
if os.path.exists(progress_path):
    try:
        with open(progress_path, "r", encoding="utf-8") as f:
            saved_map = json.load(f)
    except Exception:
        saved_map = {}
else:
    saved_map = {}

# buttons
col1, col2 = st.columns([1,1])
start = col1.button("🚀 Start Scraping")
download_partial = col2.button("⬇️ Download Partial Results Now")

# helper to save progress
def save_progress(mp):
    with open(progress_path, "w", encoding="utf-8") as f:
        json.dump(mp, f, ensure_ascii=False, indent=2)

# If user wants partial download
if download_partial:
    # build current results
    results = [saved_map.get(str(row[code_col])) for i,row in df.iterrows() if str(row[code_col]) in saved_map]
    if not results:
        st.warning("No results to download yet.")
    else:
        # build excel bytes
        def build_excel_bytes(results):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                main_rows = []
                aladi_rows = []
                for r in results:
                    code = r.get("code")
                    link = r.get("link")
                    h = r.get("historic", {})
                    main_rows.append({
                        "Code": code,
                        "Link": link,
                        "Last Published": h.get("last_published",""),
                        "Ad-valorem": h.get("ad_valorem",""),
                        "Description": h.get("description",""),
                        "ALADI_count": len(r.get("aladi", [])),
                        "ALADI_json": json.dumps(r.get("aladi", []), ensure_ascii=False)
                    })
                    for a in r.get("aladi", []):
                        row = {"Code": code}
                        row.update(a)
                        aladi_rows.append(row)
                pd.DataFrame(main_rows).to_excel(writer, index=False, sheet_name="Main")
                if aladi_rows:
                    pd.DataFrame(aladi_rows).to_excel(writer, index=False, sheet_name="ALADI")
            output.seek(0)
            return output.getvalue()

        excel_bytes = build_excel_bytes(results)
        st.download_button("Download partial Excel", data=excel_bytes,
                           file_name=f"tigie_partial_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Start scraping loop
if start:
    st.info("Starting Playwright. This may take time depending on number of codes and site speed.")
    total = len(df)
    progress_bar = st.progress(0)
    status_text = st.empty()
    results_map = saved_map  # modify directly and save on the fly

    # run playwright
    playwright_obj = sync_playwright().start()
    try:
        # If proxy provided, create browser context with proxy (Playwright supports passing proxy to browser.launch)
        # Note: sync_playwright().start() -> playwright_obj
        # We'll call scrape_with_playwright which launches Chromium per-call to avoid complex context; that's simpler & robust.
        # Optionally you could reuse browser between calls to improve speed.
        for idx, row in df.iterrows():
            code = str(row[code_col]).strip()
            if not code:
                continue
            link = str(row[link_col]).strip() if link_col else ""
            if code in results_map:
                status_text.info(f"{idx+1}/{total} {code} — already done, skipping.")
                progress_bar.progress((idx+1)/total)
                continue

            status_text.info(f"{idx+1}/{total} Scraping {code} ...")
            # build url if not provided
            if link:
                url = link
            else:
                # default date: today (M/D/YYYY)
                today = datetime.now()
                date_param = f"{today.month}/{today.day}/{today.year}"
                url = f"https://tigie.com.mx/?hs={code}&date={date_param}"

            try:
                html, dbg = scrape_with_playwright(playwright_obj, url, wait_for_selector="table", timeout=int(timeout))
                soup = BeautifulSoup(html, "lxml")
                historic = parse_historic_from_soup(soup)
                aladi = extract_aladi_from_soup(soup)

                # If ALADI empty, try an explicit alt URL with tab param
                if not aladi:
                    alt_url = url + ("&tab=aladi" if "?" in url else "?tab=aladi")
                    html2, dbg2 = scrape_with_playwright(playwright_obj, alt_url, wait_for_selector="table", timeout=int(timeout))
                    soup2 = BeautifulSoup(html2, "lxml")
                    aladi = extract_aladi_from_soup(soup2)
                    # merge historic if empty
                    if not historic.get("description"):
                        h2 = parse_historic_from_soup(soup2)
                        if h2.get("description"):
                            historic = h2

                entry = {"code": code, "link": url, "historic": historic, "aladi": aladi, "debug": dbg}
                # status
                if historic.get("description") or aladi:
                    entry["status"] = "OK"
                else:
                    entry["status"] = "EMPTY"

                results_map[code] = entry
                save_progress(results_map)

            except Exception as exc:
                tb = traceback.format_exc()
                entry = {"code": code, "link": url, "historic": {}, "aladi": [], "status": f"ERROR: {str(exc)}", "debug": {"error": tb}}
                results_map[code] = entry
                save_progress(results_map)

            # update progress
            progress_bar.progress((idx+1)/total)
            time.sleep(delay)

    finally:
        try:
            playwright_obj.stop()
        except Exception:
            pass

    st.success("Scraping finished. You can download the results below.")
    # build excel for final results
    final_results = [results_map.get(str(row[code_col])) for _, row in df.iterrows() if str(row[code_col]) in results_map]

    # build excel in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        main_rows = []
        aladi_rows = []
        for r in final_results:
            if not r: continue
            code = r.get("code")
            link = r.get("link")
            h = r.get("historic", {})
            main_rows.append({
                "Code": code,
                "Link": link,
                "Last Published": h.get("last_published",""),
                "Ad-valorem": h.get("ad_valorem",""),
                "Description": h.get("description",""),
                "ALADI_count": len(r.get("aladi", [])),
                "ALADI_json": json.dumps(r.get("aladi", []), ensure_ascii=False)
            })
            for a in r.get("aladi", []):
                row = {"Code": code}
                row.update(a)
                aladi_rows.append(row)
        pd.DataFrame(main_rows).to_excel(writer, index=False, sheet_name="Main")
        if aladi_rows:
            pd.DataFrame(aladi_rows).to_excel(writer, index=False, sheet_name="ALADI")
    output.seek(0)

    st.download_button("⬇️ Download final Excel", data=output.getvalue(),
                       file_name=f"tigie_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    st.write("If many codes returned EMPTY or ERROR, try:")
    st.markdown("- استخدام VPN/Proxy بموقع Mexico (بعض المحتوى قد يُقيّد حسب الموقع الجغرافي).")
    st.markdown("- زيادة قيمة timeout في الشريط الجانبي.")
    st.markdown("- تجربة تشغيل السكربت محلياً (ليس في بيئة سحابية) مع تشغيل `playwright install` مسبقًا.")
