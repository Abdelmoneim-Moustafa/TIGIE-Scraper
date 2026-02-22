# 📦 TIGIE Tariff Scraper

A web scraper for [tigie.com.mx](https://tigie.com.mx) that extracts **Historic Record** and **ALADI** tariff data for Mexican HS codes and exports everything to a formatted Excel file.


## ✨ What it does

For each HS code in your Excel file, the scraper visits:
```
https://tigie.com.mx/?hs={CODE}&date={DATE}
```

And extracts **two datasets**:

| Tab | Data collected |
|-----|---------------|
| **Historic Record** | Most recently published row → Description, Published date, Valid Since, Ad-Valorem, Unit |
| **ALADI** | Every row for every country → Country, Ad-Valorem, Only clause, Published, Valid Since/Until, Code (ACE6, RE64…), Quotas |

### Output Excel (3 sheets)

| Sheet | Contents |
|-------|---------|
| `Historic Record` | One row per HS code |
| `ALADI` | All ALADI rows — HS Code repeated in column A |
| `Summary` | Counts, coverage stats, run date |

---

## 🚀 Quick Start

### 1 — Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/tigie-scraper.git
cd tigie-scraper
```

### 2 — Install dependencies

```bash
pip install -r requirements.txt
```

### 3a — Run the web app (recommended)

```bash
streamlit run tigie_scraper.py
```

Open **http://localhost:8501** in your browser, then:
1. Upload your `codes.xlsx` (must have a `Code` column)
2. Set the date and delay in the sidebar
3. Choose which rows to process
4. Click **🚀 Start Scraping**
5. Click **⬇️ Download Excel** when done

### 3b — Run from command line (no browser needed)

```bash
# Scrape first 100 codes
python tigie_scraper.py --cli codes.xlsx --limit 100

# Scrape rows 201–300 (resume a previous run)
python tigie_scraper.py --cli codes.xlsx --start 201 --limit 100

# Custom date and slower delay
python tigie_scraper.py --cli codes.xlsx --date 1/25/2026 --delay 2.0
```

---

## 📁 File Structure

```
tigie-scraper/
│
├── tigie_scraper.py     ← single file: Streamlit UI + CLI + all scraping logic
├── requirements.txt     ← pip dependencies
├── README.md            ← this file
└── codes.xlsx           ← your input file (NOT committed to git — add to .gitignore)
```

---

## 📊 Input File Format

Your Excel file needs at minimum a **`Code`** column:

| Code | Link | Priority |
|------|------|----------|
| 85011010 | https://tigie.com.mx/?hs=85011010&date=1/25/2026 | 1 |
| 85012005 | https://tigie.com.mx/?hs=85012005&date=1/25/2026 | 1 |

- `Link` and `Priority` columns are optional
- The scraper builds URLs automatically from the `Code` column and your date setting

---

## ⚙️ CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--cli` | *(required)* | Path to input Excel file |
| `--date` | `1/25/2026` | Date for the URL parameter |
| `--delay` | `1.5` | Seconds between requests |
| `--retries` | `3` | Retry attempts per code |
| `--start` | `1` | Start from this row (1-indexed) |
| `--limit` | *(all)* | Max number of codes to process |

---

## 💡 Tips for 7,000+ codes

- Process in batches of **100–200 codes** at a time using `--start` and `--limit`
- Use a delay of **1.5–2 seconds** to avoid being blocked
- Estimated time: ~3 hours for all 7,503 codes at 1.5s/code
- The CLI prints progress live so you know exactly where to resume if interrupted

---

## 🛠 Requirements

- Python 3.10 or later
- Internet access to reach tigie.com.mx

```
streamlit>=1.32.0
requests>=2.31.0
beautifulsoup4>=4.12.0
lxml>=5.1.0
openpyxl>=3.1.2
pandas>=2.2.0
```

---

## 📂 .gitignore recommendation

Add this to your `.gitignore` to avoid committing your data files:

```
*.xlsx
*.xls
tigie_export_*.xlsx
__pycache__/
.streamlit/
```

---

## 📄 License

MIT — free to use and modify.
