import streamlit as st  # Streamlit web app framework
import pandas as pd  # For Excel I/O
from io import BytesIO  # To handle in-memory bytes buffer for download

# Selenium imports for web scraping
from selenium import webdriver
from selenium.webdriver.chrome.options import Options  # Chrome options for headless mode
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager  # To auto-manage ChromeDriver
# Note: On Streamlit Cloud, one can install geckodriver and use headless Firefox (example shown below)0

st.title("TIGIE HS Code Scraper")
st.write("Upload an Excel file with columns 'HS Code' and 'Link', then click Process to scrape data.")

# File uploader for Excel (XLSX)1
uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])
if uploaded_file is not None:
    try:
        # Read Excel into DataFrame2
        df_input = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")
        st.stop()

    # Rename first two columns to 'HS Code' and 'Link' for consistency
    if df_input.shape[1] >= 2:
        df_input = df_input.rename(columns={df_input.columns[0]: "HS Code", df_input.columns[1]: "Link"})
    if "HS Code" not in df_input.columns or "Link" not in df_input.columns:
        st.error("Excel file must have two columns: HS Code and Link.")
    else:
        if st.button("Process"):
            # Selenium Chrome headless setup3
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Run in headless mode4
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")  # Bypass OS security model
            chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

            # Use webdriver-manager to automatically handle driver installation5
            driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

            data_rows = []
            # Iterate over each HS code and link
            for idx, row in df_input.iterrows():
                hs_code = str(row["HS Code"])
                url = str(row["Link"])
                st.write(f"Processing HS Code: {hs_code}")
                try:
                    driver.get(url)
                except Exception as e:
                    st.error(f"Failed to load URL {url}: {e}")
                    continue

                wait = WebDriverWait(driver, 10)  # Explicit wait for dynamic content6

                # Click on 'Historic Record' tab if available
                try:
                    hist_tab = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Historic Record")))
                    hist_tab.click()
                except (TimeoutException, NoSuchElementException):
                    pass

                # Extract fields from 'Historic Record' tab7
                try:
                    last_published = driver.find_element(
                        By.XPATH, "//td[contains(text(),'Last published date')]/following-sibling::td"
                    ).text
                except NoSuchElementException:
                    last_published = None
                try:
                    ad_valorem = driver.find_element(
                        By.XPATH, "//td[contains(text(),'Ad-valorem')]/following-sibling::td"
                    ).text
                except NoSuchElementException:
                    ad_valorem = None
                try:
                    description = driver.find_element(
                        By.XPATH, "//td[contains(text(),'Description')]/following-sibling::td"
                    ).text
                except NoSuchElementException:
                    description = None

                # Click on 'ALADI' tab if available
                try:
                    aladi_tab = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "ALADI")))
                    aladi_tab.click()
                except (TimeoutException, NoSuchElementException):
                    pass

                # Scrape all table data from the ALADI tab
                aladi_cells = []
                try:
                    table = driver.find_element(By.TAG_NAME, "table")
                    rows = table.find_elements(By.TAG_NAME, "tr")
                    for tr in rows:
                        cells = tr.find_elements(By.TAG_NAME, "td")
                        if cells:
                            for cell in cells:
                                aladi_cells.append(cell.text)
                except Exception:
                    # Table or rows might not exist
                    pass

                # Compile row data: HS Code, link, historic data, then ALADI data flattened
                data_rows.append([hs_code, url, last_published, ad_valorem, description] + aladi_cells)

            driver.quit()

            if data_rows:
                # Determine the maximum number of ALADI cells found to standardize columns
                max_len = max(len(row) for row in data_rows)
                # Pad shorter rows with None
                for row in data_rows:
                    row.extend([None] * (max_len - len(row)))

                # Define column names: first five fixed, rest for ALADI columns
                base_cols = ["HS Code", "Link", "Last Published Date", "Ad-valorem", "Description"]
                aladi_cols = [f"ALADI_{i+1}" for i in range(max_len - 5)]
                columns = base_cols + aladi_cols

                df_out = pd.DataFrame(data_rows, columns=columns)

                # Prepare in-memory Excel file for download8
                output = BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    df_out.to_excel(writer, index=False, sheet_name="Sheet1")
                output.seek(0)

                # Download button for the compiled Excel file
                st.download_button(
                    label="Download Compiled Excel",
                    data=output.getvalue(),
                    file_name="compiled_tigie_data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )  # Provide download of in-memory Excel9
