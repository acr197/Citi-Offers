# ---------------------------------------------------------------------------
# Section: Imports
# ---------------------------------------------------------------------------

import re, sys, time
from datetime import datetime
from typing import List, Set, Tuple, Optional
from dotenv import load_dotenv
import os, json


import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
print("# imports loaded")


# ---------------------------------------------------------------------------
# Section: Constants & Credentials
# ---------------------------------------------------------------------------

# Configuration and login setup
# Load .env variables
load_dotenv()

# Configuration and login setup
CITI_USERNAME  = os.getenv("CITI_USERNAME")
CITI_PASSWORD  = os.getenv("CITI_PASSWORD")
USER_TO_HOLDER = json.loads(os.getenv("GDRIVE_USER_TO_HOLDER"))


LOGIN_URL      = "https://online.citi.com/US/login.do"
OFFERS_URL     = "https://online.citi.com/US/ag/products-offers/merchantoffers"
CARD_HOLDER    = USER_TO_HOLDER.get(CITI_USERNAME, "")
SECOND_MONITOR_OFFSET = (3440, 0)
print("# constants loaded")


# ---------------------------------------------------------------------------
# Section: Google-Sheets Setup
# ---------------------------------------------------------------------------

# Create or open a worksheet and ensure headers exist
def _ws(sheet, title: str, headers: Tuple[str, ...]):
    ws = sheet.worksheet(title) if title in [w.title for w in sheet.worksheets()] \
         else sheet.add_worksheet(title=title, rows=2000, cols=len(headers))
    if ws.row_count < 1:
        ws.append_row(list(headers), value_input_option="RAW")
    return ws
print("# _ws loaded")

# Append log entry with minimal quota use
def sheet_log(level: str, func: str, msg: str):
    LOG_WS.append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                       level, func, msg],
                      value_input_option="RAW",
                      insert_data_option="INSERT_ROWS")
print("# sheet_log loaded")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
CREDS  = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
SHEET  = gspread.authorize(CREDS).open("Credit Card Offers")

OFFER_WS = _ws(SHEET, "Card Offers",
               ("Card Holder","Last Four","Card Name","Brand",
                "Discount","Maximum Discount","Minimum Spend","Expiration","Local"))
LOG_WS   = _ws(SHEET, "Log", ("Time","Level","Function","Message"))
print("# Sheets configured")


# ---------------------------------------------------------------------------
# Section: Selenium Driver Initialization
# ---------------------------------------------------------------------------

# Start a Chrome browser on the second monitor
def build_driver():
    opts = Options()
    opts.add_argument("--start-maximized")
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                           options=opts)
    drv.set_window_position(*SECOND_MONITOR_OFFSET)
    return drv, WebDriverWait(drv, 30)
print("# build_driver loaded")

driver, wait = build_driver()


# ---------------------------------------------------------------------------
# Section: Login Functions
# ---------------------------------------------------------------------------

# Attempt login with specified typing delay
def login_once(pause: float):
    driver.get(LOGIN_URL)
    wait.until(EC.presence_of_element_located((By.ID,"username"))).send_keys(CITI_USERNAME)
    time.sleep(pause)
    wait.until(EC.presence_of_element_located((By.ID,"password"))).send_keys(CITI_PASSWORD)
    time.sleep(pause)
    wait.until(EC.element_to_be_clickable(
        (By.XPATH,"//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sign on')]")
    )).click()
print("# login_once loaded")

# Confirm login by checking redirected URL
def logged_in() -> bool:
    u = driver.current_url
    return "/dashboard" in u or "merchantoffers" in u
print("# logged_in loaded")

# Full login with retry logic and logging
def citi_login():
    for pause in (0.1, 1.0):
        login_once(pause)
        try:
            wait.until(lambda _: logged_in() or "login" in driver.current_url, 10)
        except Exception:
            pass
        if logged_in():
            sheet_log("INFO","login","success")
            return
    sheet_log("ERROR","login","failed")
    driver.quit(); sys.exit()
print("# citi_login loaded")

citi_login()


# ---------------------------------------------------------------------------
# Section: Navigate to Merchant Offers
# ---------------------------------------------------------------------------

# Check if offer tiles are present
def offers_ready() -> bool:
    return bool(driver.find_elements(By.XPATH,"//div[contains(@class,'offer-tile')]"))

# Detect the red error banner indicating failed load
def error_banner_visible() -> bool:
    return bool(driver.find_elements(By.ID,"available-err-msg"))
print("# error_banner_visible loaded")

# Visit and retry the offers page up to 3 times
def visit_offers_page():
    driver.get(OFFERS_URL)
    for attempt in range(1, 4):
        try:
            wait.until(lambda _: offers_ready() or error_banner_visible(), 15)
        except Exception:
            pass
        if offers_ready():
            sheet_log("INFO", "nav", f"offers ready (try {attempt})")
            return
        if error_banner_visible():
            sheet_log("WARN", "nav", f"load-error banner (try {attempt})")
            driver.refresh()
            time.sleep(2)
    sheet_log("ERROR", "nav", "banner persisted 3× – abort")
    print("ERROR: Offers could not load after 3 attempts – exiting.")
    driver.quit()
    sys.exit()
print("# visit_offers_page loaded")

visit_offers_page()
print("On Merchant Offers page ✔")


# ---------------------------------------------------------------------------
# Section: Offer Helpers and Modal Parsing
# ---------------------------------------------------------------------------

# Find all clickable blue plus-circle icons
def plus_icons() -> list:
    return driver.find_elements(By.XPATH,
        "//cds-icon[@name='plus-circle' and @arialabel='Enroll']")
print("# plus_icons loaded")

# Click any Show More or Load More buttons
def expand_all():
    while True:
        btns=[b for b in driver.find_elements(
            By.XPATH,"//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'show more') "
                     "or contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'load more')]")
              if b.is_displayed()]
        if not btns: break
        for b in btns:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});",b)
                wait.until(EC.element_to_be_clickable(b)).click()
            except Exception as e:
                sheet_log("ERROR","expand",str(e))
        time.sleep(0.4)
    wait.until(EC.presence_of_element_located(
        (By.XPATH,"//div[contains(@class,'offer-tile')]")), 10)
print("# expand_all loaded")

# Close modal using any available control
def close_modal():
    sels=["//button[contains(text(),'Close')]",
          "//button[@aria-label='Close']",
          "//button[contains(@class,'cds-modal-close')]",
          "//cds-icon/ancestor::button"]
    for s in sels:
        if (el:=driver.find_elements(By.XPATH,s)):
            try:
                driver.execute_script("arguments[0].click();",el[0])
                wait.until(EC.invisibility_of_element_located(
                    (By.CSS_SELECTOR,".mo-modal-img-merchant-name"))); return
            except Exception: pass
    driver.find_element(By.TAG_NAME,"body").send_keys(Keys.ESCAPE)
    wait.until(EC.invisibility_of_element_located(
        (By.CSS_SELECTOR,".mo-modal-img-merchant-name")))
print("# close_modal loaded")


# ---------------------------------------------------------------------------
# Section: Offer Value Parsers
# ---------------------------------------------------------------------------

# Extract “Max $X” style values from offer description
def parse_max_disc(text: str) -> Optional[str]:
    m = re.search(r"[Mm]ax[^$]{0,25}\$(\d[\d,]*)", text)
    return f"${m.group(1)}" if m else None
print("# parse_max_disc loaded")

# Extract minimum spend like “purchase of $75 or more”
def parse_min_spend(text: str) -> Optional[str]:
    m = re.search(r"(?:purchase|spend)[^$]{0,25}\$(\d[\d,]*)", text, re.I)
    return f"${m.group(1)}" if m else None
print("# parse_min_spend loaded")

# ---------------------------------------------------------------------------
# Section: Card Dropdown Handling
# ---------------------------------------------------------------------------

CARD_LABEL_CSS = "div#cds-dropdown-button-value.cds-dd2-pseudo-value"
BTN_DROPD_X    = "//button[@id='cds-dropdown' and contains(@class,'cds-dd2-button')]"
OPT_DROPD_X    = "//ul[@id='cds-dropdown-listbox']/li[not(contains(@class,'disabled'))]"

# Click the card dropdown
def open_card_dropdown():
    wait.until(EC.element_to_be_clickable((By.XPATH, BTN_DROPD_X))).click()
print("# open_card_dropdown loaded")

# Get the label of the selected card
def get_label():
    return wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, CARD_LABEL_CSS)))
print("# get_label loaded")


# ---------------------------------------------------------------------------
# Section: Offer Scraping
# ---------------------------------------------------------------------------

# Click through a card, open each modal, extract all offers
def scrape_card(label: str, seen: Set[Tuple]):
    if get_label().text.strip() != label:
        open_card_dropdown()
        wait.until(EC.element_to_be_clickable(
            (By.XPATH, f"{OPT_DROPD_X}[normalize-space()='{label}']"))).click()
        wait.until(lambda _: get_label().text.strip() == label)

    card, last4 = [s.strip() for s in label.rsplit("-", 1)]

    # Retry logic for load error banner
    for _ in range(3):
        if not error_banner_visible(): break
        driver.refresh(); time.sleep(1)
    else:
        sheet_log("WARN", "card", f"{label}: skipped – banner stuck")
        return

    driver.execute_script("window.scrollTo(0,0);")
    expand_all()

    new_rows = []
    while (icons := plus_icons()):
        ico = icons[0]
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", ico)
        ico.click()

        wait.until(lambda _:
            driver.find_elements(By.XPATH,"//div[contains(@class,'enrolled')]") or
            driver.find_elements(By.CSS_SELECTOR,".mo-modal-img-merchant-name"), 8)

        brand = driver.find_element(By.CSS_SELECTOR, ".mo-modal-img-merchant-name").text.strip()
        disc  = driver.find_element(By.CSS_SELECTOR, ".mo-modal-offer-title div").text.strip()
        body  = driver.find_element(By.CSS_SELECTOR, "cds-column section").text
        maxd  = parse_max_disc(body) or ""
        mins  = parse_min_spend(body) or "None"
        exp   = driver.find_element(By.CSS_SELECTOR, ".mo-modal-header-date span").text.strip()
        local = "Yes" if "philadelphia" in body.lower() else "No"

        row = (CARD_HOLDER, last4, card, brand, disc, maxd, mins, exp, local)
        if row not in seen:
            new_rows.append(list(row)); seen.add(row)

        close_modal()
        time.sleep(0.3)

    if new_rows:
        OFFER_WS.append_rows(new_rows, value_input_option="RAW",
                             insert_data_option="INSERT_ROWS")
    print(f"{card} – added {len(new_rows)} rows")
print("# scrape_card loaded")


# ---------------------------------------------------------------------------
# Section: Loop Through All Cards
# ---------------------------------------------------------------------------

# Collect all card labels from dropdown
def all_card_labels() -> List[str]:
    open_card_dropdown()
    labs = [li.text.strip() for li in driver.find_elements(By.XPATH, OPT_DROPD_X)
            if li.text.strip() and li.text.strip().lower() != "credit"]
    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
    return labs
print("# all_card_labels loaded")

# Scrape all cards listed
def scrape_all_cards() -> Set[Tuple]:
    seen = {tuple(r) for r in OFFER_WS.get_all_values()[1:]}
    for lab in all_card_labels():
        scrape_card(lab, seen)
    return seen
print("# scrape_all_cards loaded")


# ---------------------------------------------------------------------------
# Section: Sheet Cleanup
# ---------------------------------------------------------------------------

# Determine if an offer has expired
def expired(row: List[str]) -> bool:
    try:
        return datetime.strptime(row[7], "%m/%d/%Y").date() < datetime.today().date()
    except Exception:
        return False
print("# expired loaded")

# Delete expired or missing rows from Google Sheet
def clean_sheet(current: Set[Tuple]):
    rows = OFFER_WS.get_all_values()
    sid = OFFER_WS._properties["sheetId"]
    req = []
    for i in range(len(rows) - 1, 0, -1):
        if tuple(rows[i]) not in current or expired(rows[i]):
            req.append({"deleteRange": {"range": {"sheetId": sid,
                                                  "startRowIndex": i,
                                                  "endRowIndex": i + 1},
                                        "shiftDimension": "ROWS"}})
    if req:
        OFFER_WS.spreadsheet.batch_update({"requests": req})
print("# clean_sheet loaded")

# Remove duplicate offers from sheet
def dedupe_rows():
    rows = OFFER_WS.get_all_values()
    seen = set()
    sid = OFFER_WS._properties["sheetId"]
    req = []
    for i in range(len(rows) - 1, 0, -1):
        k = tuple(rows[i])
        if k in seen:
            req.append({"deleteRange": {"range": {"sheetId": sid,
                                                  "startRowIndex": i,
                                                  "endRowIndex": i + 1},
                                        "shiftDimension": "ROWS"}})
        else:
            seen.add(k)
    if req:
        OFFER_WS.spreadsheet.batch_update({"requests": req})
print("# dedupe_rows loaded")


# ---------------------------------------------------------------------------
# Section: Main Run
# ---------------------------------------------------------------------------

# Execute full card scraping and cleanup
all_rows = scrape_all_cards()
clean_sheet(all_rows)
dedupe_rows()
sheet_log("INFO", "main", "COMPLETE")
driver.quit()
print("Done ✔")

