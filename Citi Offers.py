# ---------------------------------------------------------------------------
# Section: Imports
# ---------------------------------------------------------------------------
# Standard library and typing tools
import os, sys, re, time, json
from datetime import datetime
from typing import List, Set, Tuple, Optional

# Third-party dependencies
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

print("# imports loaded – external deps resolved")

# ---------------------------------------------------------------------------
# Section: Helpers – early validation
# ---------------------------------------------------------------------------
# Ensure required files exist before proceeding
def require_file(path: str, description: str) -> str:
    """Abort script if a required file is missing."""
    if not os.path.isfile(path):
        sys.exit(f"Required {description} not found: '{path}' – aborting")
    return path
print("# require_file loaded – verifies critical paths")

# ---------------------------------------------------------------------------
# Section: Constants & Accounts
# ---------------------------------------------------------------------------
# Resolve project root so relative paths work inside PyCharm
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Load .env variables
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# Assemble Citi account credentials from .env
ACCOUNTS: list[dict] = []
idx = 1
while os.getenv(f"CITI_USERNAME_{idx}"):
    ACCOUNTS.append({
        "user":   os.getenv(f"CITI_USERNAME_{idx}"),
        "pass":   os.getenv(f"CITI_PASSWORD_{idx}"),
        "holder": os.getenv(f"CITI_HOLDER_{idx}", f"Holder {idx}")
    })
    idx += 1
if not ACCOUNTS:
    sys.exit("No Citi accounts found in .env – aborting")

LOGIN_URL  = "https://online.citi.com/US/login.do"
OFFERS_URL = "https://online.citi.com/US/ag/products-offers/merchantoffers"
SECOND_MONITOR_OFFSET = (3440, 0)
print("# constants loaded – accounts & URLs configured")

# ---------------------------------------------------------------------------
# Section: Google-Sheets Setup
# ---------------------------------------------------------------------------
# Authorise service account
SA_PATH = os.getenv("GOOGLE_SA_PATH",
                    os.path.join(PROJECT_ROOT, "service_account.json"))
require_file(SA_PATH, "Google service-account JSON")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
CREDS  = Credentials.from_service_account_file(SA_PATH, scopes=SCOPES)
SHEET  = gspread.authorize(CREDS).open("Credit Card Offers")

# Ensure worksheet exists and has correct headers
def _ws(sheet, title: str, headers: Tuple[str, ...]):
    """Create/open worksheet and enforce header row."""
    existing = {w.title: w for w in sheet.worksheets()}
    ws = existing.get(title) or sheet.add_worksheet(title=title,
                                                    rows=2000,
                                                    cols=len(headers))
    first_row = ws.row_values(1)
    if first_row != list(headers):
        if not first_row:                       # brand-new sheet
            ws.append_row(list(headers), value_input_option="RAW")
        else:                                   # existing – replace header
            ws.update("1:1", [headers], value_input_option="RAW")
    return ws
print("# _ws loaded – worksheet helper ready")

# Write log message to Log sheet
def sheet_log(level: str, func: str, msg: str):
    """Append timestamped log row to Log worksheet."""
    LOG_WS.append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                       level, func, msg],
                      value_input_option="RAW",
                      insert_data_option="INSERT_ROWS")
print("# sheet_log loaded – logging helper ready")

# Headers incl. new Date Added column
OFFER_HEADERS = (
    "Card Holder", "Last Four", "Card Name", "Brand",
    "Discount", "Maximum Discount", "Minimum Spend",
    "Date Added", "Expiration", "Local"
)
OFFER_WS = _ws(SHEET, "Card Offers", OFFER_HEADERS)
LOG_WS   = _ws(SHEET, "Log", ("Time", "Level", "Function", "Message"))
print("# Sheets configured – worksheets ready")

# Insert Date Added column if sheet existed before without it
def ensure_date_added_column():
    """Insert 'Date Added' col if missing; adjust headers once."""
    header = OFFER_WS.row_values(1)
    if "Date Added" not in header:
        idx = 8  # zero-based index where Date Added should live
        sid = OFFER_WS._properties["sheetId"]
        SHEET.batch_update({
            "requests": [{
                "insertDimension": {
                    "range": {"sheetId": sid,
                              "dimension": "COLUMNS",
                              "startIndex": idx - 1,
                              "endIndex": idx},
                    "inheritFromBefore": True
                }
            }]
        })
        OFFER_WS.update_cell(1, idx, "Date Added")
        print("# ensure_date_added_column – column inserted")

ensure_date_added_column()

# Force Log sheet row height to 21 px
def set_log_row_height():
    """Update Log sheet row height to 21 pixels for all rows."""
    sid = LOG_WS._properties["sheetId"]
    SHEET.batch_update({
        "requests": [{
            "updateDimensionProperties": {
                "range": {"sheetId": sid, "dimension": "ROWS"},
                "properties": {"pixelSize": 21},
                "fields": "pixelSize"
            }
        }]
    })
print("# set_log_row_height loaded – row-height setter ready")
set_log_row_height()

# ---------------------------------------------------------------------------
# Section: Selenium Driver Init
# ---------------------------------------------------------------------------
# Build Chrome driver
def build_driver() -> Tuple[webdriver.Chrome, WebDriverWait]:
    """Launch maximised Chrome and return driver & waiter objects."""
    opts = Options()
    opts.add_argument("--start-maximized")
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                           options=opts)
    drv.set_window_position(*SECOND_MONITOR_OFFSET)
    return drv, WebDriverWait(drv, 30)
print("# build_driver loaded – Selenium factory ready")

driver, wait = build_driver()
# ---------------------------------------------------------------------------
# Section: Login / Logout Helpers
# ---------------------------------------------------------------------------
# Perform one login attempt with optional pause between keystrokes
def login_once(username: str, password: str, pause: float) -> None:
    """Submit credentials once; pause adjusts typing delay."""
    driver.get(LOGIN_URL)
    wait.until(EC.presence_of_element_located((By.ID, "username"))).send_keys(
        username)
    time.sleep(pause)
    wait.until(EC.presence_of_element_located((By.ID, "password"))).send_keys(
        password)
    time.sleep(pause)
    wait.until(EC.element_to_be_clickable((
        By.XPATH,
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
        "'abcdefghijklmnopqrstuvwxyz'),'sign on')]"))).click()
print("# login_once loaded – single sign-on ready")

# Determine if post-login page is loaded
def logged_in() -> bool:
    """Return True when dashboard or offers page is detected."""
    u = driver.current_url
    return "/dashboard" in u or "merchantoffers" in u
print("# logged_in loaded – post-login tester ready")

# Skip Citi upsell/special-offer interstitial if present
def skip_interstitial() -> None:
    """Dismiss upsell interstitial and land on offers page."""
    try:
        if "offerintr" in driver.current_url or "special" in driver.title.lower():
            btn = wait.until(EC.element_to_be_clickable((
                By.XPATH,
                "//a[normalize-space()='No Thanks'] | "
                "//button[normalize-space()='No Thanks']"
            )), timeout=5)
            btn.click()
            time.sleep(2)
    except Exception:
        pass
    finally:
        if "offerintr" in driver.current_url:
            driver.get(OFFERS_URL)
print("# skip_interstitial loaded – upsell bypass ready")

# Attempt fast and slow login; log result
def citi_login(username: str, password: str) -> bool:
    """Try two login speeds; log outcome; return success flag."""
    for pause in (0.1, 1.0):
        login_once(username, password, pause)
        try:
            wait.until(lambda _: logged_in() or "login" in driver.current_url,
                       10)
        except Exception:
            pass
        if logged_in():
            skip_interstitial()
            sheet_log("INFO", "login", f"{username} success")
            return True
    sheet_log("ERROR", "login", f"{username} failed")
    return False
print("# citi_login loaded – dual-pace login ready")

# Sign out and clear cookies
def citi_logout() -> None:
    """Navigate to logout URL and clear cookies regardless of errors."""
    try:
        driver.get("https://online.citi.com/US/logout")
        time.sleep(3)
        driver.delete_all_cookies()
        sheet_log("INFO", "logout", "success")
    except Exception as exc:
        sheet_log("WARN", "logout", str(exc))
print("# citi_logout loaded – logout helper ready")

# ---------------------------------------------------------------------------
# Section: Page Ready & Banner Check
# ---------------------------------------------------------------------------
# Check if any offer tiles exist
def offers_ready() -> bool:
    """Return True when at least one offer tile is present."""
    return bool(driver.find_elements(By.XPATH,
                                     "//div[contains(@class,'offer-tile')]"))

# Check if error banner is visible
def error_banner_visible() -> bool:
    """Return True when Citi displays the error banner on offers page."""
    return bool(driver.find_elements(By.ID, "available-err-msg"))
print("# ready/banner helpers loaded – page state testers ready")

# Navigate to offers page with retries
def visit_offers_page() -> bool:
    """Open Merchant Offers; retry three times on banner error."""
    driver.get(OFFERS_URL)
    for attempt in range(1, 4):
        try:
            wait.until(lambda _: offers_ready() or error_banner_visible(), 15)
        except Exception:
            pass
        if offers_ready():
            sheet_log("INFO", "nav", f"offers ready (try {attempt})")
            return True
        if error_banner_visible():
            sheet_log("WARN", "nav", f"banner error (try {attempt})")
            driver.refresh()
            time.sleep(2)
    sheet_log("ERROR", "nav", "banner persisted 3× – abort account")
    return False
print("# visit_offers_page loaded – resilient loader ready")

# ---------------------------------------------------------------------------
# Section: Offer Helpers & Modal Handling
# ---------------------------------------------------------------------------
# Return visible plus-circle icons
def plus_icons():
    """Locate all plus-circle 'Enroll' icons currently shown."""
    return driver.find_elements(By.XPATH,
        "//cds-icon[@name='plus-circle' and @arialabel='Enroll']")
print("# plus_icons loaded – icon locator ready")

# Expand all 'Show more' / 'Load more' buttons
def expand_all():
    """Iteratively click 'Show more' / 'Load more' until none remain."""
    while True:
        btns = [b for b in driver.find_elements(
            By.XPATH,
            "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
            "'abcdefghijklmnopqrstuvwxyz'),'show more') "
            "or contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
            "'abcdefghijklmnopqrstuvwxyz'),'load more')]")
            if b.is_displayed()]
        if not btns:
            break
        for btn in btns:
            try:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", btn)
                wait.until(EC.element_to_be_clickable(btn)).click()
            except Exception as exc:
                sheet_log("ERROR", "expand", str(exc))
        time.sleep(0.4)
    wait.until(EC.presence_of_element_located(
        (By.XPATH, "//div[contains(@class,'offer-tile')]")), 10)
print("# expand_all loaded – offer expander ready")

# Close modal dialog safely
def close_modal():
    """Dismiss offer modal with any close control or ESC key."""
    selectors = [
        "//button[contains(text(),'Close')]",
        "//button[@aria-label='Close']",
        "//button[contains(@class,'cds-modal-close')]",
        "//cds-icon/ancestor::button",
    ]
    for sel in selectors:
        els = driver.find_elements(By.XPATH, sel)
        if els:
            try:
                driver.execute_script("arguments[0].click();", els[0])
                wait.until(EC.invisibility_of_element_located(
                    (By.CSS_SELECTOR, ".mo-modal-img-merchant-name")))
                return
            except Exception:
                pass
    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
    wait.until(EC.invisibility_of_element_located(
        (By.CSS_SELECTOR, ".mo-modal-img-merchant-name")))
print("# close_modal loaded – modal closer ready")

# ---------------------------------------------------------------------------
# Section: Value Parsers
# ---------------------------------------------------------------------------
# Parse maximum discount from offer text
def parse_max_disc(text: str) -> Optional[str]:
    """Return '$NN' string for max discount or None if not found."""
    m = re.search(r"[Mm]ax[^$]{0,25}\$(\d[\d,]*)", text)
    return f"${m.group(1)}" if m else None
print("# parse_max_disc loaded – max discount parser ready")

# Parse minimum spend from offer text
def parse_min_spend(text: str) -> Optional[str]:
    """Return '$NN' string for min spend or None if not found."""
    m = re.search(r"(?:purchase|spend)[^$]{0,25}\$(\d[\d,]*)", text, re.I)
    return f"${m.group(1)}" if m else None
print("# parse_min_spend loaded – min spend parser ready")

# ---------------------------------------------------------------------------
# Section: Card Dropdown Handling
# ---------------------------------------------------------------------------
CARD_LABEL_CSS = "div#cds-dropdown-button-value.cds-dd2-pseudo-value"
BTN_DROPD_X    = ("//button[@id='cds-dropdown' and "
                  "contains(@class,'cds-dd2-button')]")
OPT_DROPD_X    = ("//ul[@id='cds-dropdown-listbox']/li"
                  "[not(contains(@class,'disabled'))]")

# Open card dropdown
def open_card_dropdown() -> None:
    """Click dropdown button to reveal card list."""
    wait.until(EC.element_to_be_clickable((By.XPATH, BTN_DROPD_X))).click()
print("# open_card_dropdown loaded – dropdown opener ready")

# Retrieve current card label element
def get_label():
    """Return WebElement holding current card label."""
    return wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, CARD_LABEL_CSS)))
print("# get_label loaded – label getter ready")

# ---------------------------------------------------------------------------
# Section: Offer Scraping
# ---------------------------------------------------------------------------
# Scrape & enrol offers for a single card label
def scrape_card(label: str, holder: str, seen: Set[Tuple]) -> None:
    """Collect new offers for one card; always flush collected rows."""
    if get_label().text.strip() != label:
        open_card_dropdown()
        wait.until(EC.element_to_be_clickable(
            (By.XPATH, f"{OPT_DROPD_X}[normalize-space()='{label}']"))
        ).click()
        wait.until(lambda _: get_label().text.strip() == label)

    card, last4 = [s.strip() for s in label.rsplit("-", 1)]

    for _ in range(3):
        if not error_banner_visible():
            break
        driver.refresh()
        time.sleep(1)
    else:
        sheet_log("WARN", "card", f"{label}: banner stuck – skipped")
        return

    driver.execute_script("window.scrollTo(0,0);")
    expand_all()

    new_rows: List[List[str]] = []
    try:
        while (icons := plus_icons()):
            ico = icons[0]
            driver.execute_script(
                "arguments[0].scrollIntoView({block:'center'});", ico)
            ico.click()
            wait.until(lambda _:
                driver.find_elements(By.XPATH,
                    "//div[contains(@class,'enrolled')]") or
                driver.find_elements(By.CSS_SELECTOR,
                    ".mo-modal-img-merchant-name"), 8)

            brand = driver.find_element(By.CSS_SELECTOR,
                        ".mo-modal-img-merchant-name").text.strip()
            disc  = driver.find_element(By.CSS_SELECTOR,
                        ".mo-modal-offer-title div").text.strip()
            body  = driver.find_element(By.CSS_SELECTOR,
                        "cds-column section").text
            maxd  = parse_max_disc(body) or ""
            mins  = parse_min_spend(body) or "None"
            exp   = driver.find_element(By.CSS_SELECTOR,
                        ".mo-modal-header-date span").text.strip()
            local = "Yes" if "philadelphia" in body.lower() else "No"
            added = datetime.today().strftime("%m/%d/%Y")

            row = (holder, last4, card, brand, disc, maxd,
                   mins, added, exp, local)
            if row not in seen:
                new_rows.append(list(row))
                seen.add(row)

            close_modal()
            time.sleep(0.3)
    finally:
        if new_rows:
            try:
                OFFER_WS.append_rows(new_rows, value_input_option="RAW",
                                     insert_data_option="INSERT_ROWS")
                print(f"{card} – added {len(new_rows)} rows (flushed)")
            except Exception as exc:
                sheet_log("ERROR", "append_rows", str(exc))
print("# scrape_card loaded – single card scraper ready")

# ---------------------------------------------------------------------------
# Section: Account-Level Scrape
# ---------------------------------------------------------------------------
# Run full scrape for every card in one account
def scrape_account(acct: dict) -> Set[Tuple]:
    """Login, scrape each card, logout; return set of rows seen."""
    user, pwd, holder = acct["user"], acct["pass"], acct["holder"]
    if not citi_login(user, pwd):
        return set()

    if not visit_offers_page():
        citi_logout()
        return set()

    seen = {tuple(r) for r in OFFER_WS.get_all_values()[1:]}
    open_card_dropdown()
    labels = [li.text.strip() for li in driver.find_elements(By.XPATH,
                                                             OPT_DROPD_X)
              if li.text.strip() and li.text.strip().lower() != "credit"]
    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)

    for lab in labels:
        scrape_card(lab, holder, seen)

    citi_logout()
    return seen
print("# scrape_account loaded – account scraper ready")

# ---------------------------------------------------------------------------
# Section: Sheet Clean-ups
# ---------------------------------------------------------------------------
# Determine if a row is expired based on expiration date
def expired(row: List[str]) -> bool:
    """Return True when Expiration (col I) date is before today."""
    try:
        return datetime.strptime(row[8], "%m/%d/%Y").date() < \
               datetime.today().date()
    except Exception:
        return False
print("# expired loaded – expiry checker ready")

def clean_sheet(current: Set[Tuple]) -> None:
    """Delete offers not seen in this run or already expired."""
    rows = OFFER_WS.get_all_values()
    sid  = OFFER_WS._properties["sheetId"]
    req  = []
    for i in range(len(rows) - 1, 0, -1):
        if tuple(rows[i]) not in current or expired(rows[i]):
            req.append({"deleteRange": {"range": {"sheetId": sid,
                                                  "startRowIndex": i,
                                                  "endRowIndex": i + 1},
                                        "shiftDimension": "ROWS"}})
    if req:
        OFFER_WS.spreadsheet.batch_update({"requests": req})
print("# clean_sheet loaded – stale row cleaner ready")

# Remove duplicate rows
def dedupe_rows() -> None:
    """Delete duplicated rows from Card Offers sheet."""
    rows = OFFER_WS.get_all_values()
    seen = set()
    sid  = OFFER_WS._properties["sheetId"]
    req  = []
    for i in range(len(rows) - 1, 0, -1):
        key = tuple(rows[i])
        if key in seen:
            req.append({"deleteRange": {"range": {"sheetId": sid,
                                                  "startRowIndex": i,
                                                  "endRowIndex": i + 1},
                                        "shiftDimension": "ROWS"}})
        else:
            seen.add(key)
    if req:
        OFFER_WS.spreadsheet.batch_update({"requests": req})
print("# dedupe_rows loaded – duplicate remover ready")

# Refresh basic filter to update dropdown lists
def reset_filters() -> None:
    """Clear and re-apply basic filter to refresh list options."""
    sid = OFFER_WS._properties["sheetId"]
    requests = [
        {"clearBasicFilter": {"sheetId": sid}},
        {"setBasicFilter": {"filter": {
            "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1}
        }}},
    ]
    OFFER_WS.spreadsheet.batch_update({"requests": requests})
    sheet_log("INFO", "reset_filters", "basic filter toggled")
print("# reset_filters loaded – filter reset ready")

# ---------------------------------------------------------------------------
# Section: Graceful Exit Helpers
# ---------------------------------------------------------------------------
# Safely attempt to quit the driver, ignoring session errors
def safe_quit():
    """Close browser if still alive; ignore InvalidSessionId errors."""
    try:
        driver.quit()
    except InvalidSessionIdException:
        pass
print("# safe_quit loaded – graceful driver terminator ready")

# ---------------------------------------------------------------------------
# Section: Main Run (multi-account)
# ---------------------------------------------------------------------------
# Orchestrate entire workflow for all accounts
def main() -> None:
    """Run scraper for all accounts; clean sheet; exit browser gracefully."""
    all_rows: Set[Tuple] = set()
    for acct in ACCOUNTS:
        sheet_log("INFO", "account", f"start {acct['holder']}")
        rows = scrape_account(acct)
        all_rows.update(rows)

    clean_sheet(all_rows)
    dedupe_rows()
    reset_filters()
    sheet_log("INFO", "main", "COMPLETE")
    print("Done ✔")
print("# main loaded – orchestrator ready")

# ---------------------------------------------------------------------------
# Section: Script Entrypoint
# ---------------------------------------------------------------------------
# Execute main and handle manual browser closure gracefully
if __name__ == "__main__":
    try:
        main()
    except (InvalidSessionIdException, WebDriverException):
        print("Browser window closed – exiting gracefully.")
        sheet_log("WARN", "main", "browser closed by user")
    finally:
        safe_quit()
