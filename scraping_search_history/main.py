import os
import time
import random
import glob
import requests
import csv
import traceback
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from pyvirtualdisplay import Display
from flask import Flask, jsonify, send_file
from fake_useragent import UserAgent

app = Flask(__name__)

# adding chromedriver to the path
os.environ['PATH'] += ':'+os.path.dirname(os.path.realpath(__file__))+"/bin"

# ## The urls
# TODO the download folder can be /tmp if deployed in the appengin or could set a volume in the docker
# DOWNLOAD_DIR = "downloads/"
DOWNLOAD_DIR = "/tmp/"
LOGIN_URL = 'https://www.crunchbase.com/login'
FUNDING_ROUND_URL = "https://www.crunchbase.com/lists/funding-rounds-last-24-h/984dd0f3-3ee3-4f59-a71c-a4022ad591c2/funding_rounds"
IPOS_URL = "https://www.crunchbase.com/lists/ipos/607fd0f3-ece9-41bb-a735-928677fdca1e/ipos"
FUNDS_URL = "https://www.crunchbase.com/lists/venture-capital-funds/32468ff7-f748-4393-8af0-8e2e2ae6adaf/funds"
LOGOUT_URL = "https://www.crunchbase.com/logout"

# ## PATHS
ACTIVITY_FEED_XPATH = "/html/body/chrome/div/mat-sidenav-container[@class='mat-drawer-container mat-sidenav-container']/mat-sidenav-content[@class='mat-drawer-content mat-sidenav-content ng-star-inserted']/div/home-feeds[@class='ng-star-inserted']/page-layout/div[@class='page-layout-body']/div/div[@class='center-column-content cb-margin-xlarge-top flex']/div[@class='center-content cb-margin-xlarge-left cb-margin-xlarge-right']/activity-feed/activity-feed-card[@class='ng-star-inserted'][2]/activity-feed-card-press-reference[@class='ng-star-inserted']/activity-layout/mat-card[@class='mat-card mat-focus-indicator']"
FUNDING_ROUND_DOWNLOAD_XPATH = "/html/body/chrome/div/mat-sidenav-container/mat-sidenav-content/div/list-search/page-layout/div/div/form/div[2]/results/div/div/div[1]/div/div/div/export-csv-button/button"
CSV_DOWNLOAD_XPATH = '//button[normalize-space()="Export to CSV"]'

# ## set ups
USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.142 Safari/537.36'


def enable_download(driver):
    driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd':'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': DOWNLOAD_DIR}}
    driver.execute("send_command", params)


def call_browser():
    # Set screen resolution to 1366 x 768 like most 15" laptops
    display = Display(visible=False, size=(1366, 768), backend="xvfb")
    display.start()
    #ua = UserAgent()
    #USER_AGENT = ua.random
    # setting up selenium
    random.seed()
    options = Options()
    options.headless = False
    options.add_argument('--no-sandbox')  # Bypass OS security model
    options.add_argument('--disable-gpu')  # applicable to windows os only
    options.add_argument('start-maximized')  #
    options.add_argument('disable-infobars')
    options.add_argument("--disable-extensions")
    options.add_argument("--enable-javascript")
    options.add_argument("window-size=1366,768")
    options.add_argument(f'user-agent={USER_AGENT}')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_experimental_option("prefs", {
        "savefile.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "download.safebrowsing.enabled": True
    })


    # starting the driver
    driver = webdriver.Chrome(options=options)
    enable_download(driver)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            })
          """
    })
    driver.get(LOGIN_URL)
    print("Page title: ", driver.title, flush=True)
    # driver.get_screenshot_as_file("capture.png")

    x= driver.execute_script("return navigator.userAgent")
    print("agent: ", x, flush=True)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".print-logo")))

    time.sleep(random.randint(2, 10))

    email_form = driver.find_element_by_id("mat-input-1").send_keys("shayan@ensanteinsights.com")
    time.sleep(random.randint(2, 10))
    pass_form = driver.find_element_by_id("mat-input-2").send_keys("QGASupport1!$")
    time.sleep(random.randint(2, 10))
    login_button = driver.find_element_by_css_selector('.login').click()
    time.sleep(random.randint(2, 10))
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, ACTIVITY_FEED_XPATH)))
    time.sleep(random.randint(4, 15))

    # download the funding_round search result
    driver.get(FUNDING_ROUND_URL)
    print("Page title: ", driver.title, flush=True)
    driver.get_screenshot_as_file(DOWNLOAD_DIR+"FR.png")
    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "grid-cell")))
    time.sleep(random.randint(4, 15))

    driver.find_element_by_xpath(CSV_DOWNLOAD_XPATH).click()
    time.sleep(random.randint(4, 15))

    # Download ipos
    driver.get(IPOS_URL)
    print("Page title: ", driver.title, flush=True)
    driver.get_screenshot_as_file(DOWNLOAD_DIR+"ipos.png")
    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "grid-cell")))
    time.sleep(random.randint(4, 15))
    driver.find_element_by_xpath(CSV_DOWNLOAD_XPATH).click()
    time.sleep(random.randint(4, 15))

    # Download Funds
    driver.get(FUNDS_URL)
    print("Page title: ", driver.title, flush=True)
    driver.get_screenshot_as_file(DOWNLOAD_DIR+"Funds.png")
    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "grid-cell")))
    time.sleep(random.randint(4, 15))
    driver.find_element_by_xpath(CSV_DOWNLOAD_XPATH).click()
    time.sleep(random.randint(4, 15))

    # cleaning up
    driver.quit()
    display.stop()
    return True


def csv_handler(csv_file, type):
    """
    Extracts the name, and url of an entity from a csv file.
    @params
    """
    if type == 'FR':
        name = 'Transaction Name'
        url = 'Transaction Name URL'
    elif type == 'F':
        name = 'Fund Name'
        url = 'Fund Name URL'
    elif type == 'IPO':
        name = 'Stock Symbol'
        url = 'Stock Symbol URL'

    with open(csv_file) as file:
        csv_reader = csv.DictReader(file)
        data = []
        for i in csv_reader:
            data.append({'name': i[name], 'url': i[url].split("/")[-1]})

    return data


def send_funds():
    """
    Gets the fund data from csv file and send it to the cloud function.
    """
    # The url for Gcloud function for funds
    url = "https://northamerica-northeast1-cb-data-fetch.cloudfunctions.net/funds_dist"
    path = DOWNLOAD_DIR+'venture-capital-funds*'
    funds_file = glob.glob(path)[0]
    fund_data = csv_handler(funds_file, 'F')
    print(">> Fund_data", flush=True)
    upload = requests.post(url, json=fund_data)
    print("Funds: ", upload.text, upload.status_code, flush=True)
    os.remove(funds_file)


def send_funding_round():
    """
    Gets and sends the fund round data from csv file and send it to the cloud function.
    """
    url = "https://northamerica-northeast1-cb-data-fetch.cloudfunctions.net/fr_dist"
    path = DOWNLOAD_DIR + 'funding-rounds-*'
    funds_file = glob.glob(path)[0]
    fund_data = csv_handler(funds_file, "FR")
    print(">> funding_round_data", flush=True)
    req = requests.post(url, json=fund_data)
    print("Funding Round: ", req.text, req.status_code, flush=True)
    os.remove(funds_file)


def send_ipos():
    """
    Gets and sends the fund round data from csv file and send it to the cloud function.
    """
    url = "https://northamerica-northeast1-cb-data-fetch.cloudfunctions.net/ipos_dist"
    # url = "http://127.0.0.1:8080"
    path = DOWNLOAD_DIR + 'ip-os-*'
    ipo_file = glob.glob(path)[0]
    ipo_data = csv_handler(ipo_file, "IPO")
    print(">> IPO_data", flush=True)
    req = requests.post(url, json=ipo_data)
    print("IPOS: ", req.text, req.status_code, flush=True)
    os.remove(ipo_file)


def clean_up():
    """
    To clean up and removing the csv files.
    """
    path = DOWNLOAD_DIR + "*.csv"
    for f in glob.glob(path):
        os.remove(f)


@app.route("/")
def home():
    """
    Home page.
    """
    clean_up()
    return "Welcome. Nothing to do here."


@app.route("/get_cb_data")
def get_cb():
    """
    Route to start getting the browser data.
    """
    clean_up()
    try:
        call_browser()
    except Exception as e:
        clean_up()
        print(traceback.format_exc(), flush=True)
        return jsonify({'success': False, 'msg': str(e)})
    # return send_file("capture.png")
    try:
        send_ipos()
        send_funds()
        send_funding_round()
    except Exception as e:
        clean_up()
        print(traceback.format_exc(), flush=True)
        return jsonify({'success': False, 'msg': str(e)})

    clean_up()
    return "Done", 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
