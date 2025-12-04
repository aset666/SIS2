"" 
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("flashscore")


class FlashscoreScraper:
    """Stable Flashscore scraper (2025 version)."""

    def __init__(self, headless: bool = True, timeout: int = 12):
        self.headless = headless
        self.timeout = timeout
        self.driver = None
        self.url = "https://www.flashscore.com/football/"

    # ---------------------------------------------------------------------------
    # Driver setup
    # ---------------------------------------------------------------------------
    def _setup_driver(self):
        log.info("Launching Chrome WebDriver...")

        opts = Options()
        if self.headless:
            opts.add_argument("--headless=new")

        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1600,1000")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--log-level=3")
        opts.add_argument("--disable-logging")

        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=opts)
        self.driver.set_page_load_timeout(self.timeout)

    # ---------------------------------------------------------------------------
    # Cookies popup close
    # ---------------------------------------------------------------------------
    def _close_popups(self):
        time.sleep(2)
        selectors = [
            "#onetrust-accept-btn-handler",
            "button[class*='accept']",
        ]

        for s in selectors:
            try:
                btns = self.driver.find_elements(By.CSS_SELECTOR, s)
                if btns:
                    btns[0].click()
                    return
            except:
                pass

    # ---------------------------------------------------------------------------
    # Scrolling for full match list
    # ---------------------------------------------------------------------------
    def _scroll(self, count: int = 7):
        """Scroll page to load more matches."""
        last_height = self.driver.execute_script("return document.body.scrollHeight")

        for i in range(count):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.6)

            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        # return to top
        self.driver.execute_script("window.scrollTo(0, 0);")

    # ---------------------------------------------------------------------------
    # Parsing a single match (stable selectors 2025)
    # ---------------------------------------------------------------------------
    def _parse_match(self, elem, index: int) -> Dict[str, Any]:

        def safe(sel):
            try:
                return elem.find_element(By.CSS_SELECTOR, sel).text.strip()
            except:
                return None

        match = {
            "id": f"match_{index}_{int(time.time())}",
            "scraped_at": datetime.utcnow().isoformat()
        }

        # NEW Flashscore selectors (2025)
        match["home_team"] = safe(".participant__participantName--home")
        match["away_team"] = safe(".participant__participantName--away")

        match["home_score"] = safe(".detailScore__score--home")
        match["away_score"] = safe(".detailScore__score--away")

        match["time_status"] = safe(".eventRow__time")

        # stage detection
        if match["home_score"] and match["away_score"]:
            match["stage"] = "FINISHED"
        elif match["time_status"] and match["time_status"].upper() in ["FT", "AET", "HT"]:
            match["stage"] = "FINISHED"
        else:
            match["stage"] = "SCHEDULED"

        # league
        try:
            parent = elem.find_element(
                By.XPATH, "./ancestor::div[contains(@class,'event__header')]"
            )
            match["league"] = parent.find_element(By.CSS_SELECTOR, ".event__title").text
        except:
            match["league"] = None

        return match

    # ---------------------------------------------------------------------------
    # Extract all matches from page
    # ---------------------------------------------------------------------------
    def _extract_matches(self) -> List[Dict[str, Any]]:
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".event__match"))
            )
        except TimeoutException:
            log.error("Match elements did not load.")
            return []

        elems = self.driver.find_elements(By.CSS_SELECTOR, ".event__match")
        results = []

        for idx, el in enumerate(elems):
            try:
                results.append(self._parse_match(el, idx))
            except Exception as e:
                log.warning(f"Parse error on match {idx}: {e}")

        return results

    # ---------------------------------------------------------------------------
    # Main scrape
    # ---------------------------------------------------------------------------
    def scrape(self, save_path: str = None):
        self._setup_driver()

        try:
            log.info(f"Opening: {self.url}")
            self.driver.get(self.url)

            self._close_popups()
            self._scroll()

            matches = self._extract_matches()

            if save_path:
                Path(save_path).parent.mkdir(parents=True, exist_ok=True)
                with open(save_path, "w", encoding="utf-8") as f:
                    json.dump(matches, f, indent=2, ensure_ascii=False)
                log.info(f"Saved → {save_path}")

        finally:
            self.driver.quit()

        return matches


# ------------------------------------------------------------------------------
# Run test
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    scraper = FlashscoreScraper(headless=False)
    data = scraper.scrape("flashscore_raw.json")

    print("Collected:", len(data))
    if data:
        print(json.dumps(data[0], indent=2, ensure_ascii=False))
