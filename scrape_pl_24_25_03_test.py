#!/usr/bin/env python3
"""
Scrape FBref Premier-League 2024-25 z uporabo Seleniuma. (v2.4 - POTRJENA TESTNA VERZIJA)
==========================================================================================

Kaj počne:
-----------
1. Z uporabo Seleniuma prenese glavni razpored sezone.
2. **OBDELA SAMO PRVE 3 TEKME ZA HITRO TESTIRANJE.**
3. Z uporabo potrjene logike iz HTML kode pravilno izlušči rumene kartone (išče v <tfoot>).
4. Vse podatke shrani v testno CSV datoteko.

Odvisnosti:
-----------
pip install pandas beautifulsoup4 lxml selenium webdriver-manager
"""

import re
import sys
import time
import random
from io import StringIO
from typing import Tuple
import pandas as pd
from bs4 import BeautifulSoup, Comment

# Selenium in odvisnosti
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ─────────────────────────────────────────────────────────────
# 1 · Konstante in globalne nastavitve
# ─────────────────────────────────────────────────────────────
BASE_URL = "https://fbref.com"
SCHEDULE_URL = f"{BASE_URL}/en/comps/9/schedule/Premier-League-Scores-and-Fixtures"

LIMIT_MATCHES = 3  # Omejitev za testiranje

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

DELAY_RANGE = (3.0, 6.0)
BACKOFF_RANGE = (20.0, 40.0)
MAX_RETRIES = 4
TIMEOUT = 30

_driver: webdriver.Chrome | None = None

# ─────────────────────────────────────────────────────────────
# 2 · Pomožne funkcije (Selenium & Logging)
# ─────────────────────────────────────────────────────────────

def eprint(*args) -> None:
    sys.stderr.write(" ".join(map(str, args)) + "\n")
    sys.stderr.flush()

def get_driver() -> webdriver.Chrome:
    global _driver
    if _driver is None:
        eprint("Inicializiram Selenium WebDriver (Chrome)...")
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        service = ChromeService(ChromeDriverManager().install())
        _driver = webdriver.Chrome(service=service, options=options)
        eprint("WebDriver je pripravljen.")
    return _driver

def fetch_html_selenium(url: str) -> str:
    driver = get_driver()
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(random.uniform(*DELAY_RANGE))
            eprint(f"Nalagam {url} ...")
            driver.get(url)
            WebDriverWait(driver, TIMEOUT).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
            if "Just a moment..." in driver.title: raise RuntimeError(f"Zaznana Cloudflare blokada.")
            return driver.page_source
        except Exception as exc:
            wait = random.uniform(*BACKOFF_RANGE) * (2 ** attempt)
            eprint(f"[OPOZORILO] {exc}. Čakam {wait:.0f}s pred naslednjim poskusom.")
            time.sleep(wait)
    raise RuntimeError(f"Stran se ni uspela naložiti po {MAX_RETRIES} poskusih: {url}")

# ─────────────────────────────────────────────────────────────
# 3 · Obdelava podatkov
# ─────────────────────────────────────────────────────────────

def get_table_soup(html: str) -> BeautifulSoup:
    soup = BeautifulSoup(html, "lxml")
    tab = soup.find("table", id=re.compile(r"^sched_"))
    if tab: return tab
    for com in soup.find_all(string=lambda s: isinstance(s, Comment)):
        sub_soup = BeautifulSoup(com, "lxml")
        tab = sub_soup.find("table", id=re.compile(r"^sched_"))
        if tab: return tab
    raise RuntimeError("Glavna tabela ni bila najdena.")

def build_dataframe(table_soup: BeautifulSoup) -> pd.DataFrame:
    df = pd.read_html(StringIO(str(table_soup)))[0]
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df['Wk'] = pd.to_numeric(df['Wk'], errors='coerce')
    df.dropna(subset=['Wk'], inplace=True)
    df = df.reset_index(drop=True)
    
    urls = []
    all_rows = table_soup.find_all('tr')
    for row in all_rows:
        home_team_cell = row.find("td", {"data-stat": "home_team"})
        if not home_team_cell or not home_team_cell.get_text(strip=True):
            continue
        report_cell = row.find("td", {"data-stat": "match_report"})
        if report_cell and report_cell.find("a"):
            urls.append(BASE_URL + report_cell.find("a")["href"])
        else:
            urls.append(None)
    
    if len(urls) != len(df):
         raise RuntimeError(f"Neskladje pri zbiranju URL-jev: {len(urls)} vs {len(df)}.")
        
    df['match_report_url'] = urls
    df = df[df["Score"].notna()].copy()
    df = df.rename(columns={"Wk": "matchweek_number"})
    df["matchweek_number"] = df["matchweek_number"].astype("Int64")
    df[["home_goals", "away_goals"]] = (df["Score"].str.replace(r"[^\d–\-]", "", regex=True).str.replace("–", "-", regex=False).str.split("-", expand=True).astype("Int64"))
    df = df.rename(columns={"Home": "home_team", "Away": "away_team", "xG": "home_xG", "xG.1": "away_xG", "Date": "date"})
    df["home_xGA"] = df["away_xG"]
    df["away_xGA"] = df["home_xG"]
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df.insert(1, "match_id", range(1, len(df) + 1))
    return df

def fetch_match_cards(url: str) -> Tuple[int | None, int | None]:
    if not url: return None, None
    try:
        html = fetch_html_selenium(url)
        soup = BeautifulSoup(html, 'lxml')
        
        player_stats_tables = soup.find_all("table", id=lambda x: x and x.startswith("stats_") and x.endswith("_summary"))
        
        if len(player_stats_tables) < 2:
            eprint(f"[OPOZORILO] Na strani {url} nista bili najdeni obe tabeli s statistikami igralcev.")
            return None, None
        
        home_table, away_table = player_stats_tables[0], player_stats_tables[1]

        # POTRJEN IN PRAVILEN SELEKTOR: Išče v nogi tabele (tfoot)
        home_cards_td = home_table.select_one("tfoot td[data-stat='cards_yellow']")
        away_cards_td = away_table.select_one("tfoot td[data-stat='cards_yellow']")

        home_crdY = int(home_cards_td.text) if home_cards_td and home_cards_td.text.strip() else 0
        away_crdY = int(away_cards_td.text) if away_cards_td and away_cards_td.text.strip() else 0
        
        return home_crdY, away_crdY

    except Exception as e:
        eprint(f"[NAPAKA] pri obdelavi {url}: {e}")
        return None, None

# ─────────────────────────────────────────────────────────────
# 4 · Glavni program
# ─────────────────────────────────────────────────────────────

def main() -> None:
    try:
        html = fetch_html_selenium(SCHEDULE_URL)
        table_soup = get_table_soup(html)
        schedule = build_dataframe(table_soup)
        
        if LIMIT_MATCHES and LIMIT_MATCHES > 0:
            eprint(f"\nNajdenih {len(schedule)} odigranih tekem. OMEJUJEM na prvih {LIMIT_MATCHES} za testiranje...")
            schedule_to_process = schedule.head(LIMIT_MATCHES).copy()
        else:
            eprint(f"\nNajdenih {len(schedule)} odigranih tekem. Začenjam z zbiranjem podatkov o kartonih...")
            schedule_to_process = schedule.copy()

        all_home_cards, all_away_cards = [], []

        for index, row in schedule_to_process.iterrows():
            eprint(f"Obdelujem tekmo {row['match_id']}/{len(schedule_to_process)}: {row['home_team']} vs {row['away_team']}")
            home_crdY, away_crdY = fetch_match_cards(row['match_report_url'])
            all_home_cards.append(home_crdY)
            all_away_cards.append(away_crdY)

        schedule_to_process['home_crdY'] = all_home_cards
        schedule_to_process['away_crdY'] = all_away_cards

        final_cols = ["matchweek_number", "match_id", "date", "home_team", "away_team", "home_goals", "away_goals", "home_xG", "away_xG", "home_xGA", "away_xGA", "home_crdY", "away_crdY"]
        final_schedule = schedule_to_process.drop(columns=['match_report_url'], errors='ignore')[final_cols]
        out_file = f"scrape_pl_24_25_TEST_{LIMIT_MATCHES}_matches.csv"
        final_schedule.to_csv(out_file, index=False)
        eprint(f"\n[KONČANO] Podatki za {len(final_schedule)} tekem so shranjeni v datoteko '{out_file}'.")

    finally:
        global _driver
        if _driver:
            eprint("Zapiram brskalnik...")
            _driver.quit()

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        eprint(f"\n[KRITIČNA NAPAKA] Med izvajanjem je prišlo do napake: {exc}")
        sys.exit(1)