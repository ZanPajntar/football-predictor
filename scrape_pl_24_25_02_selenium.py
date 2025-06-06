#!/usr/bin/env python3
"""
Scrape FBref Premier-League 2024-25 z uporabo Seleniuma. (POPRAVLJENA VERZIJA)
=============================================================================

Kaj počne:
-----------
1. Z uporabo Seleniuma in avtomatiziranega brskalnika Chrome prenese
   razpored sezone 2024-25 (Premier League) z FBref.
2. Uporablja mehanizem za ponovne poskuse, naključne pavze in 
   prikrivanje avtomatizacije za večjo robustnost.
3. Izlušči podatke: matchweek, zaporedni match_id, datum, goli, xG, xGA.
4. Rezultat shrani v CSV datoteko 'scrape_pl_24_25_selenium.csv'.

Odvisnosti:
-----------
pip install pandas beautifulsoup4 lxml selenium webdriver-manager
"""

import re
import sys
import time
import random
from io import StringIO
import pandas as pd
from bs4 import BeautifulSoup, Comment

# Selenium in odvisnosti za avtomatizacijo brskalnika
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ─────────────────────────────────────────────────────────────
# 1 · Konstante in globalne nastavitve
# ─────────────────────────────────────────────────────────────
URL = "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
]

DELAY_RANGE = (3.0, 7.0)     # Pavza pred vsakim GET-om (sekunde)
BACKOFF_RANGE = (20.0, 40.0) # Pavza pri ponovnem poskusu (retry)
MAX_RETRIES = 4
TIMEOUT = 30                 # Čas čakanja za nalaganje strani v Selenium (s)

_driver: webdriver.Chrome | None = None

# ─────────────────────────────────────────────────────────────
# 2 · Pomožne funkcije (Selenium & Logging)
# ─────────────────────────────────────────────────────────────

def eprint(*args) -> None:
    """Izpiše sporočilo v stderr, da ne moti morebitnega izpisa podatkov."""
    sys.stderr.write(" ".join(map(str, args)) + "\n")
    sys.stderr.flush()

def get_driver() -> webdriver.Chrome:
    """
    Inicializira in vrne eno instanco brskalnika za celotno sejo (singleton).
    Če že obstaja, vrne obstoječo.
    """
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
    """Naloži stran z uporabo Seleniuma, vključno z logiko za ponovne poskuse."""
    driver = get_driver()
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(random.uniform(*DELAY_RANGE))
            eprint(f"Nalagam {url} ... (Poskus {attempt + 1}/{MAX_RETRIES})")
            driver.get(url)
            
            WebDriverWait(driver, TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
            )
            
            if "Just a moment..." in driver.title or "Verifying you are human" in driver.page_source:
                raise RuntimeError(f"Zaznana Cloudflare blokada.")

            return driver.page_source

        except Exception as exc:
            wait = random.uniform(*BACKOFF_RANGE) * (2 ** attempt)
            eprint(f"[OPOZORILO] {exc}. Čakam {wait:.0f}s pred naslednjim poskusom.")
            time.sleep(wait)
            
    raise RuntimeError(f"Stran se ni uspela naložiti po {MAX_RETRIES} poskusih: {url}")

# ─────────────────────────────────────────────────────────────
# 3 · Obdelava podatkov (logika iz originalne skripte s popravkom)
# ─────────────────────────────────────────────────────────────

def get_table_soup(html: str) -> BeautifulSoup:
    """Poišče glavno tabelo s podatki, tudi če je skrita v HTML komentarju."""
    soup = BeautifulSoup(html, "lxml")

    tab = soup.find("table", id=re.compile(r"^sched_"))
    if tab:
        return tab

    for com in soup.find_all(string=lambda s: isinstance(s, Comment)):
        sub_soup = BeautifulSoup(com, "lxml")
        tab = sub_soup.find("table", id=re.compile(r"^sched_"))
        if tab:
            return tab

    raise RuntimeError("Tabela ni bila najdena – FBref je morda spremenil strukturo strani.")


def build_dataframe(table_soup: BeautifulSoup) -> pd.DataFrame:
    """Pretvori BeautifulSoup objekt tabele v urejen Pandas DataFrame."""
    df = pd.read_html(StringIO(str(table_soup)))[0]
    
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    # --- KLJUČNI POPRAVEK JE TUKAJ ---
    # Odstranimo ponavljajoče se glave, ki jih pandas prebere kot podatkovne vrstice.
    # To naredimo tako, da pretvorimo stolpec 'Wk' v števila in izločimo vrstice,
    # kjer to ni mogoče (npr. kjer je vrednost beseda 'Wk').
    df['Wk'] = pd.to_numeric(df['Wk'], errors='coerce')
    df.dropna(subset=['Wk'], inplace=True)
    # --- KONEC POPRAVKA ---

    # Obdrži samo vrstice z rezultati (odigrane tekme)
    df = df[df["Score"].notna()].copy()

    # Matchweek
    df = df.rename(columns={"Wk": "matchweek_number"})
    # Sedaj bo ta pretvorba delovala, ker so ne-številske vrednosti odstranjene
    df["matchweek_number"] = df["matchweek_number"].astype("Int64")

    # Goals
    df[["home_goals", "away_goals"]] = (
        df["Score"]
        .str.replace(r"[^\d–\-]", "", regex=True)
        .str.replace("–", "-", regex=False)
        .str.split("-", expand=True)
        .astype("Int64")
    )

    # Preimenovanje stolpcev
    df = df.rename(
        columns={
            "Home": "home_team",
            "Away": "away_team",
            "xG": "home_xG",
            "xG.1": "away_xG",
            "Date": "date",
        }
    )

    # Izračun xGA
    df["home_xGA"] = df["away_xG"]
    df["away_xGA"] = df["home_xG"]

    # Formatiranje datuma v ISO format
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # Ustvarjanje zaporednega match_id
    df.insert(1, "match_id", range(1, len(df) + 1))

    # Izbira in ureditev končnih stolpcev
    cols = [
        "matchweek_number", "match_id", "date",
        "home_team", "away_team",
        "home_goals", "away_goals",
        "home_xG", "away_xG",
        "home_xGA", "away_xGA",
    ]
    return df[cols]

# ─────────────────────────────────────────────────────────────
# 4 · Glavni program
# ─────────────────────────────────────────────────────────────

def main() -> None:
    """Glavna funkcija, ki orkestrira celoten proces."""
    try:
        html = fetch_html_selenium(URL)
        table_soup = get_table_soup(html)
        schedule = build_dataframe(table_soup)

        out_file = "scrape_pl_24_25_selenium.csv"
        schedule.to_csv(out_file, index=False)
        eprint(f"\n[KONČANO] Podatki so uspešno shranjeni v datoteko '{out_file}'.")

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