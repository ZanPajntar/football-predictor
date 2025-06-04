#!/usr/bin/env python3
"""
Scrape FBref Premier-League Scores & Fixtures 2024-2025 + xG
------------------------------------------------------------
Ustvari CSV: premier_league_2024_2025_scores_xg.csv
"""

import re
import sys
import pandas as pd
from bs4 import BeautifulSoup, Comment
import cloudscraper

URL = "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://fbref.com/",
}


def fetch_html(url: str) -> str:
    """Obide Cloudflare in vrne HTML."""
    scraper = cloudscraper.create_scraper()
    scraper.headers.update(HEADERS)
    resp = scraper.get(url, timeout=30)
    if resp.status_code == 403:
        raise RuntimeError(
            "Še vedno 403 – tudi cloudscraper ni uspel. "
            "Poskusi kasneje ali uporabi selenium/playwright."
        )
    resp.raise_for_status()
    return resp.text


def pick_table_from_html(html: str) -> str:
    """Vrne <table> kot HTML-niz, ne glede na to, ali je v DOM-u ali v komentarju."""
    soup = BeautifulSoup(html, "lxml")

    # --- 1) Poskusi najti v živem DOM-u ----------------------------
    direct = soup.find("table", id=re.compile(r"^sched_"))
    if direct:
        return str(direct)

    # --- 2) Skeniraj komentarje -----------------------------------
    for com in soup.find_all(string=lambda s: isinstance(s, Comment)):
        sub = BeautifulSoup(com, "lxml")
        tab = sub.find("table", id=re.compile(r"^sched_"))
        if tab:
            return str(tab)

    raise RuntimeError("Tabela ni bila najdena (niti v DOM-u niti v komentarjih).")


def clean_df(table_html: str) -> pd.DataFrame:
    """Pretvori table_html v DataFrame in preuredi stolpce."""
    df = pd.read_html(table_html)[0]

    # poravnaj morebiten multi-header
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    # obdrži le odigrane tekme (Score ni NaN)
    df = df[df["Score"].notna()].copy()

    # razbij 'Score' v home_goals / away_goals
    df[["home_goals", "away_goals"]] = (
        df["Score"]
        .str.replace(r"[^\d–\-]", "", regex=True)
        .str.replace("–", "-", regex=False)        # en-dash → minus
        .str.split("-", expand=True)
        .astype("Int64")
    )

    # preimenuj ključe
    df = df.rename(
        columns={
            "Home": "home_team",
            "Away": "away_team",
            "xG": "home_xG",
            "xG.1": "away_xG",
        }
    )

    return df[
        ["home_team", "away_team",
         "home_goals", "away_goals",
         "home_xG", "away_xG"]
    ]


def main() -> None:
    html = fetch_html(URL)
    table_html = pick_table_from_html(html)
    schedule = clean_df(table_html)
    out_file = "premier_league_2024_2025_scores_xg.csv"
    schedule.to_csv(out_file, index=False)
    print(f"Končano. CSV shranjen kot '{out_file}'.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        sys.exit(f"Napaka: {exc}")
