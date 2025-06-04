#!/usr/bin/env python3
"""
Scrape FBref Premier-League Scores & Fixtures 2024-2025 + xG + xGA
------------------------------------------------------------------
Ustvari CSV: premier_league_2024_2025_scores_xg_xga.csv
Stolpci:
  home_team, away_team, home_goals, away_goals,
  home_xG, away_xG, home_xGA, away_xGA
"""

import re
import sys
from io import StringIO
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
    soup = BeautifulSoup(html, "lxml")

    # 1) v živem DOM-u
    direct = soup.find("table", id=re.compile(r"^sched_"))
    if direct:
        return str(direct)

    # 2) v komentarjih
    for com in soup.find_all(string=lambda s: isinstance(s, Comment)):
        sub = BeautifulSoup(com, "lxml")
        tab = sub.find("table", id=re.compile(r"^sched_"))
        if tab:
            return str(tab)

    raise RuntimeError("Tabela ni bila najdena (niti v DOM-u niti v komentarjih).")


def clean_df(table_html: str) -> pd.DataFrame:
    df = pd.read_html(StringIO(table_html))[0]

    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df = df[df["Score"].notna()].copy()

    df[["home_goals", "away_goals"]] = (
        df["Score"]
        .str.replace(r"[^\d–\-]", "", regex=True)
        .str.replace("–", "-", regex=False)
        .str.split("-", expand=True)
        .astype("Int64")
    )

    df = df.rename(
        columns={
            "Home": "home_team",
            "Away": "away_team",
            "xG": "home_xG",
            "xG.1": "away_xG",
        }
    )

    # --- NOVO: izračun xGA ----------------------------------------
    df["home_xGA"] = df["away_xG"]
    df["away_xGA"] = df["home_xG"]

    return df[
        ["home_team", "away_team",
         "home_goals", "away_goals",
         "home_xG", "away_xG",
         "home_xGA", "away_xGA"]
    ]


def main() -> None:
    html = fetch_html(URL)
    table_html = pick_table_from_html(html)
    schedule = clean_df(table_html)
    out_file = "premier_league_2024_2025_scores_xg_xga.csv"
    schedule.to_csv(out_file, index=False)
    print(f"Končano. CSV shranjen kot '{out_file}'.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        sys.exit(f"Napaka: {exc}")
