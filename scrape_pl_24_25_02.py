#!/usr/bin/env python3
"""
Scrape FBref Premier-League 2024-25:
  matchweek, zaporedni match_id, datum, goli, xG, xGA
----------------------------------------------------------------
Ustvari CSV: scrape_pl_24_25_02.csv
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
            "HTTP 403 – cloudscraper ni uspel. "
            "Poskusi kasneje ali uporabi selenium/playwright."
        )
    resp.raise_for_status()
    return resp.text


def get_table_soup(html: str) -> BeautifulSoup:
    soup = BeautifulSoup(html, "lxml")

    tab = soup.find("table", id=re.compile(r"^sched_"))
    if tab:
        return tab

    for com in soup.find_all(string=lambda s: isinstance(s, Comment)):
        sub = BeautifulSoup(com, "lxml")
        tab = sub.find("table", id=re.compile(r"^sched_"))
        if tab:
            return tab

    raise RuntimeError("Tabela ni bila najdena – FBref je spremenil strukturo.")


def build_dataframe(table_soup: BeautifulSoup) -> pd.DataFrame:
    df = pd.read_html(StringIO(str(table_soup)))[0]
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    df = df[df["Score"].notna()].copy()

    # Matchweek
    df = df.rename(columns={"Wk": "matchweek_number"})
    df["matchweek_number"] = df["matchweek_number"].astype("Int64")

    # Goals
    df[["home_goals", "away_goals"]] = (
        df["Score"]
        .str.replace(r"[^\d–\-]", "", regex=True)
        .str.replace("–", "-", regex=False)
        .str.split("-", expand=True)
        .astype("Int64")
    )

    # Rename
    df = df.rename(
        columns={
            "Home": "home_team",
            "Away": "away_team",
            "xG": "home_xG",
            "xG.1": "away_xG",
            "Date": "date",
        }
    )

    # xGA
    df["home_xGA"] = df["away_xG"]
    df["away_xGA"] = df["home_xG"]

    # ISO date
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # Sequential match_id
    df.insert(1, "match_id", range(1, len(df) + 1))

    cols = [
        "matchweek_number", "match_id", "date",
        "home_team", "away_team",
        "home_goals", "away_goals",
        "home_xG", "away_xG",
        "home_xGA", "away_xGA",
    ]
    return df[cols]


def main() -> None:
    html = fetch_html(URL)
    table_soup = get_table_soup(html)
    schedule = build_dataframe(table_soup)

    out_file = "scrape_pl_24_25_02.csv"     # ← nova izhodna datoteka
    schedule.to_csv(out_file, index=False)
    print(f"Končano. CSV shranjen kot '{out_file}'.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        sys.exit(f"Napaka: {exc}")
