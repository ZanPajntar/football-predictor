#!/usr/bin/env python3
"""
Scrape FBref Premier-League Scores & Fixtures 2024-2025 + xG + xGA
------------------------------------------------------------------
Ustvari CSV: premier_league_2024_2025_scores_xg_xga.csv
Stolpci:
  home_team, away_team, home_goals, away_goals,
  home_xG, away_xG, home_xGA, away_xGA,
  home_CrdY, away_CrdY
"""

import re
import sys
from io import StringIO
import pandas as pd
from bs4 import BeautifulSoup, Comment
import cloudscraper

BASE_URL = "https://fbref.com"
URL = f"{BASE_URL}/en/comps/9/schedule/Premier-League-Scores-and-Fixtures"

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


def match_report_links(table_html: str) -> list[str]:
    """Extract Match Report links for played games in the same order as rows."""
    soup = BeautifulSoup(table_html, "lxml")
    links: list[str] = []
    for row in soup.select("tbody tr"):
        if row.get("class") and "spacer" in row.get("class"):
            continue
        score = row.find("td", {"data-stat": "score"})
        if not score or not score.get_text(strip=True):
            continue
        cell = row.find("td", {"data-stat": "match_report"})
        href = cell.find("a") if cell else None
        links.append(BASE_URL + href["href"] if href else None)
    return links


def match_yellow_cards(url: str) -> tuple[int | None, int | None]:
    """Return total yellow cards for home and away teams from match report."""
    if url is None:
        return pd.NA, pd.NA
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    tables = soup.select("table[id^='stats_'][id$='_summary']")
    if not tables:
        for com in soup.find_all(string=lambda s: isinstance(s, Comment)):
            sub = BeautifulSoup(com, "lxml")
            tables = sub.select("table[id^='stats_'][id$='_summary']")
            if tables:
                break
    if len(tables) < 2:
        return pd.NA, pd.NA

    def yellow(tab: BeautifulSoup) -> int | None:
        row = tab.find("tfoot").find("tr") if tab.find("tfoot") else tab.find_all("tr")[-1]
        cell = row.find("td", {"data-stat": "cards_yellow"})
        if not cell:
            return pd.NA
        try:
            return int(cell.get_text(strip=True))
        except ValueError:
            return pd.NA

    return yellow(tables[0]), yellow(tables[1])


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
    links = match_report_links(table_html)
    schedule = clean_df(table_html)

    home_y, away_y = [], []
    for url in links:
        h, a = match_yellow_cards(url)
        home_y.append(h)
        away_y.append(a)

    schedule["home_CrdY"] = home_y
    schedule["away_CrdY"] = away_y

    out_file = "premier_league_2024_2025_scores_xg_xga.csv"
    schedule.to_csv(out_file, index=False)
    print(f"Končano. CSV shranjen kot '{out_file}'.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        sys.exit(f"Napaka: {exc}")
