#!/usr/bin/env python3
"""
Premier League 2024-25 | razširjen FBref scrape
Izhod: scrape_pl_24_25_03.csv
"""

import re, sys, time, unicodedata
from io import StringIO
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup, Comment
import cloudscraper, requests

BASE   = "https://fbref.com"
COMP_URL = f"{BASE}/en/comps/9/schedule/Premier-League-Scores-and-Fixtures"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}
SCRAPER = cloudscraper.create_scraper(); SCRAPER.headers.update(HEADERS)
DELAY, MAX_RETRY = 5, 3

# ── HTTP helpers ─────────────────────────────────────────────
def get_html(url, attempt=1):
    try:
        r = SCRAPER.get(url, timeout=30); r.raise_for_status(); return r.text
    except requests.HTTPError as e:
        if r.status_code in (429, 503) and attempt < MAX_RETRY:
            wait = 10 * attempt; print(f"[{r.status_code}] retry v {wait}s …"); time.sleep(wait)
            return get_html(url, attempt+1)
        raise

def table_html(soup, patt):
    tab = soup.find("table", id=re.compile(patt))
    if tab: return tab
    for c in soup.find_all(string=lambda s: isinstance(s, Comment)):
        sub = BeautifulSoup(c, "lxml")
        tab = sub.find("table", id=re.compile(patt))
        if tab: return tab
    return None

# ── 1. Schedule ─────────────────────────────────────────────
def build_schedule():
    soup = BeautifulSoup(get_html(COMP_URL), "lxml")
    df   = pd.read_html(StringIO(str(table_html(soup, r"^sched_"))))[0]
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df = df[df["Score"].notna()].copy()

    df[["home_goals","away_goals"]] = (
        df["Score"].str.replace(r"[^\d–\-]", "", regex=True)
                   .str.replace("–", "-", regex=False)
                   .str.split("-", expand=True).astype("Int64"))
    df = df.drop(columns=["Score", "Match Report"])

    df = df.rename(columns={"Wk":"matchweek", "Home":"home_team", "Away":"away_team",
                            "Date":"date", "xG":"home_xG", "xG.1":"away_xG"})
    df["matchweek"] = df["matchweek"].astype("Int64")
    df["home_xGA"], df["away_xGA"] = df["away_xG"], df["home_xG"]
    df["date"] = pd.to_datetime(df["date"])
    df.insert(1, "match_id", range(1, len(df)+1))

    order = ["matchweek","match_id","date","home_team","away_team",
             "home_goals","away_goals","home_xG","away_xG","home_xGA","away_xGA"]
    return df[order]

# ── 2. Team-metrics ─────────────────────────────────────────
def slugify(name:str)->str:
    s = unicodedata.normalize("NFKD",name).encode("ascii","ignore").decode()
    s = re.sub(r"[^\w\s-]","",s).strip().replace("&","and")
    return re.sub(r"\s+","-",s)

def build_stats_url(href:str, team:str)->str:
    """
    href iz schedule: /en/squads/b8fd03ef/Manchester-City
    → /en/squads/b8fd03ef/Manchester-City-Stats
    """
    parts = href.strip("/").split("/")
    club_id = parts[2]
    slug = parts[3] if len(parts)>3 and not parts[3].startswith("20") else slugify(team)
    if not slug.endswith("Stats"):
        slug += "-Stats"
    return f"/en/squads/{club_id}/{slug}"

def grab_row(soup, patt):
    t = table_html(soup, patt)
    return None if not t else pd.read_html(StringIO(str(t)))[0].iloc[-1]

def team_metrics(url):
    soup = BeautifulSoup(get_html(BASE+url),"lxml"); o={}
    std,stdA = grab_row(soup,"^stats_squads_standard_for$"), grab_row(soup,"^stats_squads_standard_against$")
    if std is not None:  o["npxG"]=float(std["npxG"]);  o["RedCards"]=float(std["Red Card"])
    if stdA is not None: o["npxGA"]=float(stdA["npxG"]); o["OppRed"]=float(stdA["Red Card"])

    sht,shtA = grab_row(soup,"^stats_squads_shooting_for$"), grab_row(soup,"^stats_squads_shooting_against$")
    if sht is not None:  o["xG_SET"]=float(sht["xG Off-Set"]); o["SoT_for"]=float(sht["SoT"])
    if shtA is not None: o["xG_SET_A"]=float(shtA["xG Off-Set"]); o["SoT_ag"]=float(shtA["SoT"])

    gk,gkA = grab_row(soup,"^stats_squads_keeper_for$"), grab_row(soup,"^stats_squads_keeper_against$")
    if gk is not None:   o["PSxG_G_diff"]=float(gk["PSxG+/-"])
    if gkA is not None:  o["PSxG_G_diff_A"]=float(gkA["PSxG+/-"])

    pas,pasA = grab_row(soup,"^stats_squads_passing_for$"), grab_row(soup,"^stats_squads_passing_against$")
    if pas is not None:  o["PenPass_for"]=float(pas["Passes into Pen Area"])
    if pasA is not None: o["PenPass_ag"]=float(pasA["Passes into Pen Area"])

    pos,posA = grab_row(soup,"^stats_squads_possession_for$"), grab_row(soup,"^stats_squads_possession_against$")
    if pos is not None and posA is not None:
        t3f,t3a = float(pos["Touches (Att 3rd)"]), float(posA["Touches (Att 3rd)"])
        o.update({"Touches3rd_for":t3f,"Touches3rd_ag":t3a,"FieldTilt":t3f/(t3f+t3a+1e-6)})
    return o

def collect_metrics():
    soup = BeautifulSoup(get_html(COMP_URL),"lxml")
    m={}
    for a in soup.select("td[data-stat='home_team'] a, td[data-stat='away_team'] a"):
        team=a.text.strip()
        if team in m: continue
        url=build_stats_url(a["href"],team)
        try: m[team]=team_metrics(url)
        except Exception as e: print(f"[WARN] {team}: {e}"); m[team]={}
        time.sleep(DELAY)
    return m

# ── 3. Merge + rest days ────────────────────────────────────
KEYS = ["npxG","npxGA","xG_SET","xG_SET_A","SoT_for","SoT_ag",
        "PSxG_G_diff","PSxG_G_diff_A","PenPass_for","PenPass_ag",
        "Touches3rd_for","Touches3rd_ag","FieldTilt","RedCards","OppRed"]

def merge(df, met):
    pick = lambda r,s,k: met.get(r[f"{s}_team"],{}).get(k,pd.NA)
    for k in KEYS:
        df[f"home_{k}"]=df.apply(lambda r:pick(r,"home",k),axis=1)
        df[f"away_{k}"]=df.apply(lambda r:pick(r,"away",k),axis=1)
    for side in ("home","away"):
        col,last=f"{side}_rest_days",{}
        df[col]=pd.NA
        for i,r in df.iterrows():
            t,d=r[f"{side}_team"],r["date"]
            if t in last: df.at[i,col]=(d-last[t]).days
            last[t]=d
    return df

# ── safe save ────────────────────────────────────────────────
def safe_save(df, fname="scrape_pl_24_25_03.csv"):
    try:
        df.to_csv(fname,index=False)
        print(f"Končano → {fname} ({len(df)} vrstic, {len(df.columns)} stolpcev)")
    except PermissionError:
        alt=fname.replace(".csv",f"_{datetime.now():%Y%m%d_%H%M%S}.csv")
        df.to_csv(alt,index=False)
        print(f"[WARN] '{fname}' zaklenjena.  Shranjeno kot '{alt}'.")

# ── main ─────────────────────────────────────────────────────
def main():
    print("1/3 Schedule"); sched=build_schedule()
    print("2/3 Team metrics (≈2 min)"); met=collect_metrics()
    print("3/3 Merge & save"); full=merge(sched,met); safe_save(full)

if __name__=="__main__":
    try: main()
    except Exception as e: sys.exit(f"Napaka: {e}")
