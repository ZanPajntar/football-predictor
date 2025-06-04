#!/usr/bin/env python3
"""
Napoved Tottenham – Brighton (25 May 2025)
• xG-Poisson + home advantage + bivariantni λ3
• Train MW 1-30 | valid MW 31-37
• 20 % forma (zadnjih 5 tekem)
• 100 000 simulacij
"""

import sys, pathlib, random, numpy as np, pandas as pd

CSV_DEFAULT  = "scrape_pl_24_25_02.csv"
MATCH_DATE   = pd.Timestamp("2025-05-25")
HOME_TEAM    = "Tottenham"
AWAY_TEAM    = "Brighton"
SIMS         = 100_000
FORM_WEIGHT  = 0.20

# ──────────────────────────────────────────────────────────────
def load_matches(csv_path):
    df = pd.read_csv(csv_path, parse_dates=["date"])
    train  = df[df["matchweek_number"] <= 30]
    valida = df[(df["matchweek_number"] >= 31) & (df["matchweek_number"] <= 37)]
    played = df[df["date"] < MATCH_DATE]
    return played, train, valida

def league_avgs(df):
    return df["home_xG"].mean(), df["away_xG"].mean()

def team_strengths(df, team):
    h = df[df["home_team"] == team]
    a = df[df["away_team"] == team]
    return dict(
        att_home = h["home_xG"].mean(),
        att_away = a["away_xG"].mean(),
        def_home = h["away_xG"].mean(),
        def_away = a["home_xG"].mean(),
    )

def build_tables(train):
    home_avg, away_avg = league_avgs(train)
    H_att, A_att, H_def, A_def = {}, {}, {}, {}
    for t in set(train["home_team"]).union(train["away_team"]):
        st = team_strengths(train, t)
        H_att[t] = st["att_home"] / home_avg
        A_att[t] = st["att_away"] / away_avg
        H_def[t] = st["def_home"] / away_avg
        A_def[t] = st["def_away"] / home_avg
    return H_att, A_att, H_def, A_def, home_avg, away_avg

def form_adjust(df, team, n=5):
    recent = df[((df["home_team"] == team) | (df["away_team"] == team)) &
                (df["date"] < MATCH_DATE)].sort_values("date").tail(n)
    if recent.empty:
        return 0.0
    diff = recent.apply(
        lambda r: r["home_xG"] - r["away_xG"] if r["home_team"] == team
        else r["away_xG"] - r["home_xG"], axis=1)
    return diff.mean()

def shared_lambda(df):
    cov = np.cov(df["home_goals"], df["away_goals"], ddof=0)[0, 1]
    return max(cov, 0.01)

def calibrate_scaling(val_df, H_att, A_att, H_def, A_def, home_avg, away_avg):
    p_h, p_a, o_h, o_a = [], [], [], []
    for _, r in val_df.iterrows():
        h, a = r["home_team"], r["away_team"]
        lam_h = home_avg * H_att[h] * A_def[a]
        lam_a = away_avg * A_att[a] * H_def[h]
        p_h.append(lam_h); p_a.append(lam_a)
        o_h.append(r["home_xG"]); o_a.append(r["away_xG"])
    scale_h = np.mean(o_h) / np.mean(p_h) if p_h else 1.0
    scale_a = np.mean(o_a) / np.mean(p_a) if p_a else 1.0
    return scale_h, scale_a

def simulate(lh, la, ls, sims=SIMS, rng=None):
    rng = rng or np.random.default_rng()
    ls = min(ls, lh * 0.9, la * 0.9)
    S = rng.poisson(ls, sims)
    H = rng.poisson(lh - ls, sims) + S
    A = rng.poisson(la - ls, sims) + S
    return H, A

# ──────────────────────────────────────────────────────────────
def main(csv):
    played, train, valida = load_matches(csv)
    H_att, A_att, H_def, A_def, home_avg, away_avg = build_tables(train)

    # kalibracija
    sH, sA = calibrate_scaling(valida, H_att, A_att, H_def, A_def,
                               home_avg, away_avg)
    home_avg *= sH
    away_avg *= sA

    # forma
    form_tot = form_adjust(played, HOME_TEAM)
    form_bha = form_adjust(played, AWAY_TEAM)
    f_tot = 1 + FORM_WEIGHT * form_tot / home_avg
    f_bha = 1 + FORM_WEIGHT * (-form_bha) / away_avg

    # λ-ji
    λ_home = home_avg * H_att[HOME_TEAM] * A_def[AWAY_TEAM] * f_tot
    λ_away = away_avg * A_att[AWAY_TEAM] * H_def[HOME_TEAM] * f_bha
    λ_shared = shared_lambda(train)

    H, A = simulate(λ_home, λ_away, λ_shared)

    # statistika
    pH, pX, pA = np.mean(H > A), np.mean(H == A), np.mean(H < A)
    btts  = np.mean((H > 0) & (A > 0))
    over25 = np.mean((H + A) > 2.5)

    # --- POPRAVLJEN del: top 5 rezultatov -----------------------
    uniq, cnt = np.unique(list(zip(H, A)), axis=0, return_counts=True)
    pairs = [tuple(u) for u in uniq]               # ndarray → tuple
    top5 = sorted(zip(cnt / len(H), pairs),
                  key=lambda t: t[0], reverse=True)[:5]
    # ------------------------------------------------------------

    print("\n=== Tottenham – Brighton, 25 May 2025 ===")
    print(f"λ_home={λ_home:.2f}, λ_away={λ_away:.2f}, λ_shared={λ_shared:.2f}")
    print(f"\n  Spurs   zmaga : {pH:6.2%}")
    print(f"  Remi (X)     : {pX:6.2%}")
    print(f"  Brighton zmaga: {pA:6.2%}")
    print(f"\n  BTTS          : {btts:6.2%}")
    print(f"  Over 2.5 gola : {over25:6.2%}\n")
    print("  Top 5 izidov:")
    for p, (h, a) in top5:
        print(f"    {h}-{a}: {p:6.2%}")

    # shrani simulacije
    pd.DataFrame({"home": H, "away": A}).to_csv("sim_outcomes.csv", index=False)

# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    csv = sys.argv[1] if len(sys.argv) > 1 else CSV_DEFAULT
    if not pathlib.Path(csv).exists():
        sys.exit(f"CSV datoteka '{csv}' ne obstaja.")
    random.seed(42); np.random.seed(42)
    main(csv)
