from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from datetime import datetime
import sqlite3, json, threading, time, subprocess, os, urllib.request, ssl

app = Flask(__name__, static_folder='.')
CORS(app)

CLUB_ID = "1667112"
DB = "/app/data/stats.db"
PORT = 8080

def db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con

def init():
    con = db()
    con.executescript("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY, timestamp INTEGER,
            goals_for INTEGER, goals_against INTEGER,
            outcome TEXT, opponent TEXT, raw_json TEXT
        );
        CREATE TABLE IF NOT EXISTS player_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id TEXT, player_name TEXT, position TEXT,
            goals INTEGER DEFAULT 0, assists INTEGER DEFAULT 0,
            shots INTEGER DEFAULT 0, pass_attempts INTEGER DEFAULT 0,
            passes_made INTEGER DEFAULT 0, tackle_attempts INTEGER DEFAULT 0,
            tackles_made INTEGER DEFAULT 0, rating REAL DEFAULT 0,
            mom INTEGER DEFAULT 0, seconds_played INTEGER DEFAULT 0,
            clean_sheet_gk INTEGER DEFAULT 0, clean_sheet_def INTEGER DEFAULT 0,
            interceptions INTEGER DEFAULT 0, clearances INTEGER DEFAULT 0,
            blocks INTEGER DEFAULT 0, dribbles_attempted INTEGER DEFAULT 0,
            dribbles_completed INTEGER DEFAULT 0, key_passes INTEGER DEFAULT 0,
            chances_created INTEGER DEFAULT 0, aerial_won INTEGER DEFAULT 0,
            aerial_lost INTEGER DEFAULT 0, headed_goals INTEGER DEFAULT 0,
            sprints INTEGER DEFAULT 0, touches INTEGER DEFAULT 0,
            possession_lost INTEGER DEFAULT 0, yellow_cards INTEGER DEFAULT 0,
            own_goals INTEGER DEFAULT 0, fouls_committed INTEGER DEFAULT 0,
            UNIQUE(match_id, player_name)
        );
    """)
    con.commit()
    con.close()

EVENT = {
    1:"goals_e", 8:"shots_on", 13:"headed_goals", 32:"interceptions",
    33:"blocks", 34:"clearances", 35:"aerial_won", 36:"aerial_lost",
    37:"fouls_committed", 99:"yellow_cards", 100:"sprints",
    106:"drib_att", 107:"drib_ok", 112:"key_passes", 114:"chances_created",
    215:"touches", 219:"duel_lost", 238:"own_goals", 97:"possession_lost"
}

def parse_ev(s):
    r = {}
    for part in str(s or "").split(","):
        if ":" not in part: continue
        try:
            k, v = part.strip().split(":", 1)
            n = EVENT.get(int(k))
            if n: r[n] = r.get(n, 0) + int(v)
        except: pass
    return r

def store(data):
    if not data: return 0
    con = db()
    cur = con.cursor()
    new = 0
    for m in data:
        mid = str(m.get("matchId", ""))
        if not mid or cur.execute("SELECT 1 FROM matches WHERE match_id=?", (mid,)).fetchone():
            continue
        clubs = m.get("clubs", {})
        ours  = clubs.get(CLUB_ID, {})
        gf = int(ours.get("goals", 0))
        ga = int(ours.get("goalsAgainst", 0))
        wins = int(ours.get("wins", 0))
        losses = int(ours.get("losses", 0))
        ties = int(ours.get("ties", 0))
        outcome = "W" if wins else "L" if losses else "D" if ties else "?"
        opp = next((cd.get("details",{}).get("name", cid)
                    for cid, cd in clubs.items() if cid != CLUB_ID), "?")
        cur.execute("INSERT OR IGNORE INTO matches VALUES (?,?,?,?,?,?,?)",
            (mid, m.get("timestamp",0), gf, ga, outcome, opp, json.dumps(m)))
        for pid, pd in m.get("players",{}).get(CLUB_ID,{}).items():
            ev = {}
            for k in ["match_event_aggregate_0","match_event_aggregate_1"]:
                for ek, ev2 in parse_ev(pd.get(k,"")).items():
                    ev[ek] = ev.get(ek, 0) + ev2
            e = lambda k: ev.get(k, 0)
            cur.execute("""INSERT OR IGNORE INTO player_stats (
                match_id, player_name, position,
                goals, assists, shots, pass_attempts, passes_made,
                tackle_attempts, tackles_made, rating, mom, seconds_played,
                clean_sheet_gk, clean_sheet_def, interceptions, clearances,
                blocks, dribbles_attempted, dribbles_completed, key_passes,
                chances_created, aerial_won, aerial_lost, headed_goals,
                sprints, touches, possession_lost, yellow_cards, own_goals,
                fouls_committed
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                mid, pd.get("playername", pid), pd.get("pos",""),
                int(pd.get("goals",0)), int(pd.get("assists",0)),
                int(pd.get("shots",0)), int(pd.get("passattempts",0)),
                int(pd.get("passesmade",0)), int(pd.get("tackleattempts",0)),
                int(pd.get("tacklesmade",0)), float(pd.get("rating",0)),
                int(pd.get("mom",0)), int(pd.get("secondsPlayed",0)),
                int(pd.get("cleansheetsgk",0)), int(pd.get("cleansheetsdef",0)),
                e("interceptions"), e("clearances"), e("blocks"),
                e("drib_att"), e("drib_ok"), e("key_passes"), e("chances_created"),
                e("aerial_won"), e("aerial_lost"), e("headed_goals"),
                e("sprints"), e("touches"), e("possession_lost"),
                e("yellow_cards"), e("own_goals"), e("fouls_committed")
            ))
        new += 1
    con.commit()
    con.close()
    return new

@app.route("/api/ingest", methods=["POST"])
def ingest():
    data = request.get_json()
    n = store(data)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ingest: {n} new match(es)")
    return jsonify({"new_matches": n})

@app.route("/api/stats")
def stats():
    con = db()
    rows = con.execute("""
        SELECT player_name, COUNT(*) games,
               MAX(position) position,
               ROUND(AVG(CASE WHEN rating>3 THEN rating END),2) avg_rating,
               SUM(goals) goals, SUM(assists) assists, SUM(shots) shots,
               SUM(pass_attempts) pass_att, SUM(passes_made) pass_ok,
               SUM(tackle_attempts) tackle_att, SUM(tackles_made) tackle_ok,
               SUM(mom) motm, SUM(seconds_played) secs,
               SUM(clean_sheet_gk) cs_gk, SUM(clean_sheet_def) cs_def,
               SUM(interceptions) ints, SUM(clearances) clears,
               SUM(blocks) blocks, SUM(dribbles_attempted) drib_att,
               SUM(dribbles_completed) drib_ok, SUM(key_passes) kp,
               SUM(chances_created) cc, SUM(aerial_won) air_w,
               SUM(aerial_lost) air_l, SUM(headed_goals) hg,
               SUM(sprints) sprints, SUM(touches) touches,
               SUM(possession_lost) poss_lost, SUM(yellow_cards) yc,
               SUM(own_goals) og, SUM(fouls_committed) fouls
        FROM player_stats GROUP BY player_name ORDER BY games DESC
    """).fetchall()
    wr = con.execute("""
        SELECT ps.player_name,
               SUM(CASE WHEN m.outcome='W' THEN 1 ELSE 0 END) wins,
               COUNT(*) played
        FROM player_stats ps JOIN matches m ON ps.match_id=m.match_id
        GROUP BY ps.player_name
    """).fetchall()
    wm = {r["player_name"]: (r["wins"], r["played"]) for r in wr}
    con.close()
    out = []
    for r in rows:
        d = dict(r)
        g = d["games"]
        w, p = wm.get(d["player_name"], (0, max(g,1)))
        d["winrate"]    = round(w/p*100) if p else 0
        d["pass_pct"]   = round(d["pass_ok"]/d["pass_att"]*100) if d["pass_att"] else 0
        d["shot_pct"]   = round(d["goals"]/d["shots"]*100) if d["shots"] else 0
        d["drib_pct"]   = round(d["drib_ok"]/d["drib_att"]*100) if d["drib_att"] else 0
        d["aerial_pct"] = round(d["air_w"]/(d["air_w"]+d["air_l"])*100) if (d["air_w"]+d["air_l"]) else 0
        d["gpg"]        = round(d["goals"]/g, 2) if g else 0
        d["apg"]        = round(d["assists"]/g, 2) if g else 0
        d["minutes"]    = round(d["secs"]/60)
        out.append(d)
    return jsonify(out)

@app.route("/api/matches")
def matches():
    con = db()
    rows = con.execute("""
        SELECT match_id, timestamp,
               goals_for||'-'||goals_against||' vs '||opponent AS result,
               outcome FROM matches ORDER BY timestamp DESC LIMIT 30
    """).fetchall()
    con.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/match/<mid>")
def match_detail(mid):
    con = db()
    rows = con.execute("""
        SELECT player_name, position, goals, assists, shots,
               pass_attempts, passes_made, tackle_attempts, tackles_made,
               interceptions, clearances, blocks, dribbles_attempted,
               dribbles_completed, aerial_won, aerial_lost, key_passes,
               chances_created, sprints, touches, possession_lost,
               fouls_committed, yellow_cards, mom, rating, seconds_played
        FROM player_stats WHERE match_id=? ORDER BY rating DESC
    """, (mid,)).fetchall()
    con.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/status")
def status():
    con = db()
    tm = con.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
    tp = con.execute("SELECT COUNT(DISTINCT player_name) FROM player_stats").fetchone()[0]
    con.close()
    return jsonify({"matches": tm, "players": tp})

@app.route("/")
def index():
    return send_from_directory(".", "dashboard.html")

@app.route("/<path:f>")
def static_files(f):
    return send_from_directory(".", f)

def auto_fetch():
    while True:
        time.sleep(30)
        try:
            ctx = ssl._create_unverified_context()
            req = urllib.request.Request(
                "https://proclubs.ea.com/api/fc/clubs/matches?platform=common-gen5&matchType=leagueMatch&clubIds=1667112",
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://proclubstracker.com/",
                    "Origin": "https://proclubstracker.com",
                }
            )
            with urllib.request.urlopen(req, timeout=15, context=ctx) as r:
                data = json.loads(r.read())
                n = store(data)
                print(f"[AUTO] {n} new match(es)")
        except Exception as e:
            print(f"[AUTO] {e}")

if __name__ == "__main__":
    init()
    port = int(os.environ.get("PORT", PORT))
    threading.Thread(target=auto_fetch, daemon=True).start()
    print(f"Video United FC -> http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
