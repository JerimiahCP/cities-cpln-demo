"""
app.py — World Cities Explorer
================================
A Flask app backed by PostgreSQL (RDS via Cloud Wormhole).
Demonstrates ephemeral dev environments on Control Plane.

Required env vars:
  DB_HOST     Postgres hostname          (default: rds-postgres)
  DB_PORT     Postgres port              (default: 5432)
  DB_NAME     Database name              (default: cities)
  DB_USER     Database user              (default: cities)
  DB_PASS     Database password          (default: CitiesPass123)
  DEV_MODE    "local" or "ephemeral"     (default: ephemeral)
  PORT        HTTP port                  (default: 8080)
"""

import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, render_template, request, jsonify, redirect, url_for

app = Flask(__name__)

DEV_MODE = os.environ.get("DEV_MODE", "ephemeral").upper()

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "pg-postgres.cities-demo.cpln.local"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "dbname":   os.environ.get("DB_NAME", "cities"),
    "user":     os.environ.get("DB_USER", "cities"),
    "password": os.environ.get("DB_PASS", "CitiesPass123"),
}

SEED_CITIES = [
    ("Tokyo", "Japan", 13960000, 35.68, 139.69, "Asia/Tokyo"),
    ("Delhi", "India", 32941000, 28.66, 77.23, "Asia/Kolkata"),
    ("Shanghai", "China", 24870000, 31.23, 121.47, "Asia/Shanghai"),
    ("São Paulo", "Brazil", 12330000, -23.55, -46.63, "America/Sao_Paulo"),
    ("Mexico City", "Mexico", 9209944, 19.43, -99.13, "America/Mexico_City"),
    ("Cairo", "Egypt", 21323000, 30.06, 31.25, "Africa/Cairo"),
    ("Mumbai", "India", 20667656, 19.08, 72.88, "Asia/Kolkata"),
    ("Beijing", "China", 21540000, 39.90, 116.40, "Asia/Shanghai"),
    ("Dhaka", "Bangladesh", 22478116, 23.72, 90.41, "Asia/Dhaka"),
    ("Osaka", "Japan", 19059856, 34.69, 135.50, "Asia/Tokyo"),
    ("New York", "USA", 8336000, 40.71, -74.01, "America/New_York"),
    ("Karachi", "Pakistan", 17616000, 24.86, 67.01, "Asia/Karachi"),
    ("Buenos Aires", "Argentina", 3054300, -34.61, -58.38, "America/Argentina/Buenos_Aires"),
    ("Istanbul", "Turkey", 15460000, 41.01, 28.95, "Europe/Istanbul"),
    ("Lagos", "Nigeria", 15388000, 6.46, 3.38, "Africa/Lagos"),
    ("London", "United Kingdom", 9748000, 51.51, -0.13, "Europe/London"),
    ("Bangkok", "Thailand", 10723000, 13.75, 100.52, "Asia/Bangkok"),
    ("Paris", "France", 2161000, 48.86, 2.35, "Europe/Paris"),
    ("Lima", "Peru", 10883000, -12.05, -77.05, "America/Lima"),
    ("Los Angeles", "USA", 3979576, 34.05, -118.24, "America/Los_Angeles"),
]


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def init_db():
    """Create the cities table and seed it if empty. Retries on startup."""
    for attempt in range(30):
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cities (
                    id         SERIAL PRIMARY KEY,
                    city       VARCHAR(100) NOT NULL,
                    country    VARCHAR(100) NOT NULL,
                    population BIGINT,
                    lat        DECIMAL(9,6),
                    lng        DECIMAL(9,6),
                    timezone   VARCHAR(50)
                )
            """)
            cur.execute("SELECT COUNT(*) FROM cities")
            count = cur.fetchone()[0]
            if count == 0:
                cur.executemany(
                    "INSERT INTO cities (city, country, population, lat, lng, timezone)"
                    " VALUES (%s,%s,%s,%s,%s,%s)",
                    SEED_CITIES
                )
                print(f"Seeded {len(SEED_CITIES)} cities")
            conn.commit()
            cur.close()
            conn.close()
            print(f"DB ready  host={DB_CONFIG['host']}  mode={DEV_MODE}")
            return
        except Exception as e:
            print(f"DB attempt {attempt + 1}/30: {e}")
            time.sleep(5)
    print("WARNING: DB not reachable after 30 attempts — starting anyway")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return jsonify({"status": "ok", "mode": DEV_MODE, "db": "connected"})
    except Exception as e:
        return jsonify({"status": "degraded", "mode": DEV_MODE,
                        "db": "error", "error": str(e)}), 500


@app.route("/")
def index():
    search = request.args.get("q", "").strip()
    sort   = request.args.get("sort", "population")
    order  = request.args.get("order", "desc")

    valid_cols = {"city", "country", "population", "lat", "lng", "timezone"}
    if sort not in valid_cols:
        sort = "population"
    order_sql = "DESC" if order == "desc" else "ASC"

    try:
        conn = get_conn()
        cur  = conn.cursor(cursor_factory=RealDictCursor)
        if search:
            cur.execute(
                f"SELECT * FROM cities"
                f" WHERE city ILIKE %s OR country ILIKE %s"
                f" ORDER BY {sort} {order_sql}",
                (f"%{search}%", f"%{search}%"),
            )
        else:
            cur.execute(f"SELECT * FROM cities ORDER BY {sort} {order_sql}")
        cities = cur.fetchall()
        cur.execute("SELECT COUNT(*) as total, SUM(population) as total_pop FROM cities")
        stats = cur.fetchone()
        cur.close()
        conn.close()
        error = None
    except Exception as e:
        cities = []
        stats  = {"total": 0, "total_pop": 0}
        error  = str(e)

    return render_template("index.html",
        cities=cities,
        stats=stats,
        search=search,
        sort=sort,
        order=order,
        error=error,
        mode=DEV_MODE,
    )


@app.route("/add", methods=["POST"])
def add_city():
    city       = request.form.get("city", "").strip()
    country    = request.form.get("country", "").strip()
    population = request.form.get("population", "0").strip()

    if not city or not country:
        return redirect(url_for("index"))

    try:
        pop = int(population) if population else 0
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO cities (city, country, population, lat, lng, timezone)"
            " VALUES (%s, %s, %s, 0, 0, 'UTC')",
            (city, country, pop),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

    return redirect(url_for("index"))


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting  mode={DEV_MODE}  port={port}")
    app.run(host="0.0.0.0", port=port, debug=True)
