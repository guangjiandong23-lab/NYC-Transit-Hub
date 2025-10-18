from datetime import datetime, timezone
from collections import defaultdict
from flask import Flask, jsonify, render_template
import csv
import os
import re
import requests
from google.transit import gtfs_realtime_pb2

app = Flask(__name__)

# --------------------------------------
# Public GTFS-RT group feeds (no API key)
# --------------------------------------
FEEDS = {
    "1234567": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "ACE":     "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "BDFM":    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "NQRW":    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "JZ":      "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "G":       "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "L":       "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "SIR":     "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
}

ALL_SUBWAY_ROUTES = sorted({
    *"1234567", *"ACE", *"BDFM", *"NQRW", *"JZ", "G", "L", "SIR"
})

# Optional: MTA Alerts feed (often 403 without key) — we will gracefully ignore failures
ALERTS_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys/all-alerts"

EFFECT_MAP = {
    1: "NO_SERVICE", 2: "REDUCED_SERVICE", 3: "SIGNIFICANT_DELAYS", 4: "DETOUR",
    5: "ADDED_SERVICE", 6: "MODIFIED_SERVICE", 7: "OTHER_EFFECT",
    8: "UNKNOWN_EFFECT", 9: "STOP_MOVED",
}

# -------------------
# Stops name resolver
# -------------------
# If you place a CSV file "stops.csv" next to app.py with columns: stop_id,stop_name,stop_lat,stop_lon
# we'll use it for names; otherwise we fall back to show stop_id only.
STOPS = {}
def load_stops_csv(path="stops.csv"):
    if not os.path.exists(path):
        return
    try:
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                sid = (row.get("stop_id") or "").strip()
                name = (row.get("stop_name") or "").strip()
                lat = float(row.get("stop_lat") or "0") if row.get("stop_lat") else None
                lon = float(row.get("stop_lon") or "0") if row.get("stop_lon") else None
                if sid:
                    # NYC stops often have A/B or N/S suffix; also index by base id w/o trailing letter
                    STOPS[sid] = {"name": name, "lat": lat, "lon": lon}
                    base = re.sub(r"[NSWE]$", "", sid)
                    STOPS.setdefault(base, {"name": name, "lat": lat, "lon": lon})
    except Exception as e:
        print("Failed to load stops.csv:", e)

load_stops_csv()  # best-effort

def stop_name(stop_id: str) -> str:
    if not stop_id: return ""
    base = re.sub(r"[NSWE]$", "", stop_id)
    info = STOPS.get(stop_id) or STOPS.get(base)
    return info["name"] if info and info.get("name") else stop_id

# ----------
# Utilities
# ----------
def _fetch_bytes(url: str) -> bytes:
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.content

def _parse_feed(blob: bytes) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(blob)
    return feed

# ---------------------------------------
# Parse Alerts (optional; ignore failures)
# ---------------------------------------
def parse_alerts(blob: bytes):
    feed = _parse_feed(blob)
    alerts = []
    for e in feed.entity:
        if not e.HasField("alert"):
            continue
        a = e.alert
        routes = set()
        modes = set()
        for ie in a.informed_entity:
            if ie.route_id:
                routes.add(ie.route_id)
            if ie.HasField("route_type"):
                modes.add(ie.route_type)
        if not (routes & set(ALL_SUBWAY_ROUTES)):
            if 1 not in modes:
                continue

        periods = []
        for ap in a.active_period:
            start = datetime.fromtimestamp(ap.start, tz=timezone.utc).isoformat() if ap.start else None
            end = datetime.fromtimestamp(ap.end, tz=timezone.utc).isoformat() if ap.end else None
            periods.append({"start": start, "end": end})

        header = next((t.text for t in a.header_text.translation if t.language.startswith("en")), None)
        desc   = next((t.text for t in a.description_text.translation if t.language.startswith("en")), None)
        effect = EFFECT_MAP.get(a.effect, "UNKNOWN_EFFECT")

        alerts.append({
            "id": e.id, "routes": sorted(routes), "effect": effect,
            "periods": periods, "header": header, "description": desc
        })
    return alerts

# ---------------------------------------------------
# Merge VehiclePositions + TripUpdates into live view
# ---------------------------------------------------
def summarize_trip_feed(feed: gtfs_realtime_pb2.FeedMessage):
    ts = feed.header.timestamp if feed.header and feed.header.timestamp else None
    return {
        "header_timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None,
        "entity_count": len(feed.entity),
        "trip_updates": sum(1 for e in feed.entity if e.HasField("trip_update")),
        "vehicles":     sum(1 for e in feed.entity if e.HasField("vehicle")),
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
    }

def extract_live_objects(feed: gtfs_realtime_pb2.FeedMessage):
    """
    Returns:
      vehicles: dict trip_id -> {route_id, lat, lon, bearing, current_status, ts, direction_id, occupancy}
      upcoming: dict trip_id -> first future stop_time_update ({stop_id, arrival_time, delay})
    """
    vehicles = {}
    upcoming = {}

    now_ts = int(datetime.now(timezone.utc).timestamp())

    for e in feed.entity:
        if e.HasField("vehicle"):
            v = e.vehicle
            trip_id = v.trip.trip_id if v.trip and v.trip.trip_id else None
            vehicles[trip_id] = {
                "route_id": (v.trip.route_id if v.trip and v.trip.route_id else None),
                "lat": (v.position.latitude if v.HasField("position") else None),
                "lon": (v.position.longitude if v.HasField("position") else None),
                "bearing": (v.position.bearing if v.HasField("position") else None),
                "current_status": v.current_status if v.HasField("current_status") else None,
                "ts": v.timestamp if v.HasField("timestamp") else None,
                "direction_id": (v.trip.direction_id if v.trip and v.trip.HasField("direction_id") else None),
                "occupancy": (v.occupancy_status if v.HasField("occupancy_status") else None),
                "trip_id": trip_id,
            }

    for e in feed.entity:
        if e.HasField("trip_update"):
            tu = e.trip_update
            trip_id = tu.trip.trip_id if tu.trip and tu.trip.trip_id else None
            next_st = None
            for stu in tu.stop_time_update:
                # pick first future stop (arrival or departure)
                arr = stu.arrival.time if stu.HasField("arrival") and stu.arrival.HasField("time") else None
                dep = stu.departure.time if stu.HasField("departure") and stu.departure.HasField("time") else None
                t = arr or dep
                if t and t >= now_ts:
                    next_st = {
                        "stop_id": stu.stop_id,
                        "arrival_time": t,
                        "delay": (stu.arrival.delay if stu.HasField("arrival") and stu.arrival.HasField("delay") else 0)
                    }
                    break
            if trip_id and next_st:
                upcoming[trip_id] = next_st

    return vehicles, upcoming

def build_live_snapshot():
    """Fetch all feeds, assemble vehicles + next-arrivals + summaries. Alerts are best-effort."""
    feeds_summary = {}
    vehicles_all = {}
    upcoming_all = {}

    for group, url in FEEDS.items():
        try:
            blob = _fetch_bytes(url)
            feed = _parse_feed(blob)
            feeds_summary[group] = summarize_trip_feed(feed)
            v, u = extract_live_objects(feed)
            vehicles_all.update(v)
            upcoming_all.update(u)
        except Exception as ex:
            feeds_summary[group] = {"error": str(ex)}

    # Attach next-stop info to vehicles
    live_list = []
    for trip_id, v in vehicles_all.items():
        next_st = upcoming_all.get(trip_id)
        if next_st:
            v = {**v, **{
                "next_stop_id": next_st["stop_id"],
                "next_stop_name": stop_name(next_st["stop_id"]),
                "next_arrival_time": datetime.fromtimestamp(next_st["arrival_time"], tz=timezone.utc).isoformat(),
                "delay_sec": next_st.get("delay", 0)
            }}
        live_list.append(v)

    # Optional alerts
    alerts = []
    alerts_unavailable = False
    alerts_error = None
    try:
        blob = _fetch_bytes(ALERTS_URL)
        alerts = parse_alerts(blob)
    except Exception as e:
        alerts_unavailable = True
        alerts_error = str(e)

    # Build ETA table: pick soonest arrivals across all trips (if present)
    eta_rows = []
    for v in live_list:
        if v.get("next_arrival_time") and v.get("route_id"):
            eta_rows.append({
                "route_id": v["route_id"],
                "trip_id": v.get("trip_id"),
                "stop_id": v.get("next_stop_id"),
                "stop_name": v.get("next_stop_name"),
                "arrival_time": v["next_arrival_time"],
                "delay_sec": v.get("delay_sec", 0),
                "direction_id": v.get("direction_id"),
            })
    # sort by soonest arrival
    eta_rows.sort(key=lambda r: r["arrival_time"])
    eta_rows = eta_rows[:200]  # cap for UI

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "feeds": feeds_summary,
        "alerts": alerts,
        "alerts_unavailable": alerts_unavailable,
        "alerts_error": alerts_error,
        "vehicles": live_list,
        "etas": eta_rows,
    }

# ---------------
# Flask endpoints
# ---------------
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/status")
def api_status():
    # keep previous health endpoint for compatibility
    snapshot = build_live_snapshot()
    # For status, just return same snapshot; front-end uses what it needs
    return jsonify({"ok": True, **snapshot})

@app.route("/api/live")
def api_live():
    snapshot = build_live_snapshot()
    return jsonify({"ok": True, **snapshot})

# Back-compat for old frontend
@app.route("/api/alerts")
def api_alerts_compat():
    return api_status()

if __name__ == "__main__":
    app.run(debug=True)
