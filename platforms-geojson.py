import csv
import datetime
import json
import os
import sys
import hashlib
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

GTFS_FOLDER = '../../../assets/bmtc-vonter/'  # Path to GTFS folder (needs stop_times.txt and stops.txt)

API_URL = 'https://bmtcmobileapi.karnataka.gov.in/WebAPI/'
VARNAM_API_URL = 'https://api.varnamproject.com/tl/kn/{word}'

REQUEST_HEADERS_EN = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Content-Type': 'application/json',
    'lan': 'en',
    'deviceType': 'WEB',
    'Origin': 'https://bmtcwebportal.amnex.com',
    'Referer': 'https://bmtcwebportal.amnex.com/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

REQUEST_HEADERS_KN = {
    **REQUEST_HEADERS_EN,
    'lan': 'kn',
}

CACHE_DB_PATH = 'api_cache_.db'
CACHE_DURATION_HOURS = 24
MAX_WORKERS = 10
VARNAM_CONCURRENCY = 4

# ──────────────────────────────────────────────
# SQLite API Cache
# ──────────────────────────────────────────────

def init_cache_db():
    conn = sqlite3.connect(CACHE_DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS api_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_hash TEXT UNIQUE NOT NULL,
            request_desc TEXT NOT NULL,
            response_data TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()


def get_cache_key(desc, request_data):
    request_string = f"{desc}:{request_data}"
    return hashlib.md5(request_string.encode()).hexdigest()


def get_cached_response(desc, request_data):
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        cache_key = get_cache_key(desc, request_data)
        cursor.execute(
            f"SELECT response_data FROM api_cache WHERE request_hash = ? AND created_at > datetime('now', '-{CACHE_DURATION_HOURS} hours')",
            (cache_key,)
        )
        result = cursor.fetchone()
        conn.close()
        if result:
            return json.loads(result[0])
        return None
    except Exception as e:
        print(f'  cache error: {e}')
        return None


def store_cached_response(desc, request_data, response_data):
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        cache_key = get_cache_key(desc, request_data)
        cursor.execute(
            'INSERT OR REPLACE INTO api_cache (request_hash, request_desc, response_data, created_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)',
            (cache_key, desc, json.dumps(response_data))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f'  cache store error: {e}')


def cleanup_expired_cache():
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM api_cache WHERE created_at <= datetime('now', '-{CACHE_DURATION_HOURS} hours')")
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        if deleted > 0:
            print(f'Cleaned up {deleted} expired cache entries')
    except Exception as e:
        print(f'Cache cleanup error: {e}')


# ──────────────────────────────────────────────
# GTFS: Discover neighboring stops
# ──────────────────────────────────────────────

def get_next_stops(stop_ids, nest_level=2):
    """Find stops reachable from stop_ids within nest_level hops using GTFS data."""
    print(f'Loading GTFS stop_times for neighbor discovery (nest_level={nest_level})...')

    with open(f"{GTFS_FOLDER}stop_times.txt", mode='r') as file:
        reader = csv.DictReader(file)
        stop_times = list(reader)

    next_stops_total = {stop_id: [] for stop_id in stop_ids}

    # Group by trip_id
    stop_times_by_trip = {}
    for st in stop_times:
        stop_times_by_trip.setdefault(st['trip_id'], []).append(st)

    for trip_id in stop_times_by_trip:
        stop_times_by_trip[trip_id].sort(key=lambda x: int(x['stop_sequence']))

    for stop_id in stop_ids:
        for st in stop_times:
            if st['stop_id'] != stop_id:
                continue

            trip_id = st['trip_id']
            trip_stop_times = stop_times_by_trip[trip_id]

            current_index = next(
                (i for i, x in enumerate(trip_stop_times) if x['stop_id'] == stop_id), None
            )
            if current_index is None:
                continue

            for offset in range(nest_level):
                idx = current_index + offset
                if idx >= len(trip_stop_times) - 1:
                    break

                curr = trip_stop_times[idx]['stop_id']
                nxt = trip_stop_times[idx + 1]['stop_id']

                if curr not in next_stops_total:
                    next_stops_total[curr] = []
                if nxt not in next_stops_total[curr]:
                    next_stops_total[curr].append(nxt)

    print(f'Found neighbors for {len(next_stops_total)} stops')
    return next_stops_total


# ──────────────────────────────────────────────
# BMTC API: Fetch routes and platform data
# ──────────────────────────────────────────────

def fetch_all_routes():
    """Fetch all routes from BMTC API in both English and Kannada."""
    print('Fetching all routes from API...')

    # English
    cached = get_cached_response('GetAllRouteList_en', '{}')
    if cached:
        routes_en_data = cached
    else:
        resp = requests.post(f'{API_URL}GetAllRouteList', headers=REQUEST_HEADERS_EN, data='{}')
        try:
            routes_en_data = resp.json()
        except Exception as e:
            print(f'Error decoding route list JSON: {e}')
            routes_en_data = {}
        store_cached_response('GetAllRouteList_en', '{}', routes_en_data)

    routes_en = {route['routeid']: route for route in routes_en_data.get('data', [])}
    print(f'  Loaded {len(routes_en)} routes (English)')

    # Kannada
    cached = get_cached_response('GetAllRouteList_kn', '{}')
    if cached:
        routes_kn_data = cached
    else:
        resp = requests.post(f'{API_URL}GetAllRouteList', headers=REQUEST_HEADERS_KN, data='{}')
        try:
            routes_kn_data = resp.json()
        except Exception as e:
            print(f'Error decoding Kannada route list JSON: {e}')
            routes_kn_data = {}
        store_cached_response('GetAllRouteList_kn', '{}', routes_kn_data)

    routes_kn = {route['routeid']: route for route in routes_kn_data.get('data', [])}
    print(f'  Loaded {len(routes_kn)} routes (Kannada)')

    return routes_en, routes_kn


def fetch_platform_assignments(stop_ids, next_stops, overrides, nest_level=2):
    """Query BMTC API for platform assignments using neighboring stop pairs."""
    print('Fetching platform assignments from API...')

    tomorrow_start = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00')
    tomorrow_end = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d 23:59')

    schedule_times = {"Failed": [], "Received": []}
    routes_done = set()
    routes_done_lock = threading.Lock()
    received_lock = threading.Lock()
    failed_lock = threading.Lock()

    s = {level: set() for level in range(nest_level + 1)}

    def send_request(from_stop, to_stop):
        data = json.dumps({
            "fromStationId": int(from_stop),
            "toStationId": int(to_stop),
            "p_startdate": tomorrow_start,
            "p_enddate": tomorrow_end,
            "p_isshortesttime": 0,
            "p_routeid": "",
            "p_date": tomorrow_start
        })

        # Check cache
        cache_desc = f'timetable_{from_stop}_{to_stop}'
        cached = get_cached_response(cache_desc, data)
        if cached is not None:
            is_failed = (
                cached.get("exception") not in (None, False) or
                cached.get("isException") is True or
                cached.get("Issuccess") is not True
            )
            return from_stop, to_stop, cached, is_failed

        try:
            response = requests.post(
                f'{API_URL}GetTimetableByStation_v4',
                headers=REQUEST_HEADERS_EN, data=data
            ).json()
        except Exception:
            response = {
                "isException": True, "Issuccess": False,
                "exception": "Response not received in JSON.",
                "Message": "Response not received in JSON."
            }

        store_cached_response(cache_desc, data, response)

        is_failed = (
            response.get("exception") not in (None, False) or
            response.get("isException") is True or
            response.get("Issuccess") is not True
        )
        return from_stop, to_stop, response, is_failed

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for stop in stop_ids:
            print(f'  Processing stop {stop}')
            s[0].add(stop)

            for level in range(nest_level):
                has_failures = False
                futures = []
                print(f'    Level {level}: querying {len(s[level])} stops')

                for b in s[level]:
                    if b not in next_stops:
                        continue
                    for n in next_stops[b]:
                        futures.append(executor.submit(send_request, stop, n))

                for future in as_completed(futures):
                    from_stop, to_stop, response, is_failed = future.result()

                    if is_failed:
                        with failed_lock:
                            schedule_times["Failed"].append({
                                "from_stop": from_stop,
                                "to_stop": to_stop,
                                "response": response,
                                "level": level
                            })
                        s[level + 1].add(to_stop)
                        has_failures = True
                    else:
                        for route_entry in response.get("data", []):
                            route_id = route_entry["routeid"]
                            pf_name = overrides.get(str(route_id), route_entry.get("platformname", ""))
                            pf_num = overrides.get(str(route_id), route_entry.get("platformnumber", ""))

                            with routes_done_lock:
                                if route_id in routes_done:
                                    continue
                                if (pf_name and pf_name != "") or (pf_num and pf_num != ""):
                                    routes_done.add(route_id)

                            with received_lock:
                                new_entry = {
                                    "route-number": route_entry.get('routeno', ''),
                                    "route-name": route_entry.get("routename", ""),
                                    "from-station-id": route_entry.get('fromstationid', ''),
                                    "route-id": route_id,
                                    "route-parent-id": '',  # populated later by fetch_all_route_stops
                                    "platform-name": pf_name,
                                    "platform-number": pf_num,
                                    "bay-number": route_entry.get("baynumber"),
                                }

                                # Update if exists, otherwise add
                                existing_index = None
                                for i, existing in enumerate(schedule_times["Received"]):
                                    if existing.get("route-id") == route_id:
                                        existing_index = i
                                        break
                                if existing_index is not None:
                                    schedule_times["Received"][existing_index] = new_entry
                                else:
                                    schedule_times["Received"].append(new_entry)

                if not has_failures:
                    print(f'    All requests successful at level {level}, stopping')
                    break

    print(f'  Received {len(schedule_times["Received"])} routes, {len(schedule_times["Failed"])} failures')
    return schedule_times


# ──────────────────────────────────────────────
# BMTC API: Fetch individual missing routes by ID
# ──────────────────────────────────────────────

def fetch_missing_routes_by_id(missing_route_ids, routes_en, overrides):
    """Query GetTimetableByRouteid_v3 for routes missing from bulk platform queries.

    A route is considered missing if its fromstationid matches one of our stop IDs
    but it was not returned by the GetTimetableByStation_v4 bulk queries.
    """
    print(f'Fetching {len(missing_route_ids)} missing routes individually by route ID...')

    today = datetime.datetime.now().strftime('%Y-%m-%d')
    start_time = f'{today} 00:01'
    end_time = f'{today} 23:59'

    received = []

    for route_id in missing_route_ids:
        data = json.dumps({
            "routeid": route_id,
            "starttime": start_time,
            "endtime": end_time,
            "current_date": today
        })

        cache_desc = f'GetTimetableByRouteid_v3_{route_id}'
        cached = get_cached_response(cache_desc, data)
        if cached is not None:
            response = cached
        else:
            try:
                response = requests.post(
                    f'{API_URL}GetTimetableByRouteid_v3',
                    headers=REQUEST_HEADERS_EN,
                    data=data,
                    timeout=30
                ).json()
                store_cached_response(cache_desc, data, response)
            except Exception as e:
                print(f'  Error fetching route {route_id}: {e}')
                continue

        if response.get('Issuccess') is not True or not response.get('data'):
            print(f'  Route {route_id}: no data returned')
            continue

        route_entry = response['data'][0]
        route_en = routes_en.get(route_id, {})

        pf_name = overrides.get(str(route_id), route_entry.get('platformname', ''))
        pf_num = overrides.get(str(route_id), route_entry.get('platformnumber', ''))

        new_entry = {
            'route-number': route_en.get('routeno', route_entry.get('routeno', '')),
            'route-name': route_en.get('routename', route_entry.get('routename', '')),
            'from-station-id': route_en.get('fromstationid', ''),
            'route-id': route_id,
            'route-parent-id': '',
            'platform-name': pf_name,
            'platform-number': pf_num,
            'bay-number': route_entry.get('baynumber'),
        }
        received.append(new_entry)
        print(f'  Fetched route {new_entry["route-number"]} (ID: {route_id}), platform: {pf_name or pf_num or "(none)"}')

    print(f'  Retrieved {len(received)}/{len(missing_route_ids)} missing routes')
    return received


def fetch_trip_counts(route_ids):
    """Fetch trip counts for each route using GetTimetableByRouteid_v3."""
    print(f'Fetching trip counts for {len(route_ids)} routes...')

    tomorrow = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    start_time = f'{tomorrow} 00:01'
    end_time = f'{tomorrow} 23:59'

    trip_counts = {}
    lock = threading.Lock()

    def fetch_one(route_id):
        data = json.dumps({
            "routeid": route_id,
            "starttime": start_time,
            "endtime": end_time,
            "current_date": tomorrow
        })
        cache_desc = f'GetTimetableByRouteid_v3_{route_id}'
        cached = get_cached_response(cache_desc, data)
        if cached is not None:
            response = cached
        else:
            try:
                response = requests.post(
                    f'{API_URL}GetTimetableByRouteid_v3',
                    headers=REQUEST_HEADERS_EN,
                    data=data,
                    timeout=30
                ).json()
                store_cached_response(cache_desc, data, response)
            except Exception as e:
                print(f'  Error fetching trip count for route {route_id}: {e}')
                return route_id, 0

        if response.get('Issuccess') is True and response.get('data'):
            trips = response['data'][0].get('tripdetails', [])
            return route_id, len(trips)
        return route_id, 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for route_id, count in executor.map(fetch_one, route_ids):
            with lock:
                trip_counts[route_id] = count

    zero_count = sum(1 for c in trip_counts.values() if c == 0)
    print(f'  Trip counts fetched: {len(trip_counts)} routes, {zero_count} with no trips')
    return trip_counts


# ──────────────────────────────────────────────
# Kannada translations via Varnam API
# ──────────────────────────────────────────────

def transliterate_varnam(word):
    """Transliterate a single word to Kannada using Varnam API."""
    cache_desc = f'varnam_{word}'
    cached = get_cached_response(cache_desc, word)
    if cached is not None:
        return cached.get('result', word)

    try:
        url = VARNAM_API_URL.format(word=word.lower())
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            result = data.get('result')
            if isinstance(result, list) and result:
                kn = result[0]
            elif isinstance(result, str):
                kn = result
            else:
                kn = word
            store_cached_response(cache_desc, word, {'result': kn})
            return kn
    except Exception:
        pass
    return word


def is_kannada(text):
    """Check if text contains Kannada characters."""
    if not text:
        return False
    for ch in text:
        if '\u0C80' <= ch <= '\u0CFF':
            return True
    return False


def get_kannada_route_info(routes_kn, route_id):
    """Get Kannada route info from API data, transliterate if needed."""
    route_kn = routes_kn.get(route_id, {})
    from_station = route_kn.get('fromstation', '')
    to_station = route_kn.get('tostation', '')

    # If API returned English even with kn header, transliterate
    if from_station and not is_kannada(from_station):
        from_station = transliterate_varnam(from_station)
    if to_station and not is_kannada(to_station):
        to_station = transliterate_varnam(to_station)

    return from_station, to_station


def get_stop_name_kn(stop_name, kn_cache):
    """Get Kannada stop name, using cache or Varnam API."""
    if stop_name in kn_cache:
        return kn_cache[stop_name]

    kn = transliterate_varnam(stop_name)
    kn_cache[stop_name] = kn
    return kn


# ──────────────────────────────────────────────
# BMTC API: Fetch stop sequences and route parent IDs
# ──────────────────────────────────────────────

def fetch_route_parent_ids(route_numbers):
    """Use SearchRoute_v2 to find routeparentid for each route number.
    Optimises by grouping routes by first character and making one API call per group."""
    print(f'Fetching route parent IDs for {len(route_numbers)} unique route numbers...')
    parent_ids = {}

    # Group route numbers by first character to minimize API calls
    # e.g. 500-A, 500-B, 501-AC all share prefix '5' -> one API call
    groups = {}
    for routeno in route_numbers:
        search_text = routeno.split(' ')[0] if ' ' in routeno else routeno
        prefix = search_text[0] if search_text else ''
        if prefix:
            groups.setdefault(prefix, []).append((routeno, search_text))

    print(f'  Grouped into {len(groups)} prefix queries: {sorted(groups.keys())}')

    for prefix, route_list in groups.items():
        cache_desc = f'SearchRoute_v2_{prefix}'
        cached = get_cached_response(cache_desc, prefix)

        if cached is not None:
            data = cached.get('data', [])
        else:
            try:
                resp = requests.post(
                    f'{API_URL}SearchRoute_v2',
                    headers=REQUEST_HEADERS_EN,
                    data=json.dumps({"routetext": prefix}),
                    timeout=30
                )
                result = resp.json()
                store_cached_response(cache_desc, prefix, result)
                data = result.get('data', [])
            except Exception as e:
                print(f'  SearchRoute_v2 error for prefix "{prefix}": {e}')
                data = []

        # Build lookup from API response
        api_lookup = {}
        for entry in data:
            rn = entry.get('routeno', '').upper()
            if rn not in api_lookup:
                api_lookup[rn] = entry['routeparentid']

        # Match each route in this group
        for routeno, search_text in route_list:
            # Exact match first
            if routeno.upper() in api_lookup:
                parent_ids[routeno] = api_lookup[routeno.upper()]
            else:
                # Prefix match fallback
                for entry in data:
                    if entry.get('routeno', '').upper().startswith(search_text.upper()):
                        parent_ids[routeno] = entry['routeparentid']
                        break

    print(f'  Found parent IDs for {len(parent_ids)}/{len(route_numbers)} routes')
    return parent_ids

def fetch_route_stops(route_parent_id, stop_ids, from_station_id=None, route_id=None):
    """Use SearchByRouteDetails_v4 to get stop sequence for a route.
    Returns list of {stop_id, stop_name} for the correct direction.

    Direction selection priority:
    0. Match route_id directly against the routeid in each direction's metadata.
    1. If from_station_id is given, pick the direction where from_station_id appears
       before one of our stop_ids (bus came from there, now departing onward).
    2. Pick the direction that starts at one of our stop_ids.
    3. Fallback to UP direction.
    """
    cache_desc = f'SearchByRouteDetails_v4_{route_parent_id}'
    cached = get_cached_response(cache_desc, str(route_parent_id))

    if cached is not None:
        result = cached
    else:
        try:
            resp = requests.post(
                f'{API_URL}SearchByRouteDetails_v4',
                headers=REQUEST_HEADERS_EN,
                data=json.dumps({"routeid": route_parent_id, "servicetypeid": 0}),
                timeout=60
            )
            result = resp.json()
            store_cached_response(cache_desc, str(route_parent_id), result)
        except Exception as e:
            print(f'  SearchByRouteDetails_v4 error for parent {route_parent_id}: {e}')
            return []

    stop_ids_set = set(str(sid) for sid in stop_ids)
    from_id = str(from_station_id) if from_station_id else None

    def direction_stops(direction):
        data = result.get(direction, {}).get('data', [])
        if not data:
            return None
        return [
            {'stop_id': str(stop.get('stationid', '')), 'stop_name': stop.get('stationname', ''), 'stop_lat': stop.get('centerlat', 0), 'stop_lon': stop.get('centerlong', 0)}
            for stop in data
        ]

    # Strategy 0: match route_id against the routeid embedded in each direction's stop data
    if route_id:
        for direction in ['up', 'down']:
            data = result.get(direction, {}).get('data', [])
            if data and str(data[0].get('routeid', '')) == str(route_id):
                return direction_stops(direction)

    # Strategy 1: use from_station_id to pick the direction where the bus came from
    # (from_station_id appears before our stop_id in the sequence)
    if from_id:
        for direction in ['up', 'down']:
            data = result.get(direction, {}).get('data', [])
            if not data:
                continue
            station_ids = [str(stop.get('stationid', '')) for stop in data]
            try:
                from_idx = station_ids.index(from_id)
            except ValueError:
                continue
            # Check that one of our stop_ids appears after from_idx
            for j in range(from_idx + 1, len(station_ids)):
                if station_ids[j] in stop_ids_set:
                    return direction_stops(direction)

    # Strategy 2: pick the direction that starts at one of our stop_ids
    for direction in ['up', 'down']:
        data = result.get(direction, {}).get('data', [])
        if not data:
            continue
        if str(data[0].get('stationid', '')) in stop_ids_set:
            return direction_stops(direction)

    # Fallback: return UP direction if available
    return direction_stops('up') or []


def fetch_all_route_stops(schedule_times, stop_ids):
    """Fetch stop sequences for all received routes via the API."""
    print('Fetching stop sequences from API...')

    # Collect unique route numbers
    route_numbers = set()
    for route_data in schedule_times["Received"]:
        rn = route_data.get('route-number', '')
        if rn:
            route_numbers.add(rn)

    # Get parent IDs
    parent_ids = fetch_route_parent_ids(route_numbers)

    # Update schedule_times with parent IDs
    for route_data in schedule_times["Received"]:
        rn = route_data.get('route-number', '')
        if rn in parent_ids:
            route_data['route-parent-id'] = parent_ids[rn]

    # Fetch stop sequences for each route, using from_station_id to pick the correct direction.
    # Routes sharing the same parent_id (opposite directions) each get their own sequence.
    route_stops = {}  # route_id -> [{'stop_id', 'stop_name'}]

    for route_data in schedule_times["Received"]:
        route_id = route_data["route-id"]
        # if route_id not in [1666, 1670]:
        #     print('Skipping route for stop sequence', route_id)
        #     continue
        parent_id = route_data.get('route-parent-id', '')
        from_station_id = route_data.get('from-station-id', '')
        if not parent_id:
            continue

        stops = fetch_route_stops(parent_id, stop_ids, from_station_id, route_id)
        if stops:
            route_stops[route_id] = stops
            # print(f'stops for route {route_id}', stops.__str__())

    print(f'  Fetched stop sequences for {len(route_stops)} routes')
    return route_stops

# ──────────────────────────────────────────────
# Build output GeoJSON
# ──────────────────────────────────────────────

def smart_match_platform(route_number, platforms_routes, platform_route_ids):
    """Intelligently match a route to a platform based on route number patterns.

    Tries:
    1. Exact prefix match (e.g., "13-C" for "13-C SGH-ISROL")
    2. Base number match with most occurrences (e.g., platform with most "13-" routes for "13-S")

    Returns matched platform name (uppercase) or None.
    """
    if not route_number:
        return None

    # Check for exact prefix matches first (e.g., "13-C" exists for "13-C SGH-ISROL")
    for platform_name in platforms_routes:
        if platform_name in ["UNKNOWN", "UNSORTED"]:
            continue

        routes_on_platform = platforms_routes[platform_name]
        for rd in routes_on_platform:
            existing_route_num = rd.get('route-number', '')
            # Check if route_number starts with an existing route number
            if existing_route_num and route_number.startswith(existing_route_num + ' '):
                return platform_name

    # Extract base route number (e.g., "13" from "13-S", "500" from "500-A")
    # Handle formats like "13-S", "500-A", "G-4", etc.
    parts = route_number.split('-')
    if len(parts) < 2:
        return None

    base_number = parts[0]  # e.g., "13", "500", "G"

    # Count occurrences of routes with this base number on each platform
    platform_counts = {}
    for platform_name in platforms_routes:
        if platform_name in ["UNKNOWN", "UNSORTED"]:
            continue

        count = 0
        routes_on_platform = platforms_routes[platform_name]
        for rd in routes_on_platform:
            existing_route_num = rd.get('route-number', '')
            if existing_route_num and existing_route_num.startswith(base_number + '-'):
                count += 1

        if count > 0:
            platform_counts[platform_name] = count

    # Return platform with most matching routes
    if platform_counts:
        best_platform = max(platform_counts.items(), key=lambda x: x[1])
        return best_platform[0]

    return None


def replacements(s: str) -> str:
    return (s
            .replace('Banashankari Hunasemara', 'Hunasemara')
            .replace('Kempegowda', 'Majestic')
            .replace('KR Market', 'Kalasipalya')
            .replace('Silk Board', 'silkboard')
            .replace('Shanthinagar', 'shantinagar')
            )

def build_geojson(
    schedule_times, routes_en, routes_kn, stop_platforms,
    platforms_geojson, overrides, stop_ids, kn_cache, route_stops_api, file_nickname,
    trip_counts=None
):
    """Build the output GeoJSON matching the current app format."""
    print('Building output GeoJSON...')

    # Build platform geometry/color lookup from input geojson
    platform_geom_lookup = {}
    platform_color_lookup = {}
    platform_icon_lookup = {}
    platform_hours_lookup = {}
    platform_alias_lookup = {}  # alias -> canonical platform name
    for feature in platforms_geojson['features']:
        plat_name = str(feature['properties'].get('Platform', '')).strip().upper()
        icon = str(str(feature['properties'].get('Icon', plat_name)).strip().upper())
        hours =  {'open': feature['properties'].get('OpenHour', 0), 'close': feature['properties'].get('CloseHour', 0)}
        if feature['geometry']['type'] == 'Point':
            platform_geom_lookup[plat_name] = feature['geometry']
            platform_color_lookup[plat_name] = feature['properties'].get('Color', '#000000')
            platform_icon_lookup[plat_name] = icon
            platform_hours_lookup[plat_name] = hours
            for alias in feature['properties'].get('Alias', []):
                platform_alias_lookup[str(alias).strip().upper()] = plat_name

    # Group routes by platform
    platforms_routes = {name.upper(): [] for name in platform_geom_lookup}
    platforms_routes["UNKNOWN"] = []
    platforms_routes["UNSORTED"] = []

    # Track which route numbers are already assigned to each platform to avoid duplicates
    platform_route_ids = {name: set() for name in platforms_routes}

    for route_data in schedule_times["Received"]:
        route_id = route_data["route-id"]
        from_station_id = str(route_data.get("from-station-id", ""))
        route_number = route_data.get('route-number', '')

        # Determine platform: stop-platforms mapping > overrides > API platform name/number
        is_manual_override = False
        if from_station_id in stop_platforms:
            platform = stop_platforms[from_station_id].upper()
            is_manual_override = True
        elif str(route_id) in overrides:
            platform = str(overrides[str(route_id)]).upper()
        else:
            pf_name = route_data.get("platform-name", "")
            pf_num = route_data.get("platform-number", "")
            platform = (pf_name if pf_name else pf_num if pf_num else "").upper()

        # Resolve platform alias (e.g. 20C -> 20B)
        if platform and platform not in platforms_routes:
            platform = platform_alias_lookup.get(platform, platform)

        # Smart matching: only apply if not from stop-platforms.json and no platform found yet
        if not platform and not is_manual_override:
            smart_platform = smart_match_platform(route_number, platforms_routes, platform_route_ids)
            if smart_platform:
                platform = smart_platform
                print(f'  Smart matched route {route_number} to platform {platform}')

        if not platform:
            platforms_routes["UNKNOWN"].append(route_data)
            continue
        if platform in platforms_routes:
            # Deduplicate: skip if this route is already assigned to this platform
            if route_id in platform_route_ids[platform]:
                continue
            platform_route_ids[platform].add(route_id)
            platforms_routes[platform].append(route_data)
        else:
            platforms_routes["UNSORTED"].append(route_data)

    # Exclusive platforms: routes must come from direct API assignment only (not cross-check)
    exclusive_platforms = set()
    for feature in platforms_geojson['features']:
        if feature['properties'].get('Exclusive'):
            plat_name = str(feature['properties'].get('Platform', '')).strip().upper()
            exclusive_platforms.add(plat_name)

    # Additional step: Cross-check stop sequences against stop_platforms
    # If a route passes through a stop in stop_platforms, add it to that platform too.
    # Exclusive platforms are skipped — a route merely passing through their stop
    # must not be pulled in, as it would then be incorrectly treated as exclusive.
    print('Cross-checking stop sequences against stop-platforms...')
    additional_assignments = 0
    for route_data in schedule_times["Received"]:
        route_id = route_data["route-id"]
        route_number = route_data.get('route-number', '')

        # Get stop sequence for this route
        route_stops = route_stops_api.get(route_id, [])

        # Check each stop in the sequence, stop at the first match.
        # The first informal stop hit is the station exit — later matches are en-route
        # stops that shouldn't determine the departure platform.
        for stop_info in route_stops:
            stop_id = str(stop_info.get('stop_id', ''))
            if stop_id in stop_platforms:
                platform = stop_platforms[stop_id].upper()

                if platform in exclusive_platforms:
                    break  # Don't cross-check into exclusive platforms

                # Add to this platform if not already there
                if platform in platforms_routes:
                    if route_id not in platform_route_ids[platform]:
                        platform_route_ids[platform].add(route_id)
                        platforms_routes[platform].append(route_data)
                        additional_assignments += 1
                        print(f'  Added route {route_number} to platform {platform} (passes through stop {stop_id})')
                break  # Only use the first informal stop match

    print(f'Added {additional_assignments} additional route assignments based on stop sequences')

    # Enforce exclusive platforms: collect route IDs from direct API assignments only
    # (cross-check was blocked above), then remove them from all other platforms.
    exclusive_route_ids = set()
    for plat_name in exclusive_platforms:
        for route_data in platforms_routes.get(plat_name, []):
            exclusive_route_ids.add(route_data['route-id'])

    if exclusive_route_ids:
        removed_count = 0
        for plat_name in list(platforms_routes.keys()):
            if plat_name in exclusive_platforms:
                continue
            before = len(platforms_routes[plat_name])
            platforms_routes[plat_name] = [
                r for r in platforms_routes[plat_name]
                if r['route-id'] not in exclusive_route_ids
            ]
            removed_count += before - len(platforms_routes[plat_name])
            platform_route_ids[plat_name] -= exclusive_route_ids

        if removed_count:
            print(f'Removed {removed_count} route assignments enforced by exclusive platforms: {exclusive_platforms}')

    # Filter routes below each platform's MinTrips threshold (default 1)
    if trip_counts is not None:
        platform_min_trips = {}
        for feature in platforms_geojson['features']:
            plat_name = str(feature['properties'].get('Platform', '')).strip().upper()
            platform_min_trips[plat_name] = feature['properties'].get('MinTrips', 1)

        removed_count = 0
        for plat_name in list(platforms_routes.keys()):
            if plat_name in ('UNKNOWN', 'UNSORTED'):
                continue
            min_trips = platform_min_trips.get(plat_name, 1)
            before = len(platforms_routes[plat_name])
            platforms_routes[plat_name] = [
                r for r in platforms_routes[plat_name]
                if trip_counts.get(r['route-id'], 0) >= min_trips
            ]
            removed_count += before - len(platforms_routes[plat_name])
            platform_route_ids[plat_name] = {r['route-id'] for r in platforms_routes[plat_name]}

        if removed_count:
            print(f'Removed {removed_count} routes below MinTrips threshold')

    # Collect API stop IDs per platform:
    # - from-station-id of routes assigned here (if it's one of our stop_ids)
    # - any of our stop_ids appearing in the route's stop sequence
    stop_ids_set_str = set(str(s) for s in stop_ids)
    platform_api_ids = {name: set() for name in platform_geom_lookup}
    for plat_name in platform_geom_lookup:
        for route_data in platforms_routes.get(plat_name, []):
            route_id = route_data['route-id']
            from_sid = str(route_data.get('from-station-id', ''))
            if from_sid in stop_ids_set_str:
                platform_api_ids[plat_name].add(from_sid)
            for stop_info in route_stops_api.get(route_id, []):
                sid = str(stop_info.get('stop_id', ''))
                if sid in stop_ids_set_str:
                    platform_api_ids[plat_name].add(sid)

    # Build features
    features = []
    for plat_name, geometry in platform_geom_lookup.items():
        color = platform_color_lookup.get(plat_name, '#008F45')
        icon = platform_icon_lookup.get(plat_name, plat_name)
        hours = platform_hours_lookup.get(plat_name, {})
        route_list = platforms_routes.get(plat_name, [])

        routes = []
        seen_route_numbers = set()
        for route_data in route_list:
            route_id = route_data["route-id"]
            route_en = routes_en.get(route_id, {})

            # English info
            from_station = route_en.get('fromstation', '')
            route_number = route_data.get('route-number', route_en.get('routeno', ''))

            # De-duplicate by route number within the same platform
            if route_number in seen_route_numbers:
                continue
            seen_route_numbers.add(route_number)

            # Use fromstation as Area (general area), tostation as Destination
            # Via can be derived from route name or left empty
            route_name = route_data.get('route-name', route_en.get('routeno', ''))
            area = from_station
            via = ''  # API doesn't provide a direct "via" field

            # Kannada info
            kn_from, kn_to = get_kannada_route_info(routes_kn, route_id)

            # Get stop sequence from API
            stops = []
            api_route_stops = route_stops_api.get(route_id, [])

            # Find the first occurrence of any of our stop IDs
            first_stop_index = None
            for i, stop_info in enumerate(api_route_stops):
                if str(stop_info.get('stop_id', '')) in [str(sid) for sid in stop_ids] and str(file_nickname).lower() in replacements(str(stop_info.get('stop_name', ''))).lower():
                    first_stop_index = i
                    break

            # Skip routes that don't pass through any main station stop
            # (e.g. routes only assigned via informal stop cross-check)
            if first_stop_index is None:
                continue

            # Splice to only include stops from our station onwards
            api_route_stops = api_route_stops[first_stop_index:]

            for stop_info in api_route_stops:
                stop_name = stop_info['stop_name']
                stop_kn = get_stop_name_kn(stop_name, kn_cache)
                stops.append({
                    'name': stop_name,
                    'name_kn': stop_kn,
                    'stop_id': stop_info['stop_id'],
                    'lat': stop_info['stop_lat'],
                    'lon': stop_info['stop_lon']
                })

            # Use last stop name as destination
            destination = stops[-1]['name'] if stops else route_en.get('tostation', '')
            kn_destination = stops[-1]['name_kn'] if stops else kn_to

            route_obj = {
                'Route': route_number,
                'RouteId': route_id,
                'RouteParentId': route_data.get('route-parent-id', ''),
                'Destination': destination,
                'Via': via,
                'Area': area,
                'PlatformNumber': plat_name,
                'KannadaDestination': kn_destination,
                'KannadaArea': kn_from,
                'KannadaVia': '',
                'FromStationId': route_data.get('from-station-id', ''),
                'Stops': stops
            }
            routes.append(route_obj)

        if routes:
            feature = {
                'type': 'Feature',
                'geometry': geometry,
                'properties': {
                    'Platform': plat_name,
                    'Color': color,
                    'Icon': icon,
                    'OpenHour': hours.get('open', 0),
                    'CloseHour': hours.get('close', 0),
                    'ApiIds': sorted(platform_api_ids.get(plat_name, set())),
                    'Routes': routes
                }
            }
            features.append(feature)

    geojson = {
        'type': 'FeatureCollection',
        'features': features
    }

    # Report unknown/unsorted
    unknown_count = len(platforms_routes.get("UNKNOWN", []))
    unsorted_count = len(platforms_routes.get("UNSORTED", []))
    if unknown_count > 0 or unsorted_count > 0:
        print(f'  WARNING: {unknown_count} unknown, {unsorted_count} unsorted routes')

    return geojson, platforms_routes


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    # Parse command-line arguments
    # Usage: python generate-geojson.py <stop_id1> [stop_id2 ...] <nickname> [nest_level]
    if len(sys.argv) < 3:
        print('Usage: python generate-geojson.py <stop_id1> [stop_id2 ...] <nickname> [nest_level]')
        print('Example: python generate-geojson.py 20621 20623 banashankari 2')
        print('Using default options')
        nest_level = 2
        file_nickname = 'banashankari'
        stop_ids = ['21149', '20621', '22459', '21711', '22062', '20897', '20623', '39241']
    else:
        if sys.argv[-1].isdigit() and not sys.argv[-2].isdigit():
            # Last arg is nest_level, second-to-last is nickname
            nest_level = int(sys.argv[-1])
            file_nickname = sys.argv[-2]
            stop_ids = sys.argv[1:-2]
        elif sys.argv[-1].isdigit():
            # All digits — ambiguous, treat last as nest_level
            nest_level = int(sys.argv[-1])
            file_nickname = sys.argv[-2]
            stop_ids = sys.argv[1:-2]
        else:
            nest_level = 2
            file_nickname = sys.argv[-1]
            stop_ids = sys.argv[1:-1]

        print(f'Stop IDs: {stop_ids}')
        print(f'Nickname: {file_nickname}')
        print(f'Nest level: {nest_level}')

    # File paths
    platforms_geojson_path = f'in/platforms-{file_nickname}.geojson'
    stop_platforms_path = f'in/stops-platforms-{file_nickname}.json'
    overrides_path = 'overrides.json'
    output_geojson_path = f'out/platforms-routes-{file_nickname}.geojson'
    raw_output_path = f'raw/platforms-{file_nickname}.json'

    # Initialize cache
    init_cache_db()
    cleanup_expired_cache()

    # Load config files
    with open(platforms_geojson_path, encoding='utf-8') as f:
        platforms_geojson = json.load(f)
    if os.path.exists(stop_platforms_path):
        with open(stop_platforms_path, encoding='utf-8') as f:
            stop_platforms_raw = json.load(f)
    else:
        stop_platforms_raw = {}

    # Expand comma-separated stop IDs: {"21149,20621": "East"} -> {"21149": "East", "20621": "East"}
    stop_platforms = {}
    for key, platform_name in stop_platforms_raw.items():
        for sid in key.split(','):
            sid = sid.strip()
            if sid:
                stop_platforms[sid] = platform_name

    overrides = {}
    if os.path.exists(overrides_path):
        with open(overrides_path, encoding='utf-8') as f:
            overrides_json = json.load(f)
            # Overrides may be nested by stop_id (like reference repo)
            # Flatten: if value is a dict, merge it in
            for key, val in overrides_json.items():
                if isinstance(val, dict):
                    overrides.update(val)
                else:
                    overrides[key] = val

    # Step 1: Discover neighboring stops from GTFS
    next_stops = get_next_stops(stop_ids, nest_level=nest_level)

    # Step 2: Fetch all routes
    routes_en, routes_kn = fetch_all_routes()

    # Step 3: Fetch platform assignments
    schedule_times = fetch_platform_assignments(stop_ids, next_stops, overrides, nest_level)

    # Step 3b: Find routes with matching fromstationid missing from bulk queries and fetch them
    received_route_ids = {r['route-id'] for r in schedule_times['Received']}
    stop_ids_set = set(str(s) for s in stop_ids)
    missing_route_ids = [
        route_id for route_id, route in routes_en.items()
        if str(route.get('fromstationid', '')) in stop_ids_set
        and route_id not in received_route_ids
    ]
    if missing_route_ids:
        print(f'Found {len(missing_route_ids)} routes with matching fromstationid absent from bulk queries')
        missing_received = fetch_missing_routes_by_id(missing_route_ids, routes_en, overrides)
        schedule_times['Received'].extend(missing_received)

    # Save raw data
    os.makedirs('raw', exist_ok=True)
    with open(raw_output_path, 'w', encoding='utf-8') as f:
        json.dump(schedule_times, f, indent=2, ensure_ascii=False)
    print(f'Saved raw data to {raw_output_path}')

    # Step 4: Fetch stop sequences from API (SearchRoute_v2 + SearchByRouteDetails_v4)
    route_stops_api = fetch_all_route_stops(schedule_times, stop_ids)

    # Step 4b: Fetch trip counts for all received routes
    all_route_ids = [r['route-id'] for r in schedule_times['Received']]
    trip_counts = fetch_trip_counts(all_route_ids)

    # Step 5: Build Kannada cache
    kn_cache = {}
    # Load existing Kannada translations if available
    # kn_csv_path = 'input/bus-stops-kn.csv'
    # if os.path.exists(kn_csv_path):
    #     try:
    #         with open(kn_csv_path, encoding='utf-8') as f:
    #             reader = csv.DictReader(f)
    #             for row in reader:
    #                 kn_cache[row['stop_name']] = row['stop_name_kn']
    #         print(f'Loaded {len(kn_cache)} existing Kannada translations')
    #     except Exception as e:
    #         print(f'WARNING: Could not load {kn_csv_path}: {e}')

    # Step 6: Build output GeoJSON
    geojson, platforms_routes = build_geojson(
        schedule_times, routes_en, routes_kn, stop_platforms,
        platforms_geojson, overrides, stop_ids, kn_cache,
        route_stops_api, file_nickname, trip_counts
    )

    # Write output
    os.makedirs(os.path.dirname(output_geojson_path), exist_ok=True)
    with open(output_geojson_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    print(f'Wrote {len(geojson["features"])} platform features to {output_geojson_path}')

    # Step 7: Filter and write stops-coordinates.json
    # Collect all unique stop IDs from the geojson
    stop_ids_in_geojson = set()
    for feature in geojson['features']:
        for route in feature['properties'].get('Routes', []):
            for stop in route.get('Stops', []):
                stop_id = str(stop.get('stop_id', ''))
                if stop_id:
                    stop_ids_in_geojson.add(stop_id)

    # Load GTFS stops to get coordinates
    stops_txt_path = os.path.join(GTFS_FOLDER, 'stops.txt')
    stops_coordinates = {}
    if os.path.exists(stops_txt_path):
        with open(stops_txt_path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stop_id = row['stop_id']
                if stop_id in stop_ids_in_geojson:
                    stops_coordinates[stop_id] = {
                        'name': row['stop_name'],
                        'lat': float(row['stop_lat']),
                        'lon': float(row['stop_lon'])
                    }

    # Write filtered stops-coordinates.json
    stops_coords_path = f'out/stops-coordinates-{file_nickname}.json'
    os.makedirs(os.path.dirname(stops_coords_path), exist_ok=True)
    with open(stops_coords_path, 'w', encoding='utf-8') as f:
        json.dump(stops_coordinates, f, ensure_ascii=False, indent=2)
    print(f'Wrote {len(stops_coordinates)} stop coordinates to {stops_coords_path}')

    # Save unknown/unsorted
    unknown = platforms_routes.get("UNKNOWN", [])
    unsorted = platforms_routes.get("UNSORTED", [])
    if unknown or unsorted:
        os.makedirs('help', exist_ok=True)
        with open(f'help/platforms-unaccounted-{file_nickname}.json', 'w', encoding='utf-8') as f:
            json.dump({"Unknown": unknown, "Unsorted": unsorted}, f, indent=2, ensure_ascii=False)
        print(f'Saved {len(unknown)} unknown + {len(unsorted)} unsorted routes to help/platforms-unaccounted-{file_nickname}.json')

    # Save updated Kannada cache
    # if kn_cache:
    #     with open(kn_csv_path, 'w', encoding='utf-8', newline='') as f:
    #         writer = csv.writer(f)
    #         writer.writerow(['stop_name', 'stop_name_kn'])
    #         for stop_name in sorted(kn_cache):
    #             writer.writerow([stop_name, kn_cache[stop_name]])
    #     print(f'Updated Kannada cache: {len(kn_cache)} entries in {kn_csv_path}')

    # Print cache stats
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM api_cache')
        total = cursor.fetchone()[0]
        conn.close()
        print(f'API cache contains {total} entries')
    except Exception:
        pass

    print(f'Completed {file_nickname}')


if __name__ == '__main__':
    main()
