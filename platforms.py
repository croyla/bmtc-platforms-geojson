import csv
import datetime
import json
import os
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sqlite3
import hashlib

import requests

# Cache configuration
CACHE_DB_PATH = 'api_cache.db'
CACHE_DURATION_HOURS = 24

def init_cache_db():
    """Initialize the SQLite cache database"""
    conn = sqlite3.connect(CACHE_DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS api_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_hash TEXT UNIQUE NOT NULL,
            from_stop TEXT NOT NULL,
            to_stop TEXT NOT NULL,
            request_data TEXT NOT NULL,
            response_data TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def get_cache_key(from_stop, to_stop, request_data):
    """Generate a unique cache key for the request"""
    # Create a hash of the request parameters
    request_string = "{}:{}:{}".format(from_stop, to_stop, request_data)
    return hashlib.md5(request_string.encode()).hexdigest()

def get_cached_response(from_stop, to_stop, request_data):
    """Get cached response if it exists and is not expired"""
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        
        cache_key = get_cache_key(from_stop, to_stop, request_data)
        
        # Get response if it exists and is not older than CACHE_DURATION_HOURS
        cursor.execute('''
            SELECT response_data FROM api_cache 
            WHERE request_hash = ? 
            AND created_at > datetime('now', '-%d hours')
        ''' % CACHE_DURATION_HOURS, (cache_key,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            print('        cache hit: {} -> {}'.format(from_stop, to_stop))
            return json.loads(result[0])
        else:
            print('        cache miss: {} -> {}'.format(from_stop, to_stop))
            return None
            
    except Exception as e:
        print('        cache error: {}'.format(e))
        return None

def store_cached_response(from_stop, to_stop, request_data, response_data):
    """Store response in cache"""
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        
        cache_key = get_cache_key(from_stop, to_stop, request_data)
        
        cursor.execute('''
            INSERT OR REPLACE INTO api_cache 
            (request_hash, from_stop, to_stop, request_data, response_data, created_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (cache_key, from_stop, to_stop, request_data, json.dumps(response_data)))
        
        conn.commit()
        conn.close()
        print('        cached: {} -> {}'.format(from_stop, to_stop))
        
    except Exception as e:
        print('        cache store error: {}'.format(e))

def cleanup_expired_cache():
    """Remove expired cache entries"""
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            DELETE FROM api_cache 
            WHERE created_at <= datetime('now', '-%d hours')
        ''' % CACHE_DURATION_HOURS)
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        if deleted_count > 0:
            print('Cleaned up {} expired cache entries'.format(deleted_count))
            
    except Exception as e:
        print('Cache cleanup error: {}'.format(e))

request_headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Content-Type': 'application/json',
    'lan': 'en',
    'deviceType': 'WEB',
    'Origin': 'https://bmtcwebportal.amnex.com',
    'Referer': 'https://bmtcwebportal.amnex.com/'
}
api_url = 'https://bmtcmobileapi.karnataka.gov.in/WebAPI/'

gtfs_folder = '../bmtc-19-07-2024/'  # This gtfs folder is our source for stops, as opposed to querying api


def get_next_stops(stop_ids, nest_level=5):
    print('starting get_next_stops')
    # Load GTFS files
    # with open(f"{gtfs_folder}trips.txt", mode='r') as file:
    #     reader = csv.DictReader(file)
    #     trips_list = list(reader)
    #     trips = {x['trip_id']: x for x in trips_list}
    with open(f"{gtfs_folder}stop_times.txt", mode='r') as file:
        reader = csv.DictReader(file)
        stop_times = list(reader)
    # with open(f"{gtfs_folder}stops.txt", mode='r') as file:
    #     reader = csv.DictReader(file)
    #     stops = {row["stop_id"]: row["stop_name"] for row in reader}

    next_stops_total = {stop_id: [] for stop_id in stop_ids}

    # Group stop_times by trip_id for faster access
    stop_times_by_trip = {}
    for st in stop_times:
        stop_times_by_trip.setdefault(st['trip_id'], []).append(st)

    # Sort stop_times within each trip by stop_sequence
    for trip_id in stop_times_by_trip:
        stop_times_by_trip[trip_id].sort(key=lambda x: int(x['stop_sequence']))


    for stop_id in stop_ids:
        for st in stop_times:
            if st['stop_id'] != stop_id:
                continue

            trip_id = st['trip_id']
            trip_stop_times = stop_times_by_trip[trip_id]

            # Find index of current stop
            current_index = next(
                (i for i, x in enumerate(trip_stop_times) if x['stop_id'] == stop_id), None)

            if current_index is None:
                continue

            # Traverse up to `n` next stops from this stop along this trip
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
    print('finished get_next_stops')

    return next_stops_total


def save_platforms():
    print('starting save_platforms')
    
    # Initialize cache database and cleanup expired entries
    init_cache_db()
    cleanup_expired_cache()
    
    overrides: dict
    with open('overrides.json', 'r') as p_m:
        overrides_json = json.loads(p_m.read().replace('\n', ''))
        overrides = {}
        # Extract nest_level from command-line arguments
        if sys.argv[-1].isdigit():  # Check if the last argument is a number
            nest_level = int(sys.argv[-1])
            stop_ids = sys.argv[1:-2]  # Exclude the last two arguments (majestic and nest_level)
        else:
            nest_level = 2  # Default value if no nest_level is provided
            stop_ids = sys.argv[1:-1]  # Exclude only the last argument (majestic)

        for arg in stop_ids:
            if arg in overrides_json.keys():
                overrides.update(overrides_json[arg])

    next_stops = get_next_stops(stop_ids, nest_level=nest_level)

    response = requests.post(f'{api_url}GetAllRouteList', headers=request_headers, data='{}')
    try:
        response_json = response.json()
    except Exception as e:
        print("Error decoding JSON from GetAllRouteList:", e)
        print("Response text:", response.text)
        response_json = {}
    routes = {route['routeid']: route for route in response_json.get('data', [])}
    print(f"Loaded {len(routes)} routes from GetAllRouteList API")
    if len(routes) == 0:
        print("Warning: No routes loaded from GetAllRouteList API - this may cause issues with route metadata")
    else:
        print(f"Sample route IDs: {list(routes.keys())[:5]}")

    schedule_times = {"Failed": [], "Received": []}
    routes_done = set()
    routes_done_lock = threading.Lock()
    received_lock = threading.Lock()
    failed_lock = threading.Lock()
    failed_stops = set()
    failed_stops_lock = threading.Lock()
    s = {l: set() for l in range(nest_level + 1)}

    file = sys.argv[-1] if not sys.argv[-1].isdigit() else sys.argv[-2]
    if os.path.exists(f'raw/platforms-{file}.json'):
        with open(f'raw/platforms-{file}.json', 'r') as p_m:
            x = json.loads(p_m.read())
            # Load only successful entries from previous run, reset failed entries
            schedule_times["Received"] = x.get('Received', [])
    tomorrow_start = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00')
    tomorrow_end = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d 23:59')

    def send_request(from_stop, to_stop):
        print(f'        sending request {from_stop} to {to_stop}')
        data = f'''
            {{
            "fromStationId":{int(from_stop)},
            "toStationId":{int(to_stop)},
            "p_startdate":"{tomorrow_start}",
            "p_enddate":"{tomorrow_end}",
            "p_isshortesttime":0,
            "p_routeid":"",
            "p_date":"{tomorrow_start}"
            }}
        '''
        
        # Check cache first
        cached_response = get_cached_response(from_stop, to_stop, data)
        if cached_response is not None:
            # Return cached response
            is_failed = (
                cached_response.get("exception") not in (None, False) or
                cached_response.get("isException") is True or
                cached_response.get("Issuccess") is not True
            )
            return from_stop, to_stop, cached_response, is_failed
        
        # If not in cache, make actual API call
        print(f'        sending request {from_stop} to {to_stop}')
        now = time.time()
        try:
            response = requests.post(f'{api_url}GetTimetableByStation_v4', headers=request_headers, data=data).json()
        except:
            response = {"isException": True, "Issuccess": False, "exception": "Response not received in JSON.",
                        "Message": "Response not received in JSON."}

        # Store response in cache
        store_cached_response(from_stop, to_stop, data, response)

        is_failed = (
                response.get("exception") not in (None, False) or
                response.get("isException") is True or
                response.get("Issuccess") is not True
        )
        return from_stop, to_stop, response, is_failed

    # Main execution
    with ThreadPoolExecutor(max_workers=10) as executor:
        for stop in stop_ids:
            print(f'processing stop {stop}')
            s[0].add(stop)
            for level in range(nest_level):
                has_failures = False
                futures = []
                print(f'    processing level {level}')
                print(f'        processing stops at level {level}: {list(s[level])}')
                for b in s[level]:
                    if b not in next_stops:
                        continue
                    for n in next_stops[b]:
                        futures.append(executor.submit(send_request, stop, n))

                for future in as_completed(futures):
                    from_stop, to_stop, response, is_failed = future.result()
                    if is_failed:
                        # Log the failed query
                        with failed_lock:
                            schedule_times["Failed"].append({
                                "from_stop": from_stop,
                                "to_stop": to_stop,
                                "response": response,
                                "level": level
                            })
                        with failed_stops_lock:
                            if level == nest_level - 1:
                                failed_stops.add(from_stop)
                        # Only add to next level if this path failed (we need to explore further)
                        s[level + 1].add(to_stop)
                        print(f'        failed: {from_stop} -> {to_stop}, adding {to_stop} to level {level + 1}')
                        has_failures = True
                    else:
                        print(f'        success: {from_stop} -> {to_stop}, NOT adding {to_stop} to next level')
                        for route_entry in response.get("data", []):
                            route_id = route_entry["routeid"]
                            pf_name = overrides.get(str(route_id), route_entry["platformname"])
                            pf_num = overrides.get(str(route_id), route_entry["platformnumber"])
                            with routes_done_lock:
                                if route_id in routes_done:
                                    continue
                                if (pf_name and pf_name != "") or (pf_num and pf_num != ""): # Add only if platform is populated
                                    routes_done.add(route_id)
                            with received_lock:
                                # Check if route_id exists in routes dictionary
                                if route_id not in routes:
                                    print(f"        warning: route_id {route_id} not found in routes dictionary, skipping")
                                    continue
                                
                                # Create new entry
                                new_entry = {
                                    "route-number": route_entry['routeno'],
                                    "extended-route-number": routes[route_id]['routeno'],
                                    "route-name": route_entry["routename"],
                                    "start-station": routes[route_id]['fromstation'],
                                    "start-station-id": routes[route_id]['fromstationid'],
                                    "from-station-id": route_entry['fromstationid'],
                                    "route-id": route_id,
                                    "to-station-id": routes[route_id]["tostationid"],
                                    "to-station": routes[route_id]["tostation"],
                                    "platform-name": overrides.get(str(route_id), route_entry["platformname"]),
                                    "platform-number": overrides.get(str(route_id), route_entry["platformnumber"]),
                                    "bay-number": route_entry["baynumber"]
                                }
                                
                                # Check if entry already exists and update it, otherwise add new entry
                                existing_index = None
                                for i, existing_entry in enumerate(schedule_times["Received"]):
                                    if existing_entry.get("route-id") == route_id:
                                        existing_index = i
                                        break
                                
                                if existing_index is not None:
                                    # Update existing entry
                                    schedule_times["Received"][existing_index] = new_entry
                                else:
                                    # Add new entry
                                    schedule_times["Received"].append(new_entry)
                
                # If no failures at this level, we can stop (all successful)
                if not has_failures:
                    print(f'    all requests successful at level {level}, stopping')
                    break
    with open(f'raw/platforms-{file}.json', 'w') as p_m:
        p_m.write(json.dumps(schedule_times, indent=2))
    
    # Print cache statistics
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM api_cache')
        total_cached = cursor.fetchone()[0]
        conn.close()
        print(f'Cache contains {total_cached} entries')
    except Exception as e:
        print(f'Could not get cache statistics: {e}')
    
    print('finished save_platforms')
    return schedule_times


def geo_json():
    platforms_geo: dict  # Platform: List of routes
    geojson_json: dict
    platforms_raw: dict
    stops_platforms: dict
    stops_loc: dict
    with open(f"{gtfs_folder}stops.txt", mode='r') as file:
        reader = csv.DictReader(file)
        stops_loc = {row["stop_id"]: [float(row["stop_lat"]), float(row["stop_lon"])] for row in reader}
    with open('stops-platforms.json', 'r') as p_m:
        stops_platforms = json.loads(p_m.read().replace('\n', ''))
    file = sys.argv[-1] if not sys.argv[-1].isdigit() else sys.argv[-2]
    with open(f'raw/platforms-{file}.json', 'r') as p_m:
        platforms_raw = json.loads(p_m.read().replace('\n', ''))
    with open(f'in/platforms-{file}.geojson', 'r') as p_m_g:
        geojson_json = json.loads(p_m_g.read().replace('\n', ''))
        for feature in geojson_json["features"]:
            feature["properties"]["Platform"] = str(feature["properties"]["Platform"]).upper()
        platforms_geo = {}
        for feature in geojson_json["features"]:
            if feature["geometry"]["type"] == "Point":
                platforms_geo[feature["properties"]["Platform"]] = []
                if "Alias" in feature["properties"].keys():
                    for alias in feature["properties"]["Alias"]:
                        platforms_geo[str(alias)] = []

        platforms_geo["Unknown"] = []
        platforms_geo["Unsorted"] = []
    if not platforms_geo or not platforms_raw:
        return ''
    platforms_names = [feature["properties"]["Platform"] for feature in geojson_json["features"]]
    stop_names = []
    # Add stops (not identified as platforms by BMTC API) to geojson and platforms_geo
    for stop_id, stop_name in stops_platforms.items():
        if stop_id in stops_loc and stop_id in sys.argv:
            platforms_geo[stop_name] = []
            if stop_name in platforms_names or stop_name in stop_names:
                continue
            stop_names.append(stop_name)
            geojson_json["features"].append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        stops_loc[stop_id][1], stops_loc[stop_id][0]
                    ]
                },
                "properties": {
                    "Platform": stop_name
                }
            })

    # Process received routes and populate local data accordingly
    for route in platforms_raw["Received"]:
        platform = stops_platforms[str(route['from-station-id'])] if str(route['from-station-id']) in stops_platforms else route['platform-name'] if route['platform-name'] != "" else route['platform-number']
        if platform == "" or platform is None:
            platforms_geo["Unknown"].append(route)
            continue
        if platform in platforms_geo.keys():
            platforms_geo[platform].append(route)
            continue
        platforms_geo["Unsorted"].append(route)
    for feature in geojson_json["features"]:
        if feature["geometry"]["type"] == "Point":
            feature["properties"]["Routes"] = [{
                "Name": route['route-number'],
                "Destination": route['to-station'],
                "From": route['from-station-id'],
                "UniqueName": route['route-name'],
                "Id": route['route-id'],
                "BayReported": route['bay-number']
            } for route in platforms_geo[str(feature["properties"]["Platform"])]]
            if "Alias" in feature["properties"].keys():
                for alias in feature["properties"]["Alias"]:
                    feature["properties"]["Routes"].extend([{
                        "Name": route['route-number'],
                        "Destination": route['to-station'],
                        "From": route['from-station-id'],
                        "UniqueName": route['route-name'],
                        "Id": route['route-id'],
                        "BayReported": route['bay-number']
                    } for route in platforms_geo[str(alias)]])

    # Save all data. Unknown and Unsorted as well.
    with open(f'out/platforms-routes-{file}.geojson', 'w') as p_m_g:
        p_m_g.write(json.dumps(geojson_json, indent=2))
    if (len(platforms_geo["Unknown"]) > 0) or (len(platforms_geo["Unsorted"]) > 0):
        with open(f'help/platforms-unaccounted-{file}.json', 'w') as p_u:
            p_u.write(
                json.dumps({"Unknown": platforms_geo["Unknown"], "Unsorted": platforms_geo["Unsorted"]}, indent=2))

    return geojson_json


def add_routes_gtfs_geojson():
    # Extract nest_level from command-line arguments
    if sys.argv[-1].isdigit():  # Check if the last argument is a number
        stop_ids = sys.argv[1:-2]  # Exclude the last two arguments (majestic and nest_level)
    else:
        stop_ids = sys.argv[1:-1]  # Exclude only the last argument (majestic)
    geojson_json: dict
    file = sys.argv[-1] if not sys.argv[-1].isdigit() else sys.argv[-2]
    with (open(f'out/platforms-routes-{file}.geojson', 'r') as p_m_g):
        geojson_json = json.loads(p_m_g.read().replace('\n', ''))

    def get_dicts(filename) -> list[dict]:
        data = []
        with open(f'{gtfs_folder}{filename}', 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            data = [dict(read) for read in reader]
        return data

    def group_by(key, li: list):
        # Group values by 'id' and honor the sequence value
        grouped_data = {}
        for item in li:
            if item[key] not in grouped_data.keys():
                grouped_data[item[key]] = []
            grouped_data[item[key]].append(item)
        return grouped_data

    stops = {stop["stop_id"]: stop["stop_name"] for stop in get_dicts('stops.txt')}

    trips = group_by('route_id', get_dicts('trips.txt'))
    stop_times = {
        key:
            sorted(items, key=lambda x: int(x['stop_sequence']))
        for key, items in group_by('trip_id', get_dicts('stop_times.txt')).items()
    }
    for feature in geojson_json["features"]:
        for route in feature["properties"]["Routes"]:
            stops_now = []
            if trips.keys().__contains__(str(route["Id"])):
                loop_stops = stop_times[trips[str(route["Id"])][0]["trip_id"]]
                loop_stops = loop_stops[next((i for i, stop in enumerate(loop_stops) if stop['stop_id'] in stop_ids), None):]
                for stop in loop_stops:
                    stops_now.append(stops[stop['stop_id']])
            route["Stops"] = stops_now
    with open(f'out/platforms-routes-{file}.geojson', 'w') as p_m_g:
        p_m_g.write(json.dumps(geojson_json, indent=2))
    return geojson_json


if __name__ == '__main__':
    print(sys.argv)
    save_platforms()
    geo_json()
    add_routes_gtfs_geojson()
    print(f"Completed {sys.argv[-2]}")
