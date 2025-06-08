import csv
import datetime
import json
import os
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests

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


def print_progress_bar(iteration, total, prefix='', length=40):
    percent = f"{100 * (iteration / float(total)):.1f}"
    filled_length = int(length * iteration // total)
    bar = '█' * filled_length + '-' * (length - filled_length)
    sys.stdout.write(f'\r{prefix} |{bar}| {percent}%')
    sys.stdout.flush()
    if iteration == total:
        print()  # Move to next line when done


def get_next_stops(stop_ids, nest_level=5):
    # Load GTFS files
    print("Loading trips.txt...")
    with open(f"{gtfs_folder}trips.txt", mode='r') as file:
        reader = csv.DictReader(file)
        trips_list = list(reader)
        trips = {x['trip_id']: x for x in trips_list}
    print(f"Loaded {len(trips)} trips.")

    print("Loading stop_times.txt...")
    with open(f"{gtfs_folder}stop_times.txt", mode='r') as file:
        reader = csv.DictReader(file)
        stop_times = list(reader)
    print(f"Loaded {len(stop_times)} stop times.")

    print("Loading stops.txt...")
    with open(f"{gtfs_folder}stops.txt", mode='r') as file:
        reader = csv.DictReader(file)
        stops = {row["stop_id"]: row["stop_name"] for row in reader}
    print(f"Loaded {len(stops)} stops.")

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

    print(f"Next stops mapping: {next_stops_total}")

    # print(f"Final nested stops mapping: {next_stops_total}")
    return next_stops_total


def save_platforms():
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

        print(f"Using nest level: {nest_level}")

        for arg in stop_ids:
            if arg in overrides_json.keys():
                overrides.update(overrides_json[arg])

    next_stops = get_next_stops(stop_ids, nest_level=nest_level)

    response = requests.post(f'{api_url}GetAllRouteList', headers=request_headers)
    routes = {route['routeid']: route for route in response.json().get('data', [])}

    schedule_times = {"Failed": [], "Received": {}}
    routes_done = set()
    routes_done_lock = threading.Lock()
    received_lock = threading.Lock()
    failed_stops = set()
    failed_stops_lock = threading.Lock()
    s = {l: set() for l in range(nest_level + 1)}

    file = sys.argv[-1] if not sys.argv[-1].isdigit() else sys.argv[-2]
    if os.path.exists(f'raw/platforms-{file}.json'):
        with open(f'raw/platforms-{file}.json', 'r') as p_m:
            x = json.loads(p_m.read())
            for y in x['Received']:
                with received_lock:
                    schedule_times[y['route-id']] = y
    tomorrow_start = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00')
    tomorrow_end = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d 23:59')

    def send_request(from_stop, to_stop):
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
        print(f'Sending query for stops {from_stop} -> {to_stop}')
        now = time.time()
        try:
            response = requests.post(f'{api_url}GetTimetableByStation_v4', headers=request_headers, data=data).json()
        except:
            print("Failed in response.json")
            response = {"isException": True, "Issuccess": False, "exception": "Response not received in JSON.",
                        "Message": "Response not received in JSON."}
        print(f"Received in {time.time() - now} seconds")

        is_failed = (
                response.get("exception") not in (None, False) or
                response.get("isException") is True or
                response.get("Issuccess") is not True
        )
        return from_stop, to_stop, response, is_failed

    # Main execution
    with ThreadPoolExecutor(max_workers=10) as executor:
        for stop in stop_ids:
            s[0].add(stop)
            for level in range(nest_level):
                print(f"Processing stop {stop}, current level: {level}")
                to_break = True
                futures = []
                for b in s[level]:
                    for n in next_stops[b]:
                        futures.append(executor.submit(send_request, stop, n))

                for future in as_completed(futures):
                    from_stop, to_stop, response, is_failed = future.result()
                    if is_failed:
                        with failed_stops_lock:
                            if level == nest_level - 1:
                                failed_stops.add(from_stop)
                        s[level + 1].add(to_stop)
                        to_break = False
                    else:
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
                                schedule_times["Received"][route_id] = {
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
                if to_break:
                    break

    print(f'Failed in processing {len(failed_stops)} stop(s)')
    print(f'Succeeded in receiving {len(schedule_times["Received"])}')
    schedule_times["Received"] = list(schedule_times["Received"].values())
    with open(f'raw/platforms-{file}.json', 'w') as p_m:
        p_m.write(json.dumps(schedule_times, indent=2))

    return schedule_times


def geo_json():
    platforms_geo: dict  # Platform: List of routes
    geojson_json: dict
    platforms_majestic: dict
    stops_platforms: dict
    stops_loc: dict
    print("Loading stops.txt...")
    with open(f"{gtfs_folder}stops.txt", mode='r') as file:
        reader = csv.DictReader(file)
        stops_loc = {row["stop_id"]: [float(row["stop_lat"]), float(row["stop_lon"])] for row in reader}
    with open('stops-platforms.json', 'r') as p_m:
        stops_platforms = json.loads(p_m.read().replace('\n', ''))
    file = sys.argv[-1] if not sys.argv[-1].isdigit() else sys.argv[-2]
    with open(f'raw/platforms-{file}.json', 'r') as p_m:
        platforms_majestic = json.loads(p_m.read().replace('\n', ''))
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
    if not platforms_geo or not platforms_majestic:
        return ''
    platforms_names = [feature["properties"]["Platform"] for feature in geojson_json["features"]]
    # Add stops (not identified as platforms by BMTC API) to geojson and platforms_geo
    for stop_id, stop_name in stops_platforms.items():
        if stop_id in stops_loc and stop_id in sys.argv:
            platforms_geo[stop_name] = []
            if stop_name in platforms_names:
                continue
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
    for route in platforms_majestic["Received"]:
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
                for stop in stop_times[trips[str(route["Id"])][0]["trip_id"]]:
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
