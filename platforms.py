import csv
import datetime
import json
import time
import sys

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
api_url = 'https://bmtcmobileapistaging.amnex.com/WebAPI/'

gtfs_folder = '../bmtc-19-07-2024/'  # This gtfs folder is our source for stops, as opposed to querying api


def get_next_stops(stop_ids):
    # Load GTFS files
    stop_times = []
    print("Loading stop_times.txt...")
    # Load stop_times.txt which contains stop sequence information for each trip
    with open(f"{gtfs_folder}stop_times.txt", mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            stop_times.append(row)
    print(f"Loaded {len(stop_times)} stop times.")

    trips = []
    print("Loading trips.txt...")
    # Load trips.txt which contains trip and route information
    with open(f"{gtfs_folder}trips.txt", mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            trips.append(row)
    print(f"Loaded {len(trips)} trips.")

    stops = []
    print("Loading stops.txt...")
    # Load stops.txt which contains stop information, including stop names
    with open(f"{gtfs_folder}stops.txt", mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            stops.append(row)
    print(f"Loaded {len(stops)} stops.")

    # Create a set to store unique next stops
    next_stops_total = {}
    for stop_id in stop_ids:
        next_stops = set()
        print(f"Processing stop_id: {stop_id}")
        # Get the list of stop times that include the given stop_id
        trips_with_stop = [stop_time for stop_time in stop_times if stop_time['stop_id'] == stop_id]
        print(f"Found {len(trips_with_stop)} trips with stop_id {stop_id}.")

        # Get one trip_id per route to avoid processing all trips of the same route
        trip_ids = set()
        route_ids_got = set()
        for trip in trips_with_stop:
            # Find the route_id for the current trip_id
            route_id = next((t['route_id'] for t in trips if t['trip_id'] == trip['trip_id']), None)
            # Add the trip_id to the set if the route_id has not been processed yet
            if route_id and route_id not in route_ids_got:
                route_ids_got.add(route_id)
                trip_ids.add(trip['trip_id'])
                print(f"Added trip_id {trip['trip_id']} for route_id {route_id}.")

        for trip_id in trip_ids:
            # print(f"Processing trip_id: {trip_id}")
            # Get all stop times for the specific trip, sorted by stop_sequence to maintain the correct order
            trip_stop_times = sorted([stop_time for stop_time in stop_times if stop_time['trip_id'] == trip_id],
                                     key=lambda x: int(x['stop_sequence']))
            # print(f"Trip {trip_id} has {len(trip_stop_times)} stops.")

            # Find the current stop in the sequence
            current_stop_index = next(
                (index for index, stop_time in enumerate(trip_stop_times) if stop_time['stop_id'] == stop_id), None)
            # print(f"Current stop index for stop_id {stop_id} in trip_id {trip_id}: {current_stop_index}")

            # Check if there is a next stop in the sequence
            if current_stop_index is not None and current_stop_index + 1 < len(trip_stop_times):
                # Get the next stop_id from the sequence
                next_stop_id = trip_stop_times[current_stop_index + 1]['stop_id']
                # Find the name of the next stop using the stop_id
                next_stop_name = next((stop['stop_name'] for stop in stops if stop['stop_id'] == next_stop_id), None)
                # print(f"Next stop for stop_id {stop_id} in trip_id {trip_id}: {next_stop_name} {next_stop_id}")

                # Add the next stop name to the set of unique next stops
                if next_stop_name:
                    next_stops.add(next_stop_id)
        next_stops_total[stop_id] = list(next_stops)

    print(f"Unique next stops: {next_stops_total}")
    return next_stops_total


def save_platforms():
    overrides: dict
    with open('overrides.json', 'r') as p_m:  # Overrides contain manual overrides for routes wrongly provided by API
        # For example KIA-9 is on platform 30, but it is retrieved in API under platform 5
        overrides_json = json.loads(p_m.read().replace('\n', ''))
        overrides = {}
        for arg in sys.argv[1:-1]:
            if arg in overrides_json.keys():
                overrides.update(overrides_json[arg])
    next_stops = get_next_stops(sys.argv[1:-1])

    response = requests.post(f'{api_url}GetAllRouteList',
                             headers=request_headers)
    routes = {route['routeid']: route for route in response.json()['data']}

    schedule_times = {"Failed": [], "Received": []}
    print(
        f'using overrides {overrides}')
    # Introduced override for when we know a platform is wrong, and we know the correct platform as well

    tomorrow_start = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00')
    tomorrow_end = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d 23:59')
    routes_done = set()
    for stop, stops in next_stops.items():
        for query in stops:
            if stops.index(query) != 0:
                time.sleep(1.5)
            data = f'''
                    {{
                    "fromStationId":{int(stop)},
                    "toStationId":{int(query)},
                    "p_startdate":"{tomorrow_start}",
                    "p_enddate":"{tomorrow_end}",
                    "p_isshortesttime":0,
                    "p_routeid":"",
                    "p_date":"{tomorrow_start}"
                    }}
                    '''
            print(f'Sending query for stops {stop} and {query}')
            # print('With data')
            # print(data)
            now = time.time()

            response = requests.post(f'{api_url}GetTimetableByStation_v4',
                                     headers=request_headers, data=data).json()

            print(f"Received in {time.time() - now} seconds")

            response_data = response['data'] if 'data' in response.keys() else None
            if not response_data or response_data is None or len(response_data) == 0:
                schedule_times["Failed"].append({"stop": stop, "next": query, "schedule": response})
                continue
            for route_entry in response_data:
                if not route_entry['routeid'] in routes_done:
                    routes_done.add(route_entry['routeid'])
                    schedule_times["Received"].append(
                        {
                            "route-number": route_entry['routeno'],
                            "extended-route-number": routes[route_entry['routeid']]['routeno'],
                            "route-name": route_entry["routename"],
                            "start-station": routes[route_entry['routeid']]['fromstation'],
                            "start-station-id": routes[route_entry['routeid']]['fromstationid'],  # Start of route
                            "from-station-id": route_entry['fromstationid'],  # Current station id
                            "route-id": route_entry['routeid'],
                            "to-station-id": routes[route_entry['routeid']]["tostationid"],  # End of route
                            "to-station": routes[route_entry['routeid']]["tostation"],
                            "platform-name": route_entry["platformname"] if not str(
                                route_entry['routeid']) in overrides.keys() else overrides[str(route_entry['routeid'])],
                            "platform-number": route_entry["platformnumber"] if not str(
                                route_entry['routeid']) in overrides.keys() else overrides[str(route_entry['routeid'])],
                            "bay-number": route_entry["baynumber"]
                        }
                    )

    print(f'Failed in receiving {len(schedule_times["Failed"])}')
    print(f'Succeeded in receiving {len(schedule_times["Received"])}')
    with open(f'out/platforms-{sys.argv[-1]}.json', 'w') as p_m:
        p_m.write(json.dumps(schedule_times, indent=2))
    return schedule_times


def geo_json():
    platforms_geo: dict  # Platform: List of routes
    geojson_json: dict
    platforms_majestic: dict
    with open(f'out/platforms-{sys.argv[-1]}.json', 'r') as p_m:
        platforms_majestic = json.loads(p_m.read().replace('\n', ''))
    with (open(f'out/platforms-{sys.argv[-1]}.geojson', 'r') as p_m_g):
        geojson_json = json.loads(p_m_g.read().replace('\n', ''))
        for feature in geojson_json["features"]:
            print(feature)
            feature["properties"]["Platform"] = str(feature["properties"]["Platform"]).upper()
        platforms_geo = {feature["properties"]["Platform"]: [] for feature in geojson_json["features"]
                         if feature["geometry"]["type"] == "Point"}
        platforms_geo["Unknown"] = []
        platforms_geo["Unsorted"] = []
    if not platforms_geo or not platforms_majestic:
        return ''
    for route in platforms_majestic["Received"]:
        platform = route['platform-name'] if route['platform-name'] != "" else route['platform-number']
        if platform == "":
            platforms_geo["Unknown"].append(route)
            continue
        if platform in platforms_geo.keys():
            platforms_geo[platform].append(route)
            continue
        platforms_geo["Unsorted"].append(route)
    for feature in geojson_json["features"]:
        feature["properties"]["Routes"] = [{
            "Name": route['route-number'],
            "Destination": route['to-station'],
            "From": route['from-station-id'],
            "UniqueName": route['route-name'],
            "Id": route['route-id'],
            "BayReported": route['bay-number']
        } for route in platforms_geo[str(feature["properties"]["Platform"])]]
    with open(f'out/platforms-routes-{sys.argv[-1]}.geojson', 'w') as p_m_g:
        p_m_g.write(json.dumps(geojson_json, indent=2))
    if (len(platforms_geo["Unknown"]) > 0) or (len(platforms_geo["Unsorted"]) > 0):
        with open(f'out/help/platforms-unaccounted-{sys.argv[-1]}.json', 'w') as p_u:
            p_u.write(
                json.dumps({"Unknown": platforms_geo["Unknown"], "Unsorted": platforms_geo["Unsorted"]}, indent=2))

    return geojson_json


def add_routes_gtfs_geojson():
    geojson_json: dict
    with (open(f'out/platforms-routes-{sys.argv[-1]}.geojson', 'r') as p_m_g):
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
    with open(f'out/platforms-routes-{sys.argv[-1]}.geojson', 'w') as p_m_g:
        p_m_g.write(json.dumps(geojson_json, indent=2))
    return geojson_json


if __name__ == '__main__':
    print(sys.argv)
    print(save_platforms())
    print(geo_json())
    print(add_routes_gtfs_geojson())
