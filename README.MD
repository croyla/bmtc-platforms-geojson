## [Unofficial] BMTC Platform information in GeoJSON
Unofficial GeoJSONs platform-route information for bus stands in bengaluru
in geojson file format.

### Output Files
Output files are in `out/`. These contain `platforms-routes-<stopname>.geojson`
Every feature is a point. Each feature represents a platform. Each feature contains the following properties:

| Property  | Value          | Optional | Description                                                                             |
|-----------|----------------|----------|-----------------------------------------------------------------------------------------|
| Platform  | String         | no       | A human-readable unique name of the platform                                            |
| Alias     | List           | yes      | A list of names the BMTC-API asociates with this platform (E.g B2 and 2)                |
| Icon      | String         | yes      | A character of the platform used for rendering on maps                                  |
| Color     | String Hexcode | no       | A hex color code to identify with the platform                                          |
| OpenHour  | Decimal Number | no       | Opening hour of this platform as a number (e.g 6:30 AM -> 6.5, 7 PM -> 19)              |
| CloseHour | Decimal Number | no       | Closing hour of this platform as a number                                               |
| Routes    | List           | no       | A list of objects that pass through this platform (According to BMTC and our overrides) |
| ApiIds    | List           | no       | A list of API stop ID strings that are referenced for this platform                     |

A route object contains the following properties:

| Property           | Value          | Description                                                                      |
|--------------------|----------------|----------------------------------------------------------------------------------|
| Route              | String         | The human-readable public facing name of this route                              |
| RouteId            | Number         | The BMTC-API id of this route (direction specific id)                            |
| RouteParentId      | Number         | The BMTC-API id of this route, used for live tracking (direction indifferent id) |
| Destination        | String         | The destination stop name of this route                                          |
| PlatformNumber     | String         | The reported platform of this route                                              |
| FromStationId      | Number         | The BMTC-API ID for the stop this platform/route is associated with              |
| KannadaDestination | String         | The kannada destination name for this route (as per BMTC-API)                    |
| Stops              | List           | An ordered list of stop objects this route goes through                          |

A stop object contains the following properties:

| Property | Value          | Description                                                                |
|----------|----------------|----------------------------------------------------------------------------|
| name     | String         | English name of the stop                                                   |
| name_kn  | String         | Kannada name of the stop, as per BMTC-API or transliterated via Varnam API |
| stop_id  | String         | The BMTC-API id of this stop                                               |
| lat      | Decimal Number | The center latitude of this stop, as per BMTC-API                          |
| lon      | Decimal Number | The center longitude of this stop, as per BMTC-API                         |


### Input Files
Input files are in `in/`. These contain the platform location for each stop. These files are user-contributed geojson files. The naming format is `platforms-<stopname>.geojson`
These are processed and populated by the script.
Every feature is a point. Each feature represents a platform. Each feature contains the following properties:

| Property  | Value          | Optional | Description                                                                     |
|-----------|----------------|----------|---------------------------------------------------------------------------------|
| Platform  | String         | no       | A human-readable unique name of the platform                                    |
| Alias     | List           | yes      | A list of names the BMTC-API asociates with this platform (E.g B2 and 2)        |
| Exclusive | Boolean        | yes      | Whether the routes in this platform should be exclusive to this platform or not |
| Icon      | String         | yes      | A character of the platform used for rendering on maps                          |
| Color     | String Hexcode | yes      | A hex color code to identify with the platform                                  |
| OpenHour  | Decimal Number | yes      | Opening hour of this platform as a number (e.g 6:30 AM -> 6.5, 7 PM -> 19)      |
| CloseHour | Decimal Number | yes      | Closing hour of this platform as a number                                       |

Additionally, there are two json files, `overrides.json` and `stops-platforms.json`

The `overrides.json` handles overriding the API data, and is structured like so: 
```
{
  stop_id: {
    route_id: override_platform_name
  }
}
```
The `stops-platforms.json` handles instances where a separate stop is to be treated like a platform, it is structured like below:
```
{
  stop_id: platform_name
}
```
### Raw Files
Raw files are in `raw/`. These files contain route - platform data in json files. The naming format is `platforms-<stopname>.json`
These files are intermediary files that are then used to populate the `in/` files and generate `out/` files.

### Running it yourself
#### 1. Installation
Clone the repo in your desired directory.
#### 2. Dependents
Ensure you have the python package `csv` installed, and you are using python 3. You must have a local bmtc-gtfs copy
with proper BMTC-API IDs to run the script.
## 3. Running
You can run the program itself by running `python3 <stop_id>... <stop_nickname> <nest_level>` or to generate / re-generate all files `bash commands.txt`

### Contributing
- The data for platforms - routes mapping is taken from BMTC-API, it is not accurate all the time. Simply creating an issue
for an inaccurate or unknown route wherein you can provide the actual platform for the route will allow this to be rectified
- The geojson / geographic data is manually created, and it may be inaccurate. Should new platforms open up, or old 
platforms shut down, this would need updating. Please create an issue for this matter, and provide your changes to existing 
files in a PR in which the issue is mentioned.
- The current stop collection does not represent all platformed stops operated by BMTC. If you are aware of one please raise
an issue on this repository. If you have already created a geojson for the same please create a PR referencing the issue.
- There are numerous platforms and buses that are not in the files. Please have a look at the `help/` directory to see where you can help.
The help directory is structured into "Unknown" and "Unsorted". "Unsorted" refers to an unrecognised platform, "Unknown" refers to no platform information.
- To add a new feature, fix a bug, or optimise some code please create the relevant issue and reference it in your PR.
- To update the files, please create a PR with the description noting the date, and if possible please link / upload the gtfs used for the run.
#### Tools for Contributing
- [geojson-blrtransit](https://geojson.blrtransit.com/)
- [geojson.io](https://geojson.io/)
- [bmtc-gtfs github](https://github.com/bengawalk/gtfs-blr/tree/main/bmtc)

### Credits

- [Namma BMTC](https://bmtcwebportal.amnex.com/commuter/dashboard)
- [Vonter's bmtc-gtfs](https://github.com/Vonter/bmtc-gtfs)
- [Bengawalk's gtfs-blr](https://github.com/bengawalk/gtfs-blr)

