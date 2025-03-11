import json
import sys
import urllib.parse
from datetime import datetime, timedelta

import requests


def load_endpoints(server_url):
    """Load API endpoints from the OpenAPI JSON endpoint."""
    openapi_url = f"{server_url}/openapi.json"
    response = requests.get(openapi_url, timeout=10)
    response.raise_for_status()
    api_spec = response.json()

    endpoints = {}
    for path, methods in api_spec.get("paths", {}).items():
        for method, details in methods.items():
            if method == "get":
                endpoints[path] = {
                    "parameters": details.get("parameters", []),
                    "summary": details.get("summary", ""),
                    "operationId": details.get("operationId", ""),
                }

    return endpoints


def generate_test_cases():
    """Generate test cases with various parameters for different endpoints."""
    now = datetime(2025, 2, 17, 15)
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    locations = [
        {"lat": 52.52, "lon": 13.41, "name": "Berlin"},
        {"lat": 52, "lon": 7.6, "name": "Münster"},
        {"lat": 51.54, "lon": 9.9, "name": "Münster"},
        {"lat": 53.55, "lon": 9.99, "name": "Hamburg"},
        {"lat": 48.14, "lon": 11.58, "name": "München"},
        {"lat": 50.95, "lon": 6.97, "name": "Köln"},
        {"lat": 53.8, "lon": 10.3, "name": "Glinde"},
        {"lat": 50.85, "lon": 5.69, "name": "Maastricht"},
        {"lat": 40.71, "lon": -74.01, "name": "New York"},
        {"lat": 35.69, "lon": 139.69, "name": "Tokyo"},
        {"lat": -33.87, "lon": 151.21, "name": "Sydney"},
    ]
    dwd_station_ids = ["01766", "07374"]
    wmo_station_ids = ["10315", "G005"]

    test_cases = {
        "/current_weather": [
            *[
                {
                    "lat": loc["lat"],
                    "lon": loc["lon"],
                    "desc": f"Current weather in {loc['name']}",
                }
                for loc in locations
            ],
            {
                "dwd_station_id": dwd_station_ids[0],
                "desc": f"Current weather at DWD station {dwd_station_ids[0]}",
            },
            {
                "wmo_station_id": wmo_station_ids[0],
                "desc": f"Current weather at WMO station {wmo_station_ids[0]}",
            },
            {
                "dwd_station_id": f"{dwd_station_ids[0]},{dwd_station_ids[1]}",
                "desc": "Current weather with multiple DWD station IDs",
            },
            {
                "wmo_station_id": f"{wmo_station_ids[0]},{wmo_station_ids[1]}",
                "desc": "Current weather with multiple WMO station IDs",
            },
            {
                "lat": 999.99,
                "lon": 999.99,
                "desc": "Current weather with invalid coordinates",
            },
            {
                "lat": "invalid",
                "lon": "invalid",
                "desc": "Current weather with non-numeric coordinates",
            },
            {
                "lat": 52.0,
                "desc": "Current weather with incomplete coordinates",
            },
            {
                "dwd_station_id": "nonexistent",
                "desc": "Current weather with nonexistent station ID",
            },
            {
                "lat": locations[0]["lat"],
                "lon": locations[0]["lon"],
                "tz": "Europe/Berlin",
                "units": "si",
                "desc": "With Berlin timezone and SI units",
            },
            {
                "lat": locations[0]["lat"],
                "lon": locations[0]["lon"],
                "tz": "Invalid/Timezone",
                "units": "invalid",
                "desc": "With invalid timezone and units",
            },
        ],
        "/weather": [
            *[
                {
                    "date": today,
                    "lat": loc["lat"],
                    "lon": loc["lon"],
                    "desc": f"Weather in {loc['name']} since yesterday",
                }
                for loc in locations
            ],
            {
                "date": f"{yesterday}T12:00:00+02:00",
                "last_date": f"{today}T12:00:00+02:00",
                "lat": locations[0]["lat"],
                "lon": locations[0]["lon"],
                "desc": "Weather with specific time range",
            },
            {
                "date": yesterday,
                "dwd_station_id": dwd_station_ids[0],
                "desc": f"Weather at DWD station {dwd_station_ids[0]}",
            },
            {
                "date": yesterday,
                "dwd_station_id": f"{dwd_station_ids[0]},{dwd_station_ids[1]}",
                "desc": "Weather with multiple DWD station IDs",
            },
            {
                "date": yesterday,
                "wmo_station_id": f"{wmo_station_ids[0]},{wmo_station_ids[1]}",
                "desc": "Weather with multiple WMO station IDs",
            },
            {
                "date": "invalid-date",
                "lat": locations[0]["lat"],
                "lon": locations[0]["lon"],
                "desc": "Weather with invalid date format",
            },
            {
                "date": yesterday,
                "last_date": "invalid-date",
                "lat": locations[0]["lat"],
                "lon": locations[0]["lon"],
                "desc": "Weather with invalid last_date format",
            },
            {
                "date": yesterday,
                "lat": 999.99,
                "lon": 999.99,
                "desc": "Weather with invalid coordinates",
            },
        ],
        "/radar": [
            *[
                {
                    "lat": loc["lat"],
                    "lon": loc["lon"],
                    "desc": f"Radar in {loc['name']}",
                }
                for loc in locations
            ],
            {
                "lat": locations[0]["lat"],
                "lon": locations[0]["lon"],
                "format": "bytes",
                "desc": "Radar with bytes format",
            },
            {
                "lat": locations[0]["lat"],
                "lon": locations[0]["lon"],
                "format": "plain",
                "desc": "Radar with plain format",
            },
            {
                "bbox": "100,100,300,300",
                "desc": "Radar with specific bounding box",
            },
        ],
        "/alerts": [
            *[
                {
                    "lat": loc["lat"],
                    "lon": loc["lon"],
                    "desc": f"Alerts in {loc['name']}",
                }
                for loc in locations
            ],
            {
                "desc": "All alerts",
            },
            {
                "warn_cell_id": 803159016,
                "desc": "Alerts for specific warn cell",
            },
        ],
        "/sources": [
            *[
                {
                    "lat": loc["lat"],
                    "lon": loc["lon"],
                    "desc": f"Sources near {loc['name']}",
                }
                for loc in locations
            ],
            {
                "dwd_station_id": dwd_station_ids[0],
                "desc": f"Source for DWD station {dwd_station_ids[0]}",
            },
            {
                "dwd_station_id": f"{dwd_station_ids[0]},{dwd_station_ids[1]}",
                "desc": "Sources for multiple DWD stations",
            },
            {
                "wmo_station_id": f"{wmo_station_ids[0]},{wmo_station_ids[1]}",
                "desc": "Sources for multiple WMO stations",
            },
            {
                "lat": "invalid",
                "lon": "invalid",
                "desc": "Sources with non-numeric coordinates",
            },
            {
                "observation_types": "invalid,types",
                "lat": locations[0]["lat"],
                "lon": locations[0]["lon"],
                "desc": "Sources with invalid observation types",
            },
        ],
        "/synop": [
            {
                "date": yesterday,
                "dwd_station_id": dwd_station_ids[0],
                "desc": f"SYNOP data for DWD station {dwd_station_ids[0]}",
            },
            {
                "date": yesterday,
                "wmo_station_id": wmo_station_ids[0],
                "desc": f"SYNOP data for WMO station {wmo_station_ids[0]}",
            },
        ],
    }

    return test_cases


def compare_responses(url1, url2, params, timeout=10):
    """Compare responses from two different URLs with the same parameters."""
    try:
        params = urllib.parse.urlencode(params, safe=",")
        resp1 = requests.get(url1, params=params, timeout=timeout)
        resp2 = requests.get(url2, params=params, timeout=timeout)

        if resp1.status_code != resp2.status_code:
            if {resp1.status_code, resp2.status_code} == {400, 422}:
                return (
                    True,
                    "Both servers returned validation errors (400/422)",
                )
            return (
                False,
                f"Status code mismatch: {resp1.status_code} vs {resp2.status_code}",  # noqa
            )

        if resp1.status_code != 200:
            return (
                True,
                f"Both servers returned same non-200 status: {resp1.status_code}",  # noqa
            )
        try:
            data1 = resp1.json()
            data2 = resp2.json()

            if data1 == data2:
                return True, "Responses match exactly"
            else:
                return False, "Response data mismatch"

        except json.JSONDecodeError:
            # If we can't parse JSON, compare raw content
            if resp1.content == resp2.content:
                return True, "Raw content matches"
            else:
                return False, "Raw content mismatch"

    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"


def run_tests(base_url1, base_url2, test_cases):
    """Run all test cases and report results."""
    results = {"passed": 0, "failed": 0, "details": []}

    for endpoint, cases in test_cases.items():
        print(f"\nTesting endpoint: {endpoint}")

        for i, params in enumerate(cases):
            desc = params.pop("desc", f"Test case {i + 1}")

            url1 = f"{base_url1}{endpoint}"
            url2 = f"{base_url2}{endpoint}"
            param_str = "&".join([f"{k}={v}" for k, v in params.items()])
            print(f"  {desc}: {endpoint}?{param_str}")

            success, message = compare_responses(url1, url2, params)

            if success:
                results["passed"] += 1
                status = "✓ PASS"
            else:
                results["failed"] += 1
                status = "✗ FAIL"

            print(f"    {status}: {message}")

            results["details"].append(
                {
                    "endpoint": endpoint,
                    "params": params,
                    "description": desc,
                    "success": success,
                    "message": message,
                }
            )

    return results


def main():
    server1 = "http://localhost:5000"
    server2 = "http://localhost:8000"
    print(f"Comparing responses between {server1} and {server2}")

    endpoints = load_endpoints(server2)
    print(f"Loaded {len(endpoints)} endpoints from {server2}/openapi.json")

    test_cases = generate_test_cases()
    results = run_tests(server1, server2, test_cases)

    print("\n" + "=" * 50)
    print(f"SUMMARY: {results['passed']} passed, {results['failed']} failed")
    print("=" * 50)
    return 1 if results["failed"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
