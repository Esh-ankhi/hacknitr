import time
import requests
import pathway as pw
from datetime import timedelta
from pinecone import Pinecone

class OpenSkySchema(pw.Schema):
    icao24: str | None
    callsign: str | None
    origin_country: str | None
    time_position: int | None
    last_contact: int
    longitude: float | None
    latitude: float | None
    baro_altitude: float | None
    on_ground: bool | None
    velocity: float
    true_track: float | None
    vertical_rate: float | None
    geo_altitude: float | None
    squawk: str | None
    spi: bool | None
    position_source: int | None
    category: int | None

def safe_get(arr, idx):
    return arr[idx] if idx < len(arr) else None

class FlightStreamSubject(pw.io.python.ConnectorSubject):
    def run(self):
        url = 'https://opensky-network.org/api/states/all'

        while True:
            try:
                resp = requests.get(url)
                data = resp.json()

                states = data.get('states', [])
                now = data.get('time')

                for s in states:
                    record = {
                        'icao24': safe_get(s, 0),
                        'callsign': safe_get(s, 1).strip() if safe_get(s, 1) else None,
                        'origin_country': safe_get(s, 2),
                        'time_position': safe_get(s, 3),
                        'last_contact': safe_get(s, 4),
                        'longitude': safe_get(s, 5),
                        'latitude': safe_get(s, 6),
                        'baro_altitude': safe_get(s, 7),
                        'on_ground': safe_get(s, 8),
                        'velocity': safe_get(s, 9),
                        'true_track': safe_get(s, 10),
                        'vertical_rate': safe_get(s, 11),
                        'geo_altitude': safe_get(s, 13),
                        'squawk': safe_get(s, 14),
                        'spi': safe_get(s, 15),
                        'position_source': safe_get(s, 16),
                        'category': safe_get(s, 17), 
                    }

                    self.next(**record)

            except Exception as e:
                print(e)

            time.sleep(15)

flights_table = pw.io.python.read(
    FlightStreamSubject(),
    schema=OpenSkySchema,
)

timed_flights_table = flights_table.with_columns(
    time=pw.this.last_contact.dt.from_timestamp("s")
)

filtered_flights_table = timed_flights_table.filter(
    (pw.this.latitude.is_not_none()) &
    (pw.this.longitude.is_not_none()) &
    (pw.this.geo_altitude.is_not_none()) &
    (pw.this.velocity.is_not_none())
)

result = filtered_flights_table.windowby(
    filtered_flights_table.time,
    window=pw.temporal.session(max_gap=timedelta(minutes=1)),
    instance=filtered_flights_table.callsign,
).reduce(
    pw.this.callsign,
    session_start=pw.this._pw_window_start,
    session_end=pw.this._pw_window_end,
    velocities=pw.reducers.ndarray(pw.this.velocity),
    altitudes=pw.reducers.max(pw.this.geo_altitude),
    latitudes=pw.reducers.any(pw.this.latitude),
    longitudes=pw.reducers.any(pw.this.longitude),
    origin_countries=pw.reducers.any(pw.this.origin_country)
)

def on_change(key: pw.Pointer, row: dict, time: int, is_addition: bool):
    with open("output.txt", "a", encoding="utf-8") as f:
        f.write(f"Time: {time}, Addition: {is_addition}, Data: {row}\n")

pw.io.subscribe(result, on_change)

# pw.debug.compute_and_print(result)
pw.run()