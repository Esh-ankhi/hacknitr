import time
import requests
import pathway as pw
from datetime import timedelta
from pinecone import Pinecone

class OpenSkySchema(pw.Schema):
    icao24: str
    callsign: str | None
    origin_country: str
    time_position: int | None
    last_contact: int
    longitude: float | None
    latitude: float | None
    baro_altitude: float | None
    on_ground: bool
    velocity: float
    true_track: float | None
    vertical_rate: float | None
    geo_altitude: float | None
    squawk: str | None
    spi: bool
    position_source: int
    category: int

class FlightStreamSubject(pw.io.python.ConnectorSubject):
    def run(self):
        url = 'https://hacknitr-api.vercel.app/api/states/all'

        while True:
            try:
                resp = requests.get(url)
                data = resp.json()

                states = data.get('states', [])
                now = data.get('time')

                for s in states:
                    record = {
                        'icao24': s[0],
                        'callsign': s[1].strip() if s[1] else None,
                        'origin_country': s[2],
                        'time_position': s[3],
                        'last_contact': s[4],
                        'longitude': s[5],
                        'latitude': s[6],
                        'baro_altitude': s[7],
                        'on_ground': s[8],
                        'velocity': s[9],
                        'true_track': s[10],
                        'vertical_rate': s[11],
                        'geo_altitude': s[13],
                        'squawk': s[14],
                        'spi': s[15],
                        'position_source': s[16],
                        'category': s[17],
                    }

                    self.next(**record)

            except Exception as e:
                print(e)

            time.sleep(5)

flights_table = pw.io.python.read(
    FlightStreamSubject(),
    schema=OpenSkySchema,
)

timed_flights_table = flights_table.with_columns(
    time=pw.this.last_contact.dt.from_timestamp("s")
)

result = timed_flights_table.windowby(
    timed_flights_table.time,
    window=pw.temporal.session(max_gap=timedelta(minutes=1)),
    instance=timed_flights_table.callsign,
).reduce(
    pw.this.callsign,
    session_start=pw.this._pw_window_start,
    session_end=pw.this._pw_window_end,
    velocities=pw.reducers.ndarray(pw.this.velocity),
)

def on_change(key: pw.Pointer, row: dict, time: int, is_addition: bool):
    with open("output.txt", "a", encoding="utf-8") as f:
        f.write(f"Time: {time}, Addition: {is_addition}, Data: {row}\n")

pw.io.subscribe(result, on_change)

pw.run()