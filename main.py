import time
import requests
import pathway as pw
from datetime import timedelta
from pinecone import Pinecone

INDIA_LAT_MIN = 2.5
INDIA_LAT_MAX = 38.5
INDIA_LON_MIN = 63.5
INDIA_LON_MAX = 99.5

class OpenSkySchema(pw.Schema):
    icao24: str | None
    callsign: str | None
    origin_country: str | None
    time_position: int | None
    last_contact: int
    longitude: float
    latitude: float
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

            time.sleep(30)

flights_table = pw.io.python.read(
    FlightStreamSubject(),
    schema=OpenSkySchema,
)

timed_flights_table = flights_table.with_columns(
    time=pw.this.last_contact.dt.from_timestamp("s")
)

india_flights = timed_flights_table.filter(
    pw.this.latitude.is_not_none() &
    pw.this.longitude.is_not_none() &
    pw.this.geo_altitude.is_not_none() &
    pw.this.velocity.is_not_none() &

    (pw.this.latitude >= INDIA_LAT_MIN) &
    (pw.this.latitude <= INDIA_LAT_MAX) &
    (pw.this.longitude >= INDIA_LON_MIN) &
    (pw.this.longitude <= INDIA_LON_MAX)
)

result = india_flights.windowby(
    india_flights.time,
    window=pw.temporal.session(max_gap=timedelta(minutes=1)),
    instance=india_flights.callsign,
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

sentences = result.with_columns(
    sentence=pw.apply(
        lambda callsign, start, end, vels, alt, lat, lon, country:
        (
            f"Flight {callsign} from {country} was tracked from {start} to {end}. "
            f"It reached a maximum altitude of {round(alt, 2)} meters, "
            f"with speeds ranging between {round(min(vels), 2)} and {round(max(vels), 2)} m/s. "
            f"The last known position was latitude {round(lat, 4)}, longitude {round(lon, 4)}."
        ),
        pw.this.callsign,
        pw.this.session_start,
        pw.this.session_end,
        pw.this.velocities,
        pw.this.altitudes,
        pw.this.latitudes,
        pw.this.longitudes,
        pw.this.origin_countries,
    )
)

# Pinecone Integration
pc = Pinecone(api_key="pcsk_51k6TP_RVkPcs4D5K8i8Gwj5BL6ZovfQSckBmcfNjLkZbE51D514vHEJn9hJR18gnuTRWb")
index = pc.Index("hacknitr-py")

def normalize_vector(vec, dim=1024):
    vec = vec.tolist()
    if len(vec) > dim:
        return vec[:dim]
    return vec + [0.0] * (dim - len(vec))

def on_change(key: pw.Pointer, row: dict, time: int, is_addition: bool):
    callsign = row.get("callsign")

    if not callsign:
        return 

    flight_id = str(callsign)
    
    if is_addition:
        vector = normalize_vector(row["velocities"])

        metadata = {
            "sentence": row["sentence"],
            "callsign": row["callsign"],
            "origin_country": row["origin_countries"],
            "max_altitude": float(row["altitudes"]) if row["altitudes"] else None,
            "latitude": float(row["latitudes"]) if row["latitudes"] else None,
            "longitude": float(row["longitudes"]) if row["longitudes"] else None,
            "session_start": str(row["session_start"]),
            "session_end": str(row["session_end"]),
        }

        index.upsert(
            vectors=[
                (
                    flight_id,
                    vector,
                    metadata
                )
            ]
        )

    else:
        index.delete(ids=[flight_id])


pw.io.subscribe(sentences, on_change)

# pw.debug.compute_and_print(result)
pw.run()