import time
import requests
import pathway as pw
from datetime import timedelta
from pinecone import Pinecone, ServerlessSpec
import os
from dotenv import load_dotenv

load_dotenv()

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
    geo_altitude: float
    squawk: str | None
    spi: bool | None
    position_source: int | None
    category: int | None

# INPUT CONNECTOR
class FlightStreamSubject(pw.io.python.ConnectorSubject):
    def run(self):
        url = 'https://opensky-network.org/api/states/all?lamin=2.5&lomin=63.5&lamax=38.5&lomax=99.5'
        while True:
            try:
                res = requests.get(url)
                res.raise_for_status()

                data = res.json()

                states = data.get('states', [])

                print(f"Fetched {len(states)} states")

                for s in states:
                    def get(idx):
                        return s[idx] if len(s) > idx else None

                    if s[5] is None or s[6] is None or s[13] is None or s[9] is None:
                        continue

                    record = {
                        'icao24': get(0),
                        'callsign': get(1).strip() if get(1) else None,
                        'origin_country': get(2),
                        'time_position': get(3),
                        'last_contact': get(4),
                        'longitude': get(5),
                        'latitude': get(6),
                        'baro_altitude': get(7),
                        'on_ground': get(8),
                        'velocity': get(9),
                        'true_track': get(10),
                        'vertical_rate': get(11),
                        'geo_altitude': get(13),
                        'squawk': get(14),
                        'spi': get(15),
                        'position_source': get(16),
                        'category': get(17),
                    }

                    self.next(**record)

            except Exception as e:
                print(e)

            time.sleep(10)

tables = pw.io.python.read(
    FlightStreamSubject(),
    schema=OpenSkySchema,
)

# PROCESSING
timed_tables = tables.with_columns(
    time=pw.this.last_contact.dt.from_timestamp("s")
)

filtered_tables = timed_tables.filter(
    (pw.this.velocity.is_not_none()) &
    (pw.this.geo_altitude.is_not_none()) &
    (pw.this.callsign.is_not_none())
)

windowed = filtered_tables.windowby(
    filtered_tables.time,
    window=pw.temporal.session(max_gap=timedelta(minutes=1)),
    instance=filtered_tables.callsign,
).reduce(
    pw.this.callsign,
    window_end=pw.this._pw_window_end,

    avg_velocity=pw.reducers.avg(pw.this.velocity),
    avg_altitude=pw.reducers.avg(pw.this.geo_altitude),
    earliest_velocity=pw.reducers.earliest(pw.this.velocity),
    earliest_altitude=pw.reducers.earliest(pw.this.geo_altitude),
    latitude=pw.reducers.avg(pw.this.latitude),
    longitude=pw.reducers.avg(pw.this.longitude),
)

def trend_sentence(v, pv, a, pa):
    if pv is None or pa is None:
        return "Initial flight data collected"

    parts = []

    if v > pv:
        parts.append("velocity increased")
    elif v < pv:
        parts.append("velocity decreased")
    else:
        parts.append("velocity stable")

    if a > pa:
        parts.append("altitude increased")
    elif a < pa:
        parts.append("altitude decreased")
    else:
        parts.append("altitude stable")

    return ", ".join(parts)

final_tables = windowed.select(
    pw.this.callsign,
    pw.this.window_end,
    pw.this.latitude,
    pw.this.longitude,

    sentence=pw.apply(
        trend_sentence,
        pw.this.avg_velocity,
        pw.this.earliest_velocity,
        pw.this.avg_altitude,
        pw.this.earliest_altitude,
    )
)

# pinecone
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
if not PINECONE_API_KEY:
    raise ValueError("PINECONE_API_KEY not found in .env")

# init pinecone
pc = Pinecone(api_key=PINECONE_API_KEY)

pc.create_index(
    name="hacknitr-flights",
    dimension=8,
    metric="cosine",
    spec=ServerlessSpec(
        cloud="aws",
        region="us-east-1"
    )
)
index = pc.Index("hacknitr-flights")

# OUTPUT CONNECTOR
DUMMY_VECTOR = [1.0] + [0.0] * 7

def on_change(key: pw.Pointer, row: dict, time: int, is_addition: bool):
    if not is_addition:
        return

    flight_id = row["callsign"]

    index.upsert(
        vectors=[
            (
                flight_id,
                DUMMY_VECTOR,
                {
                    "timestamp": str(row["window_end"]),
                    "sentence": row["sentence"],
                    "latitude": str(row["latitude"]),
                    "longitude": str(row["longitude"]),
                }
            )
        ]
    )

pw.io.subscribe(final_tables, on_change)

pw.run()