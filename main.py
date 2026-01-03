import time
import requests
import pathway as pw

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
    velocity: float | None
    true_track: float | None
    vertical_rate: float | None
    geo_altitude: float | None
    squawk: str | None
    spi: bool
    position_source: int

class FlightStreamSubject(pw.io.python.ConnectorSubject):
    def run(self):
        url = "https://hacknitr-api.vercel.app/api/states/all"

        while True:
            try:
                resp = requests.get(url)
                data = resp.json()

                states = data.get("states", [])
                now = data.get("time")

                for s in states:
                    record = {
                        "icao24": s[0],
                        "callsign": s[1].strip() if s[1] else None,
                        "origin_country": s[2],
                        "time_position": s[3],
                        "last_contact": s[4],
                        "longitude": s[5],
                        "latitude": s[6],
                        "baro_altitude": s[7],
                        "on_ground": s[8],
                        "velocity": s[9],
                        "true_track": s[10],
                        "vertical_rate": s[11],
                        "geo_altitude": s[13],
                        "squawk": s[14],
                        "spi": s[15],
                        "position_source": s[16],
                    }

                    self.next(**record)

            except Exception as e:
                print(e)

            time.sleep(5)

# input connector
flights = pw.io.python.read(
    FlightStreamSubject(),
    schema=OpenSkySchema,
)

# output connector
pw.io.csv.write(flights, "output.csv")
print(flights)

pw.run()
