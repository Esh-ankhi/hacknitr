from fastapi import FastAPI, HTTPException
from pinecone import Pinecone
import google.generativeai as genai

genai.configure(
    api_key="AIzaSyABMFYXHAvyp9b5kySr_UeaCQcL9QSl8uw"
)

gemini_model = genai.GenerativeModel("gemini-2.5-flash")

pc = Pinecone(api_key="pcsk_51k6TP_RVkPcs4D5K8i8Gwj5BL6ZovfQSckBmcfNjLkZbE51D514vHEJn9hJR18gnuTRWb")
index = pc.Index("hacknitr-py")   

VECTOR_DIM = 10  

app = FastAPI(
    title="Flight Data API",
    description="Fetch flight data from Pinecone by callsign",
    version="1.0.0"
)

@app.get("/")
def root():
    return {"status": "ok", "message": "Flight API is running"}


@app.get("/flight/{callsign}")
def get_flight_by_callsign(callsign: str):
    response = index.fetch(ids=[callsign])

    if callsign not in response.vectors:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for callsign {callsign}"
        )

    record = response.vectors[callsign]

    return {
        "id": callsign,
        "vector": record.values,
        "metadata": record.metadata
    }

@app.get("/flight/search/{callsign}")
def search_flight_by_metadata(callsign: str):
    response = index.query(
        vector=[0.0] * VECTOR_DIM,   
        filter={"callsign": {"$eq": callsign}},
        top_k=10,
        include_metadata=True,
        include_values=True
    )

    if not response.matches:
        raise HTTPException(
            status_code=404,
            detail=f"No records found for callsign {callsign}"
        )

    return {
        "results": [
            {
                "id": m.id,
                "score": m.score,
                "vector": m.values,
                "metadata": m.metadata
            }
            for m in response.matches
        ]
    }

def build_flight_prompt(callsign: str, metadata: dict) -> str:
    return f"""
    You are an aviation intelligence assistant.

    Flight callsign: {callsign}

    Flight information:
    {metadata}

    Instructions:
    - Respond ONLY in short bullet points.
    - Be concise and factual.
    - Do NOT include greetings, introductions, or sign-offs.
    - Do NOT mention data sources, analysis steps, or limitations.
    - Do NOT say "As an AI language model" or anything similar.

    Tasks:
    - State whether the flight shows any delay risk.
    - If yes, briefly list possible reasons (weather, speed, altitude, route).
    - Mention unusual or anomalous behavior only if clearly present.

    If there are no clear issues or anomalies, respond with EXACTLY this sentence and nothing else:
    "Flight is on its normal course."
    """


def analyze_with_gemini(prompt: str) -> str:
    response = gemini_model.generate_content(prompt)
    return response.text

@app.get("/flight/analyze/{callsign}")
def analyze_flight(callsign: str):
    response = index.fetch(ids=[callsign])

    if callsign not in response.vectors:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for callsign {callsign}"
        )

    record = response.vectors[callsign]
    metadata = record.metadata or {}

    prompt = build_flight_prompt(callsign, metadata)
    ai_analysis = analyze_with_gemini(prompt)

    return {
        "callsign": callsign,
        "metadata": metadata,
        "ai_analysis": ai_analysis
    }
