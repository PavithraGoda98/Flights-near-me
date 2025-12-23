import streamlit as st
import pandas as pd
import redis, psycopg2, requests, time
from threading import Thread
import pydeck as pdk

# 1. Cloud Connections (Setup these in Streamlit Secrets later)
# Use st.secrets for safety when going live
REDIS_URL = st.secrets["REDIS_URL"]
PG_CONN = st.secrets["POSTGRES_URL"]

r = redis.from_url(REDIS_URL, decode_responses=True)

# 2. The On-Demand Background Producer
def fetch_and_store():
    INDIA_BOUNDS = {'lamin': 6.55, 'lomin': 68.11, 'lamax': 35.67, 'lomax': 97.40}
    while st.session_state.get('run_radar', True):
        try:
            resp = requests.get("https://opensky-network.org/api/states/all", params=INDIA_BOUNDS, timeout=10)
            if resp.status_code == 200:
                states = resp.json().get('states', [])
                conn = psycopg2.connect(PG_CONN)
                cur = conn.cursor()
                for s in states:
                    # icao24, callsign, lon, lat, alt, vel
                    data = (s[0], s[1].strip() if s[1] else "NONE", s[5], s[6], s[7], s[9])
                    if data[2] and data[3]:
                        # Save to Postgres
                        cur.execute("INSERT INTO live_flights (icao24, callsign, longitude, latitude, altitude, velocity) VALUES (%s, %s, %s, %s, %s, %s)", data)
                        # Save to Redis for Live Radar
                        r.geoadd("india_flights", (data[2], data[3], data[0]))
                        r.setex(f"flight:{data[0]}", 120, data[1]) # Map ICAO to Callsign
                conn.commit()
                cur.close()
                conn.close()
            time.sleep(15)
        except Exception as e:
            print(f"Producer Error: {e}")
            break

# 3. Main Streamlit UI
st.title("✈️ India Live Radar")

if 'radar_active' not in st.session_state:
    st.session_state.radar_active = True
    st.session_state.run_radar = True
    Thread(target=fetch_and_store, daemon=True).start()

# Fetch latest data for mapping
conn = psycopg2.connect(PG_CONN)
df = pd.read_sql("SELECT DISTINCT ON (icao24) * FROM live_flights WHERE processed_at > NOW() - INTERVAL '5 minutes' ORDER BY icao24, processed_at DESC", conn)
conn.close()

if not df.empty:
    # --- PYDECK MAP WITH FLIGHT NUMBERS ---
    view_state = pdk.ViewState(latitude=20.59, longitude=78.96, zoom=4)
    
    layers = [
        # Red dots
        pdk.Layer("ScatterplotLayer", df, get_position='[longitude, latitude]', get_color='[200, 30, 0, 160]', get_radius=20000),
        # TEXT LABELS (Flight Numbers)
        pdk.Layer(
            "TextLayer",
            df,
            get_position='[longitude, latitude]',
            get_text="callsign",
            get_color="[0, 0, 0]",
            get_size=16,
            get_alignment_baseline="'bottom'",
            get_pixel_offset=[0, -15]
        )
    ]
    st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view_state))
    st.dataframe(df[['callsign', 'altitude', 'velocity']])
else:
    st.info("Engine Waking Up... Data arriving in 15s.")