import streamlit as st
import pandas as pd
import redis, psycopg2, requests, time
from threading import Thread
import pydeck as pdk

# 1. Secure Connections from Secrets
REDIS_URL = st.secrets["REDIS_URL"]
PG_CONN = st.secrets["POSTGRES_URL"]

# Initialize Redis once outside the loop
r = redis.from_url(REDIS_URL, decode_responses=True)

# 2. Optimized Background Producer
def fetch_and_store():
    # Slightly narrowed bounds to stay strictly inside India
    INDIA_BOUNDS = {'lamin': 8.4, 'lomin': 68.7, 'lamax': 33.1, 'lomax': 97.25}
    
    while st.session_state.get('run_radar', True):
        try:
            resp = requests.get("https://opensky-network.org/api/states/all", params=INDIA_BOUNDS, timeout=10)
            if resp.status_code == 200:
                states = resp.json().get('states', [])
                
                # Connection context manager for efficiency
                with psycopg2.connect(PG_CONN) as conn:
                    with conn.cursor() as cur:
                        for s in states:
                            # Filter for valid GPS data
                            if s[5] and s[6]:
                                data = (s[0], s[1].strip() if s[1] else "NONE", s[5], s[6], s[7] or 0, s[9] or 0)
                                
                                # 1. Update Postgres (History)
                                cur.execute("""INSERT INTO live_flights 
                                    (icao24, callsign, longitude, latitude, altitude, velocity) 
                                    VALUES (%s, %s, %s, %s, %s, %s)""", data)
                                
                                # 2. Update Redis (Real-time Geo-index)
                                r.geoadd("india_flights", (data[2], data[3], data[0]))
                                r.expire("india_flights", 120) 
                
            # INCREASE SLEEP: 30s instead of 15s to respect free-tier limits
            time.sleep(30)
        except Exception as e:
            print(f"Engine Error: {e}")
            time.sleep(60) # Back off if error occurs

# 3. Streamlit UI Logic
st.set_page_config(page_title="India Live Radar", layout="wide")
st.title("✈️ India Live Flight Radar")

# Thread safety: ensure only one producer runs
if 'radar_active' not in st.session_state:
    st.session_state.radar_active = True
    st.session_state.run_radar = True
    Thread(target=fetch_and_store, daemon=True).start()

# Data fetching for UI
try:
    with psycopg2.connect(PG_CONN) as conn:
        # Get only the latest position for each plane to reduce data load
        query = """SELECT DISTINCT ON (icao24) * FROM live_flights 
                   WHERE processed_at > NOW() - INTERVAL '5 minutes' 
                   ORDER BY icao24, processed_at DESC"""
        df = pd.read_sql(query, conn)
except:
    df = pd.DataFrame()

if not df.empty:
    # --- PYDECK MAP ---
    view_state = pdk.ViewState(latitude=20.59, longitude=78.96, zoom=4, pitch=0)
    
    layers = [
        # Red dots
        pdk.Layer("ScatterplotLayer", df, get_position='[longitude, latitude]', 
                  get_color='[200, 30, 0, 160]', get_radius=20000),
        # Interactive Text Labels
        pdk.Layer("TextLayer", df, get_position='[longitude, latitude]',
                  get_text="callsign", get_color="[0, 0, 0]", get_size=18,
                  get_alignment_baseline="'bottom'", get_pixel_offset=[0, -15])
    ]
    st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view_state, tooltip={"text": "{callsign}"}))
    st.dataframe(df[['callsign', 'altitude', 'velocity', 'processed_at']], use_container_width=True)
else:
    st.info("Radar warming up... Please wait 30 seconds for live data.")
