import streamlit as st
import requests

# FastAPI URL inside container
FASTAPI_URL = "http://fastapi-app:8000"

st.title("🚲 CitiBike Forecast")
st.write("Predict bike availability for a station at a given hour.")

# ---- Fetch station list from FastAPI ----
try:
    stations_resp = requests.get(f"{FASTAPI_URL}/stations")
    stations_resp.raise_for_status()
    stations = stations_resp.json().get("station_names", [])
except Exception as e:
    st.error(f"Failed to fetch stations: {e}")
    stations = []

# ---- User inputs ----
col1, col2 = st.columns(2)

with col1:
    hour = st.number_input("Hour (0-23)", min_value=0, max_value=23, value=10)

with col2:
    action = st.radio(
        "What do you want to do?",
        options=["🚲 Take a bike", "🅿️ Drop a bike"],
        horizontal=True,
    )

if stations:
    station = st.selectbox("Select Station", stations)
else:
    station = st.text_input("Station short ID", value="6233.04")

# ---- Predict button ----
if st.button("Predict", use_container_width=True):
    payload = {"hour_selected": hour, "station_selected": station}
    try:
        response = requests.post(f"{FASTAPI_URL}/forecast", json=payload)
        if response.status_code == 200:
            data  = response.json()
            pred  = data.get("predicted_net_flow", 0)
            bikes = data.get("num_bikes_available", "?")
            docks = data.get("num_docks_available", "?")

            st.success("✅ Prediction fetched!")

            # ---- Interpret result based on user intent ----
            st.markdown("### 📊 Result")

            m1, m2, m3 = st.columns(3)
            m1.metric("Predicted net flow", f"{pred:+.1f}", help="Positive = more bikes coming in, negative = more leaving")
            m2.metric("🚲 Bikes available now", bikes)
            m3.metric("🅿️ Docks available now", docks)

            st.markdown("---")

            if action == "🚲 Take a bike":
                if bikes == "?" or int(bikes) == 0:
                    st.warning("⚠️ No bikes available right now at this station.")
                elif pred < 0:
                    st.info(f"📉 Bikes are expected to **leave** this station around hour {hour}. "
                            f"Currently **{bikes} bike(s)** available — go soon!")
                else:
                    st.success(f"✅ Bikes are expected to **arrive** at this station around hour {hour}. "
                               f"Currently **{bikes} bike(s)** available.")

            else:  # Drop a bike
                if docks == "?" or int(docks) == 0:
                    st.warning("⚠️ No docks available right now at this station.")
                elif pred > 0:
                    st.info(f"📈 More bikes are expected to **arrive** at this station around hour {hour}. "
                            f"Currently **{docks} dock(s)** available — drop soon before it fills up!")
                else:
                    st.success(f"✅ Bikes are expected to **leave** this station around hour {hour}. "
                               f"Currently **{docks} dock(s)** available — good time to drop!")

            with st.expander("📋 Full API response"):
                st.json(data)

        else:
            st.error(f"Error {response.status_code}")
            st.text(response.text)

    except Exception as e:
        st.error(f"Failed to call API: {e}")
