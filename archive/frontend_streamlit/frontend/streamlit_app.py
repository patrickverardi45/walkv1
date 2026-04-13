import streamlit as st
import requests

BACKEND_URL = "http://127.0.0.1:8000"

st.set_page_config(layout="wide")

st.title("OSP Fiber Redlining / QA / Billing Demo")

# ---------------------------
# BACKEND STATUS
# ---------------------------
st.sidebar.header("Backend")

try:
    r = requests.get(f"{BACKEND_URL}/")
    st.sidebar.success("Backend connected")
except:
    st.sidebar.error("Backend status: not reachable")

# ---------------------------
# STEP 1 — UPLOAD FILES
# ---------------------------
st.header("1) Upload Files")

col1, col2 = st.columns(2)

# LEFT = KMZ
with col1:
    st.subheader("Upload KMZ / KML Route")
    kmz_file = st.file_uploader("KMZ Route", type=["kmz", "kml"])

    if st.button("Upload KMZ Route"):
        if kmz_file:
            files = {"file": kmz_file}
            res = requests.post(f"{BACKEND_URL}/api/upload-kmz", files=files)

            if res.status_code == 200:
                st.success("KMZ uploaded successfully")
            else:
                st.error(res.text)

# RIGHT = DESIGN FILE (THIS IS WHAT YOU WANTED BACK)
with col2:
    st.subheader("Upload Design (PDF / CSV / XLSX)")
    design_file = st.file_uploader("Design File", type=["pdf", "csv", "xlsx"])

    if st.button("Upload Design File"):
        if design_file:
            files = {"file": design_file}
            res = requests.post(f"{BACKEND_URL}/api/upload-fieldwire", files=files)

            if res.status_code == 200:
                st.success("Design uploaded successfully")
            else:
                st.error(res.text)

# ---------------------------
# STEP 2 — MAP
# ---------------------------
st.header("2) Route + Design Map")

try:
    map_data = requests.get(f"{BACKEND_URL}/api/route-data").json()

    st.write("Route loaded:", map_data.get("route_loaded", False))

except:
    st.warning("No route loaded yet.")

# ---------------------------
# STEP 3 — QA
# ---------------------------
st.header("3) QA Summary")

if st.button("Generate QA Summary"):
    res = requests.post(f"{BACKEND_URL}/api/generate-redlines")

    if res.status_code == 200:
        st.success("QA generated")
        st.json(res.json())
    else:
        st.error(res.text)