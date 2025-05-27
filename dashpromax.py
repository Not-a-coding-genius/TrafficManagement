import streamlit as st
import pandas as pd
import json
import numpy as np
import time
import random
import uuid
import threading
from datetime import datetime
from kafka import KafkaConsumer
import folium
from streamlit_folium import folium_static
import plotly.graph_objects as go
import plotly.express as px

# Kafka Configuration
KAFKA_TOPIC = "traffic-data"
KAFKA_SERVER = "10.5.6.240:9092"

# Weather icon mapping
WEATHER_ICONS = {
    "Clear": "☀️",
    "Rain": "🌧️",
    "Fog": "🌫️",
    "Snow": "❄️",
    "Cloudy": "☁️",
    "Unknown": "❓"
}

# Initialize additional session state variables for enhanced features
if 'street_coordinates' not in st.session_state:
    # Initialize street coordinates (same as original but with more streets)
    center_lat, center_lng = 34.0522, -118.2437  # Los Angeles
    st.session_state.street_coordinates = {
        "Main St": (center_lat + 0.01, center_lng - 0.01),
        "Highway 1": (center_lat - 0.015, center_lng),
        "Broadway": (center_lat, center_lng + 0.008),
        "5th Ave": (center_lat - 0.005, center_lng + 0.012),
        "Sunset Blvd": (center_lat + 0.008, center_lng + 0.015),
        "Elm St": (center_lat + 0.012, center_lng - 0.008),
        "Park Ave": (center_lat - 0.01, center_lng - 0.014)
    }

if 'metrics_history' not in st.session_state:
    st.session_state.metrics_history = {
        "time": [],
        "avg_speed": [],
        "avg_delay_typical": [],
        "avg_delay_freeflow": [],
        "avg_severity": [],
        "avg_visibility": [],
        "weather": []
    }

# New session state for load balancing
if 'load_balancing' not in st.session_state:
    st.session_state.load_balancing = {
        "enabled": False,
        "optimization_level": "medium",
        "last_optimization": None,
        "balanced_routes": {},
        "capacity_usage": {},
        "route_distribution": {}
    }

# Utility Functions (same as original)
def safe_float(value, default=0.0):
    """Converts a value to float safely, returning default if invalid."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def build_city_map(data_batch):
    """Dynamically builds a city map based on streets in data."""
    city_map = {}
    streets = list(set(row["Street"] for row in data_batch))
    
    for street in streets:
        # Simulated connections between streets
        city_map[street] = [s for s in streets if s != street]
    
    return city_map

def reroute_traffic(city_map, traffic_decision):
    """Suggests alternative routes for streets with congestion."""
    rerouting = {}
    
    for street, signal in traffic_decision.items():
        if "🔴 RED" in signal:  # Congested street
            alternative_routes = city_map.get(street, [])
            if alternative_routes:
                rerouting[street] = f"🚗 Reroute via: {', '.join(alternative_routes)}"
            else:
                rerouting[street] = "⚠️ No alternative routes available!"
    
    return rerouting

def analyze_traffic_data(data_batch):
    """Analyze traffic data and provide insights"""
    # Street coordinates (now using session state)
    street_coordinates = st.session_state.street_coordinates

    # Initialize analysis dictionary
    analysis = {
        "City Map": build_city_map(data_batch),
        "Streets": list(set(row["Street"] for row in data_batch)),
        "Street Coordinates": street_coordinates,
    }

    # Calculate aggregate metrics (same as original)
    metrics = {
        "Avg Speed": np.mean([safe_float(row["Congestion_Speed"]) for row in data_batch]),
        "Avg Delay (Typical)": np.mean([safe_float(row["DelayFromTypicalTraffic(mins)"]) for row in data_batch]),
        "Avg Delay (FreeFlow)": np.mean([safe_float(row.get("DelayFromFreeFlowSpeed(mins)", 0)) for row in data_batch]),
        "Avg Severity": np.mean([safe_float(row["Severity"]) for row in data_batch]),
        "Avg Visibility": np.mean([safe_float(row.get("Visibility(mi)", 0)) for row in data_batch]),
        "Timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    # Determine weather conditions
    weather_conditions = [row.get("Weather_Conditions", "Unknown") for row in data_batch]
    metrics["Weather"] = max(set(weather_conditions), key=weather_conditions.count) if weather_conditions else "Unknown"

    # Traffic decisions (same as original)
    traffic_decision = {}
    for street in analysis["Streets"]:
        street_data = [row for row in data_batch if row["Street"] == street]
        street_avg_speed = np.mean([safe_float(row["Congestion_Speed"]) for row in street_data])
        street_avg_delay = np.mean([safe_float(row["DelayFromTypicalTraffic(mins)"]) for row in street_data])

        if street_avg_speed < 15 or street_avg_delay > 20:
            traffic_decision[street] = "🔴 RED - Severe congestion, reroute traffic!"
        elif street_avg_speed < 25 or street_avg_delay > 10:
            traffic_decision[street] = "🟡 YELLOW - Moderate traffic, caution"
        else:
            traffic_decision[street] = "🟢 GREEN - Smooth traffic, proceed"

    # Add derived metrics to analysis
    analysis.update(metrics)
    analysis["Traffic Decisions"] = traffic_decision
    analysis["Rerouting Suggestions"] = reroute_traffic(analysis["City Map"], traffic_decision)

    # Update metrics history for visualizations
    st.session_state.metrics_history["time"].append(analysis["Timestamp"])
    st.session_state.metrics_history["avg_speed"].append(analysis["Avg Speed"])
    st.session_state.metrics_history["avg_delay_typical"].append(analysis["Avg Delay (Typical)"])
    st.session_state.metrics_history["avg_delay_freeflow"].append(analysis["Avg Delay (FreeFlow)"])
    st.session_state.metrics_history["avg_severity"].append(analysis["Avg Severity"])
    st.session_state.metrics_history["avg_visibility"].append(analysis["Avg Visibility"])
    st.session_state.metrics_history["weather"].append(analysis["Weather"])

    return analysis

# New load balancing functions
def calculate_street_capacity(street, data_batch):
    """Calculate the current capacity and usage of a street"""
    street_data = [row for row in data_batch if row["Street"] == street]
    avg_speed = np.mean([safe_float(row["Congestion_Speed"]) for row in street_data])
    max_capacity = 50  # Theoretical maximum in vehicles per minute
    
    # Higher speed means more capacity
    capacity_factor = min(avg_speed / 60, 1.0)  # Normalize to 0-1
    current_capacity = max_capacity * capacity_factor
    
    # Calculate usage based on congestion and delay
    avg_delay = np.mean([safe_float(row["DelayFromTypicalTraffic(mins)"]) for row in street_data])
    usage_factor = min(max(avg_delay, 0) / 30, 1.0)  # Normalize to 0-1
    current_usage = max_capacity * usage_factor
    
    return {
        "max_capacity": max_capacity,
        "current_capacity": current_capacity,
        "current_usage": current_usage,
        "usage_percentage": (current_usage / max_capacity) * 100,
        "remaining_capacity": max(0, current_capacity - current_usage)
    }

def optimize_traffic_distribution(data_batch, city_map, optimization_level="medium"):
    """Optimize traffic distribution using load balancing algorithms"""
    streets = list(set(row["Street"] for row in data_batch))
    
    # Calculate capacity and usage for each street
    street_capacities = {}
    for street in streets:
        street_capacities[street] = calculate_street_capacity(street, data_batch)
    
    # Identify congested streets (usage > 70% of capacity)
    congested_streets = [
        street for street, capacity in street_capacities.items() 
        if capacity["usage_percentage"] > 70
    ]
    
    # Identify under-utilized streets (usage < 40% of capacity)
    available_streets = [
        street for street, capacity in street_capacities.items() 
        if capacity["usage_percentage"] < 40
    ]
    
    # No rebalancing needed if no congestion or no available capacity
    if not congested_streets or not available_streets:
        return {
            "balanced_routes": {},
            "capacity_usage": street_capacities,
            "route_distribution": {},
            "optimization_summary": "No rebalancing needed or possible"
        }
    
    # Determine how aggressive to be with load balancing based on optimization level
    diversion_percentages = {
        "low": 0.15,      # Divert 15% of traffic
        "medium": 0.30,   # Divert 30% of traffic
        "high": 0.50      # Divert 50% of traffic
    }
    
    diversion_percent = diversion_percentages.get(optimization_level, 0.30)
    
    # Create optimized routing plan
    balanced_routes = {}
    route_distribution = {}
    
    # For each congested street, distribute traffic to available streets
    for street in congested_streets:
        # Find connected available streets
        connected_available = [s for s in available_streets if s in city_map.get(street, [])]
        
        if not connected_available:
            continue
            
        # Calculate traffic to divert
        traffic_to_divert = street_capacities[street]["current_usage"] * diversion_percent
        per_route_diversion = traffic_to_divert / len(connected_available)
        
        # Create distribution plan
        route_plan = {
            "divert_from": street,
            "divert_percentage": diversion_percent * 100,
            "divert_routes": {}
        }
        
        for alt_route in connected_available:
            # Calculate percentage of traffic to divert to this route
            divert_to_route = min(
                per_route_diversion,
                street_capacities[alt_route]["remaining_capacity"]
            )
            
            if divert_to_route > 0:
                percentage = (divert_to_route / traffic_to_divert) * 100
                route_plan["divert_routes"][alt_route] = percentage
                
                # Update capacity usage simulation
                street_capacities[street]["current_usage"] -= divert_to_route
                street_capacities[alt_route]["current_usage"] += divert_to_route
                
        # Add to balanced routes if we have routes to divert to
        if route_plan["divert_routes"]:
            balanced_routes[street] = route_plan
            
            # Format for display
            divert_text = []
            for route, percentage in route_plan["divert_routes"].items():
                divert_text.append(f"{route}: {percentage:.1f}%")
                
            route_distribution[street] = f"⚖️ Divert {diversion_percent*100:.0f}% traffic to: {', '.join(divert_text)}"
    
    # Recalculate usage percentages after rebalancing
    for street, capacity in street_capacities.items():
        capacity["usage_percentage"] = (capacity["current_usage"] / capacity["max_capacity"]) * 100
    
    # Return optimization results
    return {
        "balanced_routes": balanced_routes,
        "capacity_usage": street_capacities,
        "route_distribution": route_distribution,
        "optimization_summary": f"Optimized {len(balanced_routes)} congested routes"
    }

def create_interactive_map(street_coordinates, traffic_decisions, city_map, rerouting_suggestions, load_balancing_data=None):
    """Enhanced interactive map with connections and visual indicators"""
    # Calculate map center
    lats = [coord[0] for coord in street_coordinates.values()]
    lngs = [coord[1] for coord in street_coordinates.values()]
    center_lat = sum(lats) / len(lats)
    center_lng = sum(lngs) / len(lngs)
    
    # Create map
    m = folium.Map(location=[center_lat, center_lng], zoom_start=13, tiles="OpenStreetMap")
    
    # Add markers for each street with enhanced features
    for street, (lat, lng) in street_coordinates.items():
        # Determine marker properties based on traffic decision
        decision = traffic_decisions.get(street, "")
        if "🔴 RED" in decision:
            color = "red"
            icon = "times-circle"
            radius = 50
        elif "🟡 YELLOW" in decision:
            color = "orange"
            icon = "exclamation-circle"
            radius = 40
        else:
            color = "green"
            icon = "check-circle"
            radius = 30
        
        # Create enhanced popup with rerouting info
        popup_text = f"""
        <b>{street}</b><br>
        Status: {decision}<br>
        """
        
        # Add load balancing info if available
        if load_balancing_data and load_balancing_data.get("route_distribution", {}).get(street):
            popup_text += f"<b>Load Balancing:</b><br>{load_balancing_data['route_distribution'][street]}<br>"
        elif rerouting_suggestions.get(street):
            popup_text += f"{rerouting_suggestions.get(street, '')}<br>"
            
        # Add capacity info if available
        if load_balancing_data and load_balancing_data.get("capacity_usage", {}).get(street):
            capacity = load_balancing_data["capacity_usage"][street]
            popup_text += f"<br><b>Capacity:</b> {capacity['usage_percentage']:.1f}% used"
        
        # Add marker
        folium.Marker(
            location=[lat, lng],
            popup=folium.Popup(popup_text, max_width=300),
            tooltip=street,
            icon=folium.Icon(color=color, icon=icon, prefix='fa')
        ).add_to(m)
        
        # Add circle to visualize congestion level
        folium.Circle(
            location=[lat, lng],
            radius=radius,
            color=color,
            fill=True,
            fill_opacity=0.4
        ).add_to(m)
    
    # Add connection lines between streets
    for street, connections in city_map.items():
        if street in street_coordinates:
            start_coord = street_coordinates[street]
            
            for connected_street in connections:
                if connected_street in street_coordinates:
                    end_coord = street_coordinates[connected_street]
                    
                    # Determine line style based on traffic conditions
                    street_decision = traffic_decisions.get(street, "")
                    conn_decision = traffic_decisions.get(connected_street, "")
                    
                    # Check if this is a load-balanced route
                    is_balanced_route = False
                    arrow_color = None
                    if load_balancing_data and street in load_balancing_data.get("balanced_routes", {}):
                        if connected_street in load_balancing_data["balanced_routes"][street]["divert_routes"]:
                            is_balanced_route = True
                            arrow_color = "blue"
                    
                    if is_balanced_route:
                        line_color = "blue"
                        weight = 4
                        opacity = 0.9
                        dash_array = "10, 10"
                    elif "🔴 RED" in street_decision or "🔴 RED" in conn_decision:
                        line_color = "red"
                        weight = 3
                        opacity = 0.8
                        dash_array = None
                    elif "🟡 YELLOW" in street_decision or "🟡 YELLOW" in conn_decision:
                        line_color = "orange"
                        weight = 2.5
                        opacity = 0.7
                        dash_array = None
                    else:
                        line_color = "green"
                        weight = 2
                        opacity = 0.6
                        dash_array = None
                        
                    # Add the connection line
                    line = folium.PolyLine(
                        locations=[start_coord, end_coord],
                        color=line_color,
                        weight=weight,
                        opacity=opacity,
                        tooltip=f"{street} ↔ {connected_street}",
                        dash_array=dash_array
                    ).add_to(m)
                    
                    # Add arrow for load-balanced routes
                    if is_balanced_route:
                        # Add arrow icon to show direction
                        arrow_location = [
                            (start_coord[0] + end_coord[0]) / 2,
                            (start_coord[1] + end_coord[1]) / 2
                        ]
                        
                        folium.Marker(
                            location=arrow_location,
                            icon=folium.Icon(color="blue", icon="arrow-right", prefix='fa'),
                            tooltip=f"Load balanced: {street} → {connected_street}"
                        ).add_to(m)
    
    return m

def generate_sample_data():
    """Generate random traffic data for simulation"""
    streets = ["Main St", "Highway 1", "Broadway", "5th Ave", "Sunset Blvd", "Elm St", "Park Ave"]
    weather_conditions = ["Clear", "Rain", "Fog", "Snow", "Cloudy"]
    
    data_batch = []
    for street in streets:
        data_batch.append({
            "Street": street,
            "Congestion_Speed": round(random.uniform(10, 60), 2),
            "DelayFromTypicalTraffic(mins)": round(random.uniform(0, 30), 2),
            "DelayFromFreeFlowSpeed(mins)": round(random.uniform(0, 40), 2),
            "Severity": random.randint(1, 5),
            "Visibility(mi)": round(random.uniform(1, 10), 2),
            "Weather_Conditions": random.choice(weather_conditions),
            "StartTime": time.strftime("%Y-%m-%d %H:%M:%S")
        })
    return data_batch

# Streamlit App Configuration
st.set_page_config(page_title="Traffic Dashboard", page_icon="🚦", layout="wide")

# Initialize session state
if 'traffic_data' not in st.session_state:
    st.session_state.traffic_data = []

# Sidebar Configuration
st.sidebar.header("🚦 Traffic Dashboard Settings")

# Kafka Connection
st.sidebar.subheader("Kafka Connection")
kafka_server = st.sidebar.text_input("Kafka Server", KAFKA_SERVER)
kafka_topic = st.sidebar.text_input("Kafka Topic", KAFKA_TOPIC)

# Connection and Data Buttons
col1, col2 = st.sidebar.columns(2)
with col1:
    connect_button = st.button("Connect to Kafka")
with col2:
    sample_data_button = st.button("Generate Sample Data")

# Map Settings
st.sidebar.subheader("Map Settings")
if st.sidebar.button("Regenerate Street Layout"):
    center_lat, center_lng = 34.0522, -118.2437  # Los Angeles
    streets = ["Main St", "Highway 1", "Broadway", "5th Ave", "Sunset Blvd", "Elm St", "Park Ave"]
    
    new_coords = {}
    for street in streets:
        new_coords[street] = (
            center_lat + random.uniform(-0.02, 0.02),
            center_lng + random.uniform(-0.02, 0.02)
        )
        
    st.session_state.street_coordinates = new_coords
    st.sidebar.success("Street layout updated!")

# New Load Balancing Settings
st.sidebar.subheader("🔄 Load Balancing")
lb_enabled = st.sidebar.checkbox("Enable Load Balancing", value=st.session_state.load_balancing["enabled"])
st.session_state.load_balancing["enabled"] = lb_enabled

if lb_enabled:
    optimization_level = st.sidebar.select_slider(
        "Optimization Level",
        options=["low", "medium", "high"],
        value=st.session_state.load_balancing["optimization_level"]
    )
    st.session_state.load_balancing["optimization_level"] = optimization_level
    
    st.sidebar.info("""
    **Load Balancing Levels:**
    - **Low**: Minimal redistribution (15%)
    - **Medium**: Moderate redistribution (30%)
    - **High**: Aggressive redistribution (50%)
    """)
    
    if st.sidebar.button("Run Load Balancing Now"):
        if st.session_state.traffic_data:
            latest_analysis = analyze_traffic_data(st.session_state.traffic_data)
            load_balancing_data = optimize_traffic_distribution(
                st.session_state.traffic_data,
                latest_analysis["City Map"],
                optimization_level
            )
            st.session_state.load_balancing.update(load_balancing_data)
            st.session_state.load_balancing["last_optimization"] = time.strftime("%Y-%m-%d %H:%M:%S")
            st.sidebar.success("Traffic load balancing completed!")
        else:
            st.sidebar.warning("No traffic data available for load balancing.")

# Sidebar Footer
st.sidebar.markdown("---")
st.sidebar.markdown("🚦 Traffic Dashboard v3.0")
st.sidebar.markdown("Made with ❤️ by SMS")

# Main Dashboard
st.title("🚦 Real-Time Traffic Dashboard")

# Data Processing and Display Logic
if connect_button:
    try:
        # Kafka Consumer setup
        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_server,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        # Listen for messages
        for message in consumer:
            data_batch = message.value
            
            # Analyze and store data
            analysis = analyze_traffic_data(data_batch)
            
            # Run load balancing if enabled
            if st.session_state.load_balancing["enabled"]:
                load_balancing_data = optimize_traffic_distribution(
                    data_batch,
                    analysis["City Map"],
                    st.session_state.load_balancing["optimization_level"]
                )
                st.session_state.load_balancing.update(load_balancing_data)
                st.session_state.load_balancing["last_optimization"] = time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Update session state
            st.session_state.traffic_data = data_batch
            
            # Rerun to update UI
            st.rerun()
    
    except Exception as e:
        st.sidebar.error(f"Kafka Connection Error: {e}")

if sample_data_button:
    # Generate and process sample data
    data_batch = generate_sample_data()
    analysis = analyze_traffic_data(data_batch)
    
    # Run load balancing if enabled
    if st.session_state.load_balancing["enabled"]:
        load_balancing_data = optimize_traffic_distribution(
            data_batch,
            analysis["City Map"],
            st.session_state.load_balancing["optimization_level"]
        )
        st.session_state.load_balancing.update(load_balancing_data)
        st.session_state.load_balancing["last_optimization"] = time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Update session state
    st.session_state.traffic_data = data_batch

# Display Current Metrics with enhanced styling
if st.session_state.traffic_data:
    # Weather and Timestamp
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.write(f"📊 Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    with col2:
        latest_analysis = analyze_traffic_data(st.session_state.traffic_data)
        weather = latest_analysis['Weather']
        weather_icon = WEATHER_ICONS.get(weather, "❓")
        st.write(f"Weather: {weather_icon} {weather}")
    with col3:
        if st.session_state.load_balancing["enabled"]:
            lb_status = "🔄 Load Balancing: Active"
            if st.session_state.load_balancing["last_optimization"]:
                lb_status += f" (Last run: {st.session_state.load_balancing['last_optimization']})"
            st.write(lb_status)

    # Metrics Columns with enhanced styling
    st.markdown("""
    <style>
    .metric-box {
        background-color: #f6f6f6;
        border-radius: 10px;
        padding: 10px;
        text-align: center;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .metric-title {
        font-size: 16px;
        color: #555;
    }
    .metric-value {
        font-size: 24px;
        font-weight: bold;
        color: #1E88E5;
    }
    </style>
    """, unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_speed = latest_analysis['Avg Speed']
        speed_color = "#ff4b4b" if avg_speed < 20 else "#ff9800" if avg_speed < 30 else "#1E88E5"
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-title">Average Speed</div>
            <div class="metric-value" style="color: {speed_color}">{avg_speed:.1f} mph</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        avg_delay = latest_analysis['Avg Delay (Typical)']
        delay_color = "#ff4b4b" if avg_delay > 15 else "#ff9800" if avg_delay > 8 else "#1E88E5"
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-title">Average Delay</div>
            <div class="metric-value" style="color: {delay_color}">{avg_delay:.1f} mins</div>
        </div>
        """, unsafe_allow_html=True)
        
    with col3:
        severity = latest_analysis['Avg Severity']
        severity_color = "#ff4b4b" if severity > 3.5 else "#ff9800" if severity > 2.5 else "#1E88E5"
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-title">Traffic Severity</div>
            <div class="metric-value" style="color: {severity_color}">{severity:.1f}/5</div>
        </div>
        """, unsafe_allow_html=True)
        
    with col4:
        visibility = latest_analysis['Avg Visibility']
        vis_color = "#ff4b4b" if visibility < 2 else "#ff9800" if visibility < 5 else "#1E88E5"
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-title">Visibility</div>
            <div class="metric-value" style="color: {vis_color}">{visibility:.1f} mi</div>
        </div>
        """, unsafe_allow_html=True)

# Tabs for Detailed Views (enhanced version)
tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Traffic Map", "⚖️ Load Balancing", "📊 Analytics", "📋 Raw Data"])

with tab1:
    # Enhanced Interactive Map
    if st.session_state.traffic_data:
        st.subheader("Street Traffic Conditions")
        
        # Get latest analysis
        latest_analysis = analyze_traffic_data(st.session_state.traffic_data)
        
        # Get load balancing data if available
        load_balancing_data = None
        if st.session_state.load_balancing["enabled"] and st.session_state.load_balancing.get("balanced_routes"):
            load_balancing_data = {
                "balanced_routes": st.session_state.load_balancing["balanced_routes"],
                "capacity_usage": st.session_state.load_balancing["capacity_usage"],
                "route_distribution": st.session_state.load_balancing["route_distribution"]
            }
        
        # Create and display enhanced map
        map_col1, map_col2 = st.columns([3, 1])
        
        with map_col1:
            interactive_map = create_interactive_map(
                latest_analysis["Street Coordinates"], 
                latest_analysis["Traffic Decisions"],
                latest_analysis["City Map"],
                latest_analysis["Rerouting Suggestions"],
                load_balancing_data
            )
            folium_static(interactive_map, width=800, height=500)
        
        with map_col2:
            st.subheader("Traffic Decisions")
            for street, decision in latest_analysis["Traffic Decisions"].items():
                if "🔴 RED" in decision:
                    st.error(f"**{street}**: {decision}")
                elif "🟡 YELLOW" in decision:
                    st.warning(f"**{street}**: {decision}")
                else:
                    st.success(f"**{street}**: {decision}")
            
            if st.session_state.load_balancing["enabled"] and st.session_state.load_balancing.get("route_distribution"):
                st.subheader("Load Balancing")
                for street, suggestion in st.session_state.load_balancing["route_distribution"].items():
                    st.info(f"**{street}**: {suggestion}")
            else:
                st.subheader("Rerouting")
                for street, suggestion in latest_analysis["Rerouting Suggestions"].items():
                    st.info(f"**{street}**: {suggestion}")

with tab2:
    st.header("⚖️ Traffic Load Balancing")
    
    if not st.session_state.traffic_data:
        st.info("No traffic data available. Please generate sample data or connect to Kafka to enable load balancing features.")
    else:
        # Load balancing status and controls
        st.subheader("Load Balancing Status")
        
        # Status indicator
        status_col1, status_col2 = st.columns([1, 3])
        with status_col1:
            if st.session_state.load_balancing["enabled"]:
                st.markdown("🟢 **Status:** Active")
            else:
                st.markdown("🔴 **Status:** Inactive")
        
        with status_col2:
            if st.session_state.load_balancing["enabled"]:
                st.markdown(f"**Optimization Level:** {st.session_state.load_balancing['optimization_level'].capitalize()}")
                if st.session_state.load_balancing["last_optimization"]:
                    st.markdown(f"**Last Optimization:** {st.session_state.load_balancing['last_optimization']}")
        
        # Run load balancing on demand
        if not st.session_state.load_balancing["enabled"]:
            if st.button("Run One-time Load Balancing"):
                latest_analysis = analyze_traffic_data(st.session_state.traffic_data)
                load_balancing_data = optimize_traffic_distribution(
                    st.session_state.traffic_data,
                    latest_analysis["City Map"],
                    "medium"  # Default to medium for one-time runs
                )
                st.session_state.load_balancing.update(load_balancing_data)
                st.session_state.load_balancing["last_optimization"] = time.strftime("%Y-%m-%d %H:%M:%S")
                st.success("One-time load balancing completed!")
                st.rerun()
        
        # Show load balancing results if available
        if st.session_state.load_balancing.get("capacity_usage") and st.session_state.load_balancing.get("balanced_routes"):
            # Visualization of load balancing results
            st.subheader("Capacity Distribution Visualization")
            
            # Prepare data for visualization
            streets = list(st.session_state.load_balancing["capacity_usage"].keys())
            usage_data = []
            
            for street in streets:
                capacity_info = st.session_state.load_balancing["capacity_usage"][street]
                usage_data.append({
                    "Street": street,
                    "Used Capacity": capacity_info["usage_percentage"],
                    "Remaining Capacity": 100 - capacity_info["usage_percentage"]
                })
            
            # Create capacity visualization using plotly
            fig = go.Figure()
            
            for street_data in usage_data:
                fig.add_trace(go.Bar(
                    name="Used",
                    x=[street_data["Street"]],
                    y=[street_data["Used Capacity"]],
                    marker_color='coral',
                    hoverinfo="text",
                    hovertext=f"Used: {street_data['Used Capacity']:.1f}%"
                ))
                
                fig.add_trace(go.Bar(
                    name="Available",
                    x=[street_data["Street"]],
                    y=[street_data["Remaining Capacity"]],
                    marker_color='lightgreen',
                    hoverinfo="text",
                    hovertext=f"Available: {street_data['Remaining Capacity']:.1f}%"
                ))
            
            fig.update_layout(
                barmode='stack',
                title="Street Capacity Utilization",
                xaxis_title="Street",
                yaxis_title="Capacity (%)",
                legend_title="Capacity",
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Display optimized routes
            st.subheader("Optimized Traffic Routes")
            
            if len(st.session_state.load_balancing["balanced_routes"]) > 0:
                for street, plan in st.session_state.load_balancing["balanced_routes"].items():
                    with st.expander(f"🔄 {street} (Divert {plan['divert_percentage']:.1f}% traffic)"):
                        st.write("**Diversion plan:**")
                        
                        # Create a small bar chart for route distribution
                        routes = list(plan["divert_routes"].keys())
                        percentages = list(plan["divert_routes"].values())
                        
                        if routes:
                            route_fig = px.bar(
                                x=routes,
                                y=percentages,
                                labels={'x': 'Alternative Route', 'y': 'Traffic Percentage'},
                                title=f"Traffic Distribution from {street}",
                                color=percentages,
                                color_continuous_scale="Blues"
                            )
                            route_fig.update_layout(height=250)
                            st.plotly_chart(route_fig, use_container_width=True)
                        else:
                            st.write("No viable alternative routes available.")
            else:
                st.info("No routes require optimization at this time.")
            
            # Optimization summary
            st.subheader("Optimization Summary")
            col1, col2 = st.columns(2)
            
            with col1:
                # Count stats
                optimized_count = len(st.session_state.load_balancing["balanced_routes"])
                total_streets = len(st.session_state.load_balancing["capacity_usage"])
                
                st.metric(
                    label="Streets Optimized",
                    value=f"{optimized_count}/{total_streets}",
                    delta=f"{(optimized_count/total_streets)*100:.1f}%" if total_streets > 0 else "0%"
                )
            
            with col2:
                # Get average congestion before and after
                before_congestion = 0
                after_congestion = 0
                
                if optimized_count > 0:
                    for street in st.session_state.load_balancing["balanced_routes"].keys():
                        street_data = [row for row in st.session_state.traffic_data if row["Street"] == street]
                        before_congestion += np.mean([safe_float(row["DelayFromTypicalTraffic(mins)"]) for row in street_data])
                    
                    # Estimate after congestion (simplified model)
                    after_congestion = before_congestion * (1 - (float(st.session_state.load_balancing["optimization_level"] == "high") * 0.5 + 
                                                            float(st.session_state.load_balancing["optimization_level"] == "medium") * 0.3 +
                                                            float(st.session_state.load_balancing["optimization_level"] == "low") * 0.15))
                    
                    st.metric(
                        label="Avg. Congestion Reduction",
                        value=f"{(before_congestion - after_congestion)/optimized_count:.1f} mins",
                        delta=f"-{((before_congestion - after_congestion)/before_congestion)*100:.1f}%" if before_congestion > 0 else "0%",
                        delta_color="inverse"
                    )
            
            # Recommendations based on current load balancing
            st.subheader("System Recommendations")
            
            # Generate recommendations based on current stats
            if optimized_count == 0:
                st.success("✅ Traffic is currently well balanced. No interventions needed.")
            else:
                recs = []
                if optimized_count / total_streets > 0.5:
                    recs.append("⚠️ High traffic congestion detected across multiple streets. Consider public notifications.")
                
                if st.session_state.load_balancing["optimization_level"] == "high":
                    recs.append("📢 Aggressive load balancing in effect. Monitor for potential congestion points in alternative routes.")
                
                # Find the most congested street after optimization
                most_congested = max(st.session_state.load_balancing["capacity_usage"].items(), 
                                   key=lambda x: x[1]["usage_percentage"])
                                   
                if most_congested[1]["usage_percentage"] > 80:
                    recs.append(f"🚨 Critical congestion remains on {most_congested[0]} ({most_congested[1]['usage_percentage']:.1f}% capacity). Consider traffic control intervention.")
                
                if not recs:
                    recs.append("✅ Load balancing is operating effectively.")
                
                for rec in recs:
                    st.markdown(rec)
                    
            # Historical optimization tracking
            if st.checkbox("Show Optimization History"):
                # Implement this in future version - would store history of optimizations
                st.info("Optimization history tracking will be available in a future update.")
        else:
            st.info("Load balancing data not available. Enable load balancing or run a one-time optimization to see results.")
with tab3:
    st.header("📊 Traffic Analytics")
    
    if st.session_state.metrics_history["time"]:
        # Time series analysis
        st.subheader("Traffic Metrics Over Time")
        
        # Metric selection
        metrics_options = {
            "avg_speed": "Average Speed (mph)",
            "avg_delay_typical": "Average Delay (Typical) (mins)",
            "avg_delay_freeflow": "Average Delay (FreeFlow) (mins)",
            "avg_severity": "Average Severity (1-5)",
            "avg_visibility": "Average Visibility (mi)"
        }
        
        selected_metrics = st.multiselect(
            "Select metrics to display:",
            options=list(metrics_options.keys()),
            default=["avg_speed", "avg_delay_typical"],
            format_func=lambda x: metrics_options[x]
        )
        
        if selected_metrics:
            # Create time series chart
            fig = go.Figure()
            
            for metric in selected_metrics:
                fig.add_trace(go.Scatter(
                    x=st.session_state.metrics_history["time"][-20:],  # Last 20 data points
                    y=st.session_state.metrics_history[metric][-20:],
                    mode='lines+markers',
                    name=metrics_options[metric]
                ))
            
            fig.update_layout(
                title="Traffic Metrics Trend",
                xaxis_title="Time",
                yaxis_title="Value",
                height=400,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        # Weather distribution
        st.subheader("Weather Conditions Distribution")
        
        # Count occurrences of each weather condition
        weather_counts = {}
        for weather in st.session_state.metrics_history["weather"]:
            if weather in weather_counts:
                weather_counts[weather] += 1
            else:
                weather_counts[weather] = 1
        
        # Create pie chart
        weather_fig = px.pie(
            values=list(weather_counts.values()),
            names=list(weather_counts.keys()),
            title="Weather Conditions Distribution",
            color_discrete_sequence=px.colors.qualitative.Pastel,
            hover_data=[list(weather_counts.values())],
            labels={'label': 'Weather', 'value': 'Count'}
        )
        
        weather_fig.update_traces(textposition='inside', textinfo='percent+label')
        weather_fig.update_layout(height=350)
        
        st.plotly_chart(weather_fig, use_container_width=True)
        
        # Traffic condition distribution over time
        if len(st.session_state.metrics_history["time"]) > 1:
            st.subheader("Traffic Conditions Correlation")
            
            correlation_options = [
                ("avg_speed", "avg_delay_typical", "Speed vs. Delay (Typical)"),
                ("avg_speed", "avg_delay_freeflow", "Speed vs. Delay (FreeFlow)"),
                ("avg_visibility", "avg_speed", "Visibility vs. Speed"),
                ("avg_visibility", "avg_severity", "Visibility vs. Severity")
            ]
            
            selected_correlation = st.selectbox(
                "Select correlation to analyze:",
                options=range(len(correlation_options)),
                format_func=lambda i: correlation_options[i][2]
            )
            
            x_metric, y_metric, title = correlation_options[selected_correlation]
            
            # Create scatter plot
            corr_fig = px.scatter(
                x=st.session_state.metrics_history[x_metric],
                y=st.session_state.metrics_history[y_metric],
                title=title,
                labels={'x': metrics_options.get(x_metric, x_metric), 'y': metrics_options.get(y_metric, y_metric)},
                trendline="ols"
            )
            
            corr_fig.update_layout(height=400)
            st.plotly_chart(corr_fig, use_container_width=True)
            
            # Add correlation coefficient
            x_data = np.array(st.session_state.metrics_history[x_metric])
            y_data = np.array(st.session_state.metrics_history[y_metric])
            correlation = np.corrcoef(x_data, y_data)[0, 1]
            
            st.info(f"Correlation coefficient: {correlation:.3f}")
            
            if abs(correlation) > 0.7:
                st.write("Strong correlation detected.")
            elif abs(correlation) > 0.4:
                st.write("Moderate correlation detected.")
            else:
                st.write("Weak or no significant correlation.")
    else:
        st.info("No historical data available yet. Generate sample data or connect to Kafka to see analytics.")

with tab4:
    st.header("📋 Raw Traffic Data")
    
    if st.session_state.traffic_data:
        # Convert to DataFrame for better display
        df = pd.DataFrame(st.session_state.traffic_data)
        
        # Column selector
        if not df.empty:
            all_columns = list(df.columns)
            selected_columns = st.multiselect(
                "Select columns to display:",
                options=all_columns,
                default=["Street", "Congestion_Speed", "DelayFromTypicalTraffic(mins)", "Severity", "Weather_Conditions"]
            )
            
            if selected_columns:
                st.dataframe(df[selected_columns], use_container_width=True)
            else:
                st.dataframe(df, use_container_width=True)
            
            # Export options
            export_format = st.radio("Export format:", ["CSV", "JSON"], horizontal=True)
            
            if st.button("Export Data"):
                if export_format == "CSV":
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"traffic_data_{time.strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                else:
                    json_data = df.to_json(orient="records")
                    st.download_button(
                        label="Download JSON",
                        data=json_data,
                        file_name=f"traffic_data_{time.strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )
            
            # Basic data statistics
            if st.checkbox("Show Data Statistics"):
                st.subheader("Data Statistics")
                
                numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
                if numeric_cols:
                    st.dataframe(df[numeric_cols].describe(), use_container_width=True)
    else:
        st.info("No traffic data available. Generate sample data or connect to Kafka to see raw data.")
