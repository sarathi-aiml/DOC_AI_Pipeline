import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import altair as alt
import pypdfium2 as pdfium
import os
import time
import plotly.graph_objects as go
from time import sleep
import plotly.express as px
from datetime import datetime, timedelta

import plotly.express as px
from datetime import datetime

# Set up the Snowflake session
session = get_active_session()

# Database and schema names
db_name = "DS_DEV_DB"
schema_name = "DOC_AI_SCHEMA"

tables = {
    "prefilter": "DOCAI_PREFILTER",
    "extraction": "DOCAI_ORDERFORM_EXTRACTION",
    "threshold": "SCORE_THRESHOLD",
    "invoice": {
        "flattened": "Invoice_Flatten",
        "validated": "Invoice_Validated",
        "score_failed": "invoice_col_score_failed_history",
    },
    "purchase": {
        "flattened": "Purchase_Flatten",
        "validated": "Purchase_Validated",
        "score_failed": "purchase_col_score_failed_history",
    },
}



# Create a temporary directory for PDFs if it doesn't exist
TEMP_DIR = "/tmp/pdf_files/"
os.makedirs(TEMP_DIR, exist_ok=True)

# App title and layout
st.set_page_config(page_title="DocAI Dashboard", layout="wide")


# Define section functions
def I_dashboard_section():
    st.title("📊 Invoice Dashboard")
    st.header("Invoice Processing Status")

def P_dashboard_section():
    st.title("📊 Purchase Dashboard")
    st.header("Purchase Processing Status")

def live_view_logic():
    st.header("🚀 Live View")

def manual_review_section():
    st.header("🔍 Manual Review")

def score_threshold_section():
    st.header("📈 Score Threshold")
    st.write("Score thresholds and settings.")

def validated_records_section():
    st.header("✅ Validated Records")
    st.write("Recent Top 10 Validated Records.")

def metadata_table_selection():
    st.header("⚙️ MetaData")
    st.write("MetaData and settings.")
    
def welcome_page():
    st.title("🎉 Welcome to the DocAI System")
    st.write("""
        This is the main page of the DocAI Dashboard system. Use the sidebar to navigate.
        - Select a Model (e.g., **INVOICE MODEL** or **PURCHASED MODEL**).
        - Explore the dashboard options.
    """)

# Sidebar setup
st.sidebar.title("DocAI Navigation")

form_selection = st.sidebar.selectbox("Select Model", ["SELECT MODEL", "INVOICE MODEL", "PURCHASED MODEL"], index=0)

# Step 3: Initialize session state
if "selected_tab" not in st.session_state:
    st.session_state["selected_tab"] = None

# Always initialize selected_tab to avoid NameError
selected_tab = st.session_state["selected_tab"]


if form_selection == "INVOICE MODEL":
    st.sidebar.markdown("### 🧭 Invoice Model Navigation")

    menu_options = {
        "Dashboard": {"icon": "📊", "function": I_dashboard_section},
        "Live view": {"icon": "🚀", "function": live_view_logic},
        "Manual Review": {"icon": "🔍", "function": manual_review_section},
        "Score Threshold": {"icon": "📈", "function": score_threshold_section},
        "Validated Records": {"icon": "✅", "function": validated_records_section},
        "Settings": {"icon": "⚙️", "function": metadata_table_selection}
    }
    
    # Render sidebar menu items
    for tab, data in menu_options.items():
        if st.sidebar.button(f"{data['icon']} {tab}"):
            st.session_state["selected_tab"] = tab
    
    # Check the selected tab and execute the corresponding function
    if st.session_state["selected_tab"] in menu_options:
        selected_tab = st.session_state["selected_tab"]
        menu_options[selected_tab]["function"]()
    else:
        st.info("Please select an option from the sidebar.")

elif form_selection == "PURCHASED MODEL":
    st.sidebar.markdown("### 🚚 Purchase Model Navigation")

    # Define menu options for Purchased Model
    menu_options = {
        "Dashboard": {"icon": "📊", "function": P_dashboard_section},
        "Live view": {"icon": "🚀", "function": live_view_logic},
        "Manual Review": {"icon": "🔍", "function": manual_review_section},
        "Score Threshold": {"icon": "📈", "function": score_threshold_section},
        "Validated Records": {"icon": "✅", "function": validated_records_section},
        "Settings": {"icon": "⚙️", "function": metadata_table_selection}
    }

    # Render sidebar menu items
    for tab, data in menu_options.items():
        if st.sidebar.button(f"{data['icon']} {tab}"):
            st.session_state["selected_tab"] = tab
    
    # Check the selected tab and execute the corresponding function
    if st.session_state["selected_tab"] in menu_options:
        selected_tab = st.session_state["selected_tab"]
        menu_options[selected_tab]["function"]()
    else:
        st.info("Please select an option from the sidebar.")
else:
    # Default welcome page when "Select Model" is selected
    st.session_state["selected_tab"] = None  # Reset selected_tab
    welcome_page()

def get_table_name(table_key, category=None):
    if category:
        # Return table name for nested categories like "invoice" or "purchase"
        return f"{db_name}.{schema_name}.{tables[category][table_key]}"
    else:
        # Return table name for top-level keys
        return f"{db_name}.{schema_name}.{tables[table_key]}"


# PDF Viewer Helper Functions
def list_stage_files(stage_name):
    try:
        stage_name = stage_name.strip('@').upper()
        list_query = f"LIST @{stage_name}"
        result = session.sql(list_query).collect()
        pdf_files = [row['name'].split('/')[-1] for row in result 
                     if row['name'].lower().endswith('.pdf')]
        return pdf_files
    except Exception as e:
        st.error(f"Error listing files from stage: {str(e)}")
        return []

def load_pdf(sel_doc, stage_name):
    try:
        stage_name = stage_name.strip('@').upper()
        stage_path = f"@{stage_name}/{sel_doc}"
        local_file_path = os.path.join(TEMP_DIR, sel_doc)
        session.file.get(stage_path, TEMP_DIR)
        if os.path.exists(local_file_path):
            pdf = pdfium.PdfDocument(local_file_path)
            st.session_state["pdf_doc"] = pdf
            st.session_state["pdf_page"] = 0
            st.session_state["pdf_path"] = local_file_path
            return True
        else:
            st.error(f"File not found at {local_file_path}")
            return False
    except Exception as e:
        st.error(f"Error loading PDF: {str(e)}")
        return False

def display_pdf_page():
    try:
        if "pdf_doc" not in st.session_state:
            return
        pdf_doc = st.session_state["pdf_doc"]
        page_num = st.session_state["pdf_page"]
        page = pdf_doc[page_num]
        bitmap = page.render(scale=2)
        pil_image = bitmap.to_pil()
        st.image(pil_image, use_column_width=True)
    except Exception as e:
        st.error(f"Error displaying PDF page: {str(e)}")

def cleanup_temp_files():
    try:
        if "pdf_path" in st.session_state:
            pdf_path = st.session_state["pdf_path"]
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
    except Exception as e:
        st.error(f"Error cleaning up temp files: {str(e)}")

def fetch_table_data(query):
    with st.spinner("Fetching data..."):
        return session.sql(query).to_pandas()

def fetch_table_data_count(query):
    try:
        result = session.sql(query).collect()
        if result and len(result) > 0:
            return result[0][0]
        return 0
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return 0


def next_pdf_page():
    if "pdf_doc" in st.session_state and st.session_state["pdf_page"] + 1 < len(st.session_state["pdf_doc"]):
        st.session_state["pdf_page"] += 1

def previous_pdf_page():
    if "pdf_doc" in st.session_state and st.session_state["pdf_page"] > 0:
        st.session_state["pdf_page"] -= 1


# Dashboard section
def I_dashboard_section():

    # Get current time and calculate the range for the last 30 days
    now = datetime.now()
    last_30_days = now - timedelta(days=30)


    # Display the static time range as a grey box
    st.markdown(
        f"""
        <div style="
            background-color: #d9d9d9; 
            color: #333; 
            border: 1px solid #ccc; 
            border-radius: 5px; 
            padding: 4px 8px; 
            font-size: 12px; 
            display: inline-block; 
            position: relative; 
            margin : 30px auto 20px auto ;
            box-shadow: 1px 1px 3px #ddd;">
            <strong>Time Range:</strong> {last_30_days.strftime('%Y-%m-%d %H:%M:%S')} - {now.strftime('%Y-%m-%d %H:%M:%S')}
        </div>
        """,
        unsafe_allow_html=True
    )


    # Table name
    prefilter_table = get_table_name("prefilter")

    # Query for the last 30 days
    query_status = f"""
    SELECT STATUS, COUNT(*) AS COUNT
    FROM {prefilter_table}
    WHERE DATECREATED >= '{last_30_days.strftime('%Y-%m-%d %H:%M:%S')}'
          AND DATECREATED <= '{now.strftime('%Y-%m-%d %H:%M:%S')}'
    GROUP BY STATUS
    """

    # Fetch data for the last 24 hours
    status_df = fetch_table_data(query_status)

    if not status_df.empty:
        # Replace status for better display and standardize case
        status_df["STATUS"] = status_df["STATUS"].str.title()  # Convert to Title Case

        # Replace specific values for better clarity
        status_df["STATUS"] = status_df["STATUS"].replace({"Failed": "Manual Review"})
        status_df["STATUS"] = status_df["STATUS"].replace({"Manual_Review": "Manual Review"})

        # Define color mapping for statuses
        color_mapping = {
            "Processed": "#006400",
            "Not Processed": "#ffc107",
            "Manual Review": "#4285F4",
            "Skipped": "#C0C0C0",
            "In Progress": "yellow",
            "Error": "red"
        }

        # Ensure all statuses are included in the color mapping
        missing_statuses = set(status_df["STATUS"]) - set(color_mapping.keys())
        if missing_statuses:
            st.warning(f"Missing color mapping for: {', '.join(missing_statuses)}")

        # Add a new color column based on the STATUS
        status_df["COLOR"] = status_df["STATUS"].map(color_mapping).fillna("gray")  # Default to gray for unmapped statuses

        # Create a pie chart with dynamic colors
        pie_chart = alt.Chart(status_df).mark_arc().encode(
            theta=alt.Theta(field="COUNT", type="quantitative"),
            color=alt.Color(
                "STATUS:N",  # Encode color based on STATUS
                scale=alt.Scale(
                    domain=list(color_mapping.keys()),  # Define the statuses
                    range=list(color_mapping.values())  # Define corresponding colors
                )
            ),
            tooltip=["STATUS", "COUNT"]  # Add tooltips for more details
        ).properties(
            title="Status Distribution (Last 30 Days)"
        )

        # Display the chart
        st.altair_chart(pie_chart, use_container_width=True)
    else:
        st.info("No data available for the selected time range.")


    # Fetch failed records and display bar chart
    st.subheader("Failed Records Analysis")

    fetch_failed_query = f"""
        SELECT SCORE_NAME, COUNT(*) AS FAILURE_COUNT, MAX(SCORE_VALUE) AS SCORE_VALUE
        FROM {get_table_name("score_failed", "invoice")}
        GROUP BY SCORE_NAME
        ORDER BY FAILURE_COUNT DESC;
    """

    failed_df = fetch_table_data(fetch_failed_query)
    
    if not failed_df.empty:
        # Ensure data types
        failed_df["SCORE_NAME"] = failed_df["SCORE_NAME"].astype(str)
        failed_df["FAILURE_COUNT"] = pd.to_numeric(failed_df["FAILURE_COUNT"], errors="coerce")
    
        # Dynamically calculate the chart height
        bar_chart_height = max(400, len(failed_df) * 50)  # Minimum height of 400px, 50px per record
    
        # Enhanced Bar Chart with Dynamic Spacing
        bar_chart = alt.Chart(failed_df).mark_bar(size=25, color="steelblue").encode(
            y=alt.Y(
                "SCORE_NAME:N",
                sort="ascending",  # Sort alphabetically by SCORE_NAME
                title="Score Name",
                axis=alt.Axis(labelFontSize=12, titleFontSize=14)
            ),
            x=alt.X(
                "FAILURE_COUNT:Q",
                title="Failure Count",
                axis=alt.Axis(labelFontSize=12, titleFontSize=14),
                scale=alt.Scale(domain=[0, failed_df["FAILURE_COUNT"].max() + 5])  # Padding for better visibility
            ),
            tooltip=[
                alt.Tooltip("SCORE_NAME:N", title="Score Name"),
                alt.Tooltip("FAILURE_COUNT:Q", title="Failure Count"),
                alt.Tooltip("SCORE_VALUE:Q", title="Score Value", format=".2f")
            ]
        ).properties(
            title=alt.TitleParams(
                text="Failure Counts by Score Name",
                fontSize=16,
                anchor="start"
            ),
            width=1000,  # Adjust width for better readability
            height=bar_chart_height  # Dynamically calculated height
        ).configure_axis(
            grid=True,  # Add grid lines for better readability
            gridDash=[4, 4],  # Dashed grid lines
            gridColor="lightgray"  # Light gray grid color
        ).configure_view(
            strokeOpacity=0  # Remove the border around the chart
        )
    
        # Display bar chart
        st.altair_chart(bar_chart, use_container_width=True)
    else:
        st.info("No data available in col_score_failed_history.")


# Dashboard section
def P_dashboard_section():
    # Get current time and calculate the range for the last 30 days
    now = datetime.now()
    last_30_days = now - timedelta(days=30)


    # Display the static time range as a grey box
    st.markdown(
        f"""
        <div style="
            background-color: #d9d9d9; 
            color: #333; 
            border: 1px solid #ccc; 
            border-radius: 5px; 
            padding: 4px 8px; 
            font-size: 12px; 
            display: inline-block; 
            position: relative; 
            margin : 30px auto 20px auto ;
            box-shadow: 1px 1px 3px #ddd;">
            <strong>Time Range:</strong> {last_30_days.strftime('%Y-%m-%d %H:%M:%S')} - {now.strftime('%Y-%m-%d %H:%M:%S')}
        </div>
        """,
        unsafe_allow_html=True
    )


    # Table name
    prefilter_table = get_table_name("prefilter")

    # Query for the last 30 days
    query_status = f"""
    SELECT STATUS, COUNT(*) AS COUNT
    FROM {prefilter_table}
    WHERE DATECREATED >= '{last_30_days.strftime('%Y-%m-%d %H:%M:%S')}'
          AND DATECREATED <= '{now.strftime('%Y-%m-%d %H:%M:%S')}'
    GROUP BY STATUS
    """

    # Fetch data for the last 24 hours
    status_df = fetch_table_data(query_status)

    if not status_df.empty:
        # Replace status for better display and standardize case
        status_df["STATUS"] = status_df["STATUS"].str.title()  # Convert to Title Case

        # Replace specific values for better clarity
        status_df["STATUS"] = status_df["STATUS"].replace({"Failed": "Manual Review"})
        status_df["STATUS"] = status_df["STATUS"].replace({"Manual_Review": "Manual Review"})

        # Define color mapping for statuses
        color_mapping = {
            "Processed": "#006400",
            "Not Processed": "#ffc107",
            "Manual Review": "#4285F4",
            "Skipped": "#C0C0C0",
            "In Progress": "yellow",
            "Error": "red"
        }

        # Ensure all statuses are included in the color mapping
        missing_statuses = set(status_df["STATUS"]) - set(color_mapping.keys())
        if missing_statuses:
            st.warning(f"Missing color mapping for: {', '.join(missing_statuses)}")

        # Add a new color column based on the STATUS
        status_df["COLOR"] = status_df["STATUS"].map(color_mapping).fillna("gray")  # Default to gray for unmapped statuses

        # Create a pie chart with dynamic colors
        pie_chart = alt.Chart(status_df).mark_arc().encode(
            theta=alt.Theta(field="COUNT", type="quantitative"),
            color=alt.Color(
                "STATUS:N",  # Encode color based on STATUS
                scale=alt.Scale(
                    domain=list(color_mapping.keys()),  # Define the statuses
                    range=list(color_mapping.values())  # Define corresponding colors
                )
            ),
            tooltip=["STATUS", "COUNT"]  # Add tooltips for more details
        ).properties(
            title="Status Distribution (Last 30 Days)"
        )

        # Display the chart
        st.altair_chart(pie_chart, use_container_width=True)
    else:
        st.info("No data available for the selected time range.")

    # Fetch failed records and display bar chart
    st.subheader("Failed Records Analysis")

    fetch_failed_query = f"""
        SELECT SCORE_NAME, COUNT(*) AS FAILURE_COUNT, MAX(SCORE_VALUE) AS SCORE_VALUE
        FROM {get_table_name("score_failed", "purchase")}
        GROUP BY SCORE_NAME
        ORDER BY FAILURE_COUNT DESC;
    """

    
    failed_df = fetch_table_data(fetch_failed_query)
    
    if not failed_df.empty:
        # Ensure data types
        failed_df["SCORE_NAME"] = failed_df["SCORE_NAME"].astype(str)
        failed_df["FAILURE_COUNT"] = pd.to_numeric(failed_df["FAILURE_COUNT"], errors="coerce")
    
        # Dynamically calculate the chart height
        bar_chart_height = max(400, len(failed_df) * 50)  # Minimum height of 400px, 50px per record
    
        # Enhanced Bar Chart with Dynamic Spacing
        bar_chart = alt.Chart(failed_df).mark_bar(size=25, color="steelblue").encode(
            y=alt.Y(
                "SCORE_NAME:N",
                sort="ascending",  # Sort alphabetically by SCORE_NAME
                title="Score Name",
                axis=alt.Axis(labelFontSize=12, titleFontSize=14)
            ),
            x=alt.X(
                "FAILURE_COUNT:Q",
                title="Failure Count",
                axis=alt.Axis(labelFontSize=12, titleFontSize=14),
                scale=alt.Scale(domain=[0, failed_df["FAILURE_COUNT"].max() + 5])  # Padding for better visibility
            ),
            tooltip=[
                alt.Tooltip("SCORE_NAME:N", title="Score Name"),
                alt.Tooltip("FAILURE_COUNT:Q", title="Failure Count"),
                alt.Tooltip("SCORE_VALUE:Q", title="Score Value", format=".2f")
            ]
        ).properties(
            title=alt.TitleParams(
                text="Failure Counts by Score Name",
                fontSize=16,
                anchor="start"
            ),
            width=1000,  # Adjust width for better readability
            height=bar_chart_height  # Dynamically calculated height
        ).configure_axis(
            grid=True,  # Add grid lines for better readability
            gridDash=[4, 4],  # Dashed grid lines
            gridColor="lightgray"  # Light gray grid color
        ).configure_view(
            strokeOpacity=0  # Remove the border around the chart
        )
    
        # Display bar chart
        st.altair_chart(bar_chart, use_container_width=True)
    else:
        st.info("No data available in col_score_failed_history.")

def fetch_table_data1(query):
    try:
        # Get the active Snowflake session
        session = get_active_session()

        # Execute the query
        result = session.sql(query).collect()

        # Extract the count from the query result
        if result and len(result) > 0:
            return result[0][0]  # Assuming the count is in the first row, first column
        return 0  # Return 0 if no result is found

    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return 0  # Return 0 in case of error

def create_pipeline_chart(stages, manual_stage, extraction_failed_stage):
    # Create the pipeline chart
    fig = go.Figure()

    # Add primary pipeline stages
    for stage in stages:
        fig.add_shape(
            type="rect",
            x0=stage["x0"],
            y0=stage["y0"],
            x1=stage["x1"],
            y1=stage["y1"],
            line=dict(color="white"),
            fillcolor=stage["color"],
        )
        fig.add_annotation(
            x=(stage["x0"] + stage["x1"]) / 2,
            y=(stage["y0"] + stage["y1"]) / 2,
            text=f"<b style='font-size:30px'>{stage['icon']}</b><br><b>{stage['name']}</b>",
            showarrow=False,
            font=dict(size=18, color="white"),
            align="center",
        )

    # Add manual review stage as a branch
    fig.add_shape(
        type="rect",
        x0=manual_stage["x0"],
        y0=manual_stage["y0"],
        x1=manual_stage["x1"],
        y1=manual_stage["y1"],
        line=dict(color="white"),
        fillcolor=manual_stage["color"],
    )
    fig.add_annotation(
        x=(manual_stage["x0"] + manual_stage["x1"]) / 2,
        y=(manual_stage["y0"] + manual_stage["y1"]) / 2,
        text=f"<b style='font-size:30px'>{manual_stage['icon']}</b><br><b>{manual_stage['name']}</b>",
        showarrow=False,
        font=dict(size=18, color="white"),
        align="center",
    )

    # Add arrows between primary stages
    for i in range(len(stages) - 1):
        fig.add_annotation(
            ax=stages[i]["x1"], ay=(stages[i]["y0"] + stages[i]["y1"]) / 2,
            axref="x", ayref="y",
            x=stages[i + 1]["x0"], y=(stages[i + 1]["y0"] + stages[i + 1]["y1"]) / 2,
            xref="x", yref="y",
            showarrow=True, arrowhead=3, arrowsize=2, arrowwidth=2, arrowcolor="black", opacity=0.8,
        )



    # Add extraction failed stage as a branch
    fig.add_shape(
        type="rect",
        x0=extraction_failed_stage["x0"],
        y0=extraction_failed_stage["y0"],
        x1=extraction_failed_stage["x1"],
        y1=extraction_failed_stage["y1"],
        line=dict(color="white"),
        fillcolor=extraction_failed_stage["color"],
    )
    fig.add_annotation(
        x=(extraction_failed_stage["x0"] + extraction_failed_stage["x1"]) / 2,
        y=(extraction_failed_stage["y0"] + extraction_failed_stage["y1"]) / 2,
        text=f"<b style='font-size:30px'>{extraction_failed_stage['icon']}</b><br><b>{extraction_failed_stage['name']}</b>",
        showarrow=False,
        font=dict(size=18, color="white"),
        align="center",
    )

    # Add arrow from Preprocessed to Manual Review
    fig.add_annotation(
        ax=(stages[1]["x0"] + stages[1]["x1"]) / 2,  # Horizontal center of Preprocessed
        ay=stages[1]["y0"],  # Bottom edge of Preprocessed
        axref="x",
        ayref="y",
        x=(manual_stage["x0"] + manual_stage["x1"]) / 2,  # Horizontal center of Manual Review
        y=manual_stage["y1"],  # Top edge of Manual Review
        xref="x",
        yref="y",
        showarrow=True, arrowhead=3, arrowsize=2, arrowwidth=2, arrowcolor="black",
        opacity=0.8,
    )

   # Add arrow from Extraction to Extraction Failed
    fig.add_annotation(
        ax=(stages[2]["x0"] + stages[2]["x1"]) / 2,
        ay=stages[2]["y0"],
        axref="x",
        ayref="y",
        x=(extraction_failed_stage["x0"] + extraction_failed_stage["x1"]) / 2,
        y=extraction_failed_stage["y1"],
        xref="x",
        yref="y",
        showarrow=True,
        arrowhead=3,
        arrowsize=2,
        arrowwidth=2,
        arrowcolor="black",
    )
    
    # Update layout with enhanced visuals
    fig.update_layout(
        xaxis=dict(range=[-1, 20], visible=False),
        yaxis=dict(range=[2, 7], visible=False),
        width=1200,
        height=500,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor="rgba(240,248,255,1)",
        plot_bgcolor="rgba(255,255,255,1)",
    )
    st.plotly_chart(fig, use_container_width=True)


    st.caption("🔄 Dashboard refreshes every 5 seconds.")

    return fig



def live_view_logic():
    
    # Refresh interval (5 minutes = 300 seconds)
    refresh_interval = 300

    # Calculate the last 30 days range
    now = datetime.now()
    last_30_days = now - timedelta(days=30)

    try:

        # Display the time range
        st.markdown(
            f"""
            <div style="
                background-color: #d9d9d9; 
                color: #333; 
                border: 1px solid #ccc; 
                border-radius: 5px; 
                padding: 4px 8px; 
                font-size: 12px; 
                display: inline-block; 
                position: relative; 
                margin : 30px auto 20px auto ;
                box-shadow: 1px 1px 3px #ddd;">
                <strong>Time Range:</strong> {last_30_days.strftime('%Y-%m-%d %H:%M:%S')} - {now.strftime('%Y-%m-%d %H:%M:%S')}
            </div>
            """,
            unsafe_allow_html=True
        )
        # Determine table references based on selected form
        if form_selection == "INVOICE MODEL":
            table_data = tables["invoice"]
        elif form_selection == "PURCHASED MODEL":
            table_data = tables["purchase"]
        else:
            st.error("Invalid Model selection.")
            return
            
        # Fetch data counts from relevant tables
        waiting_count = fetch_table_data1(f"SELECT COUNT(*) FROM {tables['prefilter']} WHERE STATUS = 'NOT PROCESSED'")
        preprocessed_count = fetch_table_data1(f"SELECT COUNT(*) FROM {tables['prefilter']} WHERE STATUS = 'PROCESSED'")
        manual_review_count = fetch_table_data1(f"SELECT COUNT(*) FROM {tables['prefilter']} WHERE STATUS = 'FAILED'")
        extraction_count = fetch_table_data1(f"SELECT COUNT(*) FROM {tables['extraction']} WHERE STATUS = 'PROCESSED'")
        extraction_failed_count = fetch_table_data1(f"SELECT COUNT(*) FROM {table_data['flattened']} WHERE STATUS = 'FAILED'")
        validated_count = fetch_table_data1(f"SELECT COUNT(*) FROM {table_data['validated']}")


        # # Fetch data counts from relevant tables for the last 30 days
        # waiting_count = fetch_table_data1(f"""
        #     SELECT COUNT(*) 
        #     FROM {tables['prefilter']} 
        #     WHERE STATUS = 'NOT PROCESSED' 
        #     AND DATECREATED >= '{last_30_days.strftime('%Y-%m-%d %H:%M:%S')}'
        # """)

        # preprocessed_count = fetch_table_data1(f"""
        #     SELECT COUNT(*) 
        #     FROM {tables['prefilter']} 
        #     WHERE STATUS = 'PROCESSED' 
        #     AND DATECREATED >= '{last_30_days.strftime('%Y-%m-%d %H:%M:%S')}'
        # """)

        # manual_review_count = fetch_table_data1(f"""
        #     SELECT COUNT(*) 
        #     FROM {tables['prefilter']} 
        #     WHERE STATUS = 'FAILED' 
        #     AND DATECREATED >= '{last_30_days.strftime('%Y-%m-%d %H:%M:%S')}'
        # """)

        # extraction_count = fetch_table_data1(f"""
        #     SELECT COUNT(*) 
        #     FROM {tables['extraction']} 
        #     WHERE STATUS = 'PROCESSED' 
        #     AND DATECREATED >= '{last_30_days.strftime('%Y-%m-%d %H:%M:%S')}'
        # """)

        # extraction_failed_count = fetch_table_data1(f"""
        #     SELECT COUNT(*) 
        #     FROM {table_data['flattened']} 
        #     WHERE STATUS = 'FAILED' 
        #     AND DATECREATED >= '{last_30_days.strftime('%Y-%m-%d %H:%M:%S')}'
        # """)

        # validated_count = fetch_table_data1(f"""
        #     SELECT COUNT(*) 
        #     FROM {table_data['validated']} 
        #     WHERE DATECREATED >= '{last_30_days.strftime('%Y-%m-%d %H:%M:%S')}'
        # """)
        
        # Define colors based on status
        def get_color(status_count, status_type, is_processed=False, is_completed=False):
            if is_completed:
                return "#4285F4"  # SlateBlue for completed
            elif is_processed and status_count > 0:
                return "#0F9D58"  # Green for success
            elif not is_processed and status_count > 0:
                return "#F4B400"  # Yellow for in-progress
            else:
                return "#DB4437"  # Red for not processed

        # Determine the color for each stage
        extraction_color = get_color(extraction_count, "PROCESSED", is_processed=False, is_completed=(extraction_count == 0))
        waiting_color = get_color(waiting_count, "NOT PROCESSED", is_processed=False, is_completed=(waiting_count == 0))
        preprocessed_color = get_color(preprocessed_count, "PROCESSED", is_processed=False, is_completed=(preprocessed_count == 0))
        #flattened_color = get_color(flattened_count, "Processed", is_processed=True, is_completed=(flattened_count == 0))
        validated_color = get_color(validated_count, "PROCESSED", is_processed=True, is_completed=(validated_count == 0))
        manual_review_color = get_color(manual_review_count, "Failed", is_processed=False, is_completed=(manual_review_count == 0))
        extraction_failed_color = get_color(extraction_failed_count, "FAILED", is_processed=False, is_completed=(extraction_failed_count == 0))
        


        # Define pipeline stages
        y_position = 5
        branch_y_position = 3  # For branches

        stages = [
            {"name": f"{waiting_count} Doc<br>Waiting", "x0": 0, "x1": 3, "y0": y_position, "y1": y_position + 1, "color": waiting_color, "icon": "⏳"},
            {"name": f"{preprocessed_count} Doc<br>Preprocessed", "x0": 4, "x1": 7, "y0": y_position, "y1": y_position + 1, "color": preprocessed_color, "icon": "⚙️"},
            {"name": f"{extraction_count} Doc<br>Extraction", "x0": 8, "x1": 11, "y0": y_position, "y1": y_position + 1, "color": extraction_color, "icon": "📂"},
            #{"name": f"{flattened_count} Doc<br>Flattened", "x0": 12, "x1": 15, "y0": y_position, "y1": y_position + 1, "color": flattened_color, "icon": "📄"},
            {"name": f"{validated_count} Doc<br>Validated", "x0": 13, "x1": 17, "y0": y_position, "y1": y_position + 1, "color": validated_color, "icon": "✅"},
        ]
            

        # Manual review branch
        manual_stage = {
            "name": f"{manual_review_count} Sent for<br>Manual Review",
            "x0": 3.7,
            "x1": 7.2,
            "y0": branch_y_position,
            "y1": branch_y_position + 1,
            "color": manual_review_color,
            "icon": "🔍"
        }

        # Extraction failed branch
        extraction_failed_stage = {
            "name": f"{extraction_failed_count} Docs<br>Validation Failed",
            "x0": 7.5,
            "x1": 11.5,
            "y0": branch_y_position,
            "y1": branch_y_position + 1,
            "color": extraction_failed_color,
            "icon": "❌"
        }

        # Create the pipeline chart
        
        fig = create_pipeline_chart(stages, manual_stage, extraction_failed_stage)
        
        # Average processing time per table based on selected model
        queries = {
            "Preprocessed": f"""
                SELECT 
                    SUM(DATEDIFF(second, PROCESS_START_TIME, PROCESS_END_TIME)) AS TOTAL_SECONDS,
                    COUNT(*) AS TOTAL_RECORDS
                FROM {tables['prefilter']}
                WHERE PROCESS_START_TIME IS NOT NULL AND PROCESS_END_TIME IS NOT NULL;
            """,
            "Extraction": f"""
                SELECT 
                    SUM(DATEDIFF(second, PROCESS_START_TIME, PROCESS_END_TIME)) AS TOTAL_SECONDS,
                    COUNT(*) AS TOTAL_RECORDS
                FROM {tables['extraction']}
                WHERE PROCESS_START_TIME IS NOT NULL AND PROCESS_END_TIME IS NOT NULL;
            """,
            "Flattened": f"""
                SELECT 
                    SUM(DATEDIFF(second, PROCESS_START_TIME, PROCESS_END_TIME)) AS TOTAL_SECONDS,
                    COUNT(*) AS TOTAL_RECORDS
                FROM {table_data['flattened']}
                WHERE PROCESS_START_TIME IS NOT NULL AND PROCESS_END_TIME IS NOT NULL;
            """,
            "Validated": f"""
                SELECT 
                    COUNT(*) AS TOTAL_RECORDS, 
                    SUM(TIMESTAMPDIFF(milliseconds, PROCESS_START_TIME, PROCESS_END_TIME)) AS TOTAL_MILLISECONDS,
                    CONCAT(
                        FLOOR(SUM(TIMESTAMPDIFF(milliseconds, PROCESS_START_TIME, PROCESS_END_TIME)) / 60000), ' min ', 
                        FLOOR((SUM(TIMESTAMPDIFF(milliseconds, PROCESS_START_TIME, PROCESS_END_TIME)) % 60000) / 1000), ' sec ', 
                        (SUM(TIMESTAMPDIFF(milliseconds, PROCESS_START_TIME, PROCESS_END_TIME)) % 1000), ' ms'
                    ) AS FORMATTED_TIME
                FROM {table_data['validated']}
                WHERE PROCESS_START_TIME IS NOT NULL AND PROCESS_END_TIME IS NOT NULL;
            """

        }

       # Initialize average times dictionary
        # Initialize average times dictionary
        avg_times = {}
        
        # Iterate through the queries and execute them
        for table, query in queries.items():
            try:
                # Execute the query
                result = fetch_table_data(query)  # Replace with your actual query execution function
        
                # Safely handle the DataFrame result
                if isinstance(result, pd.DataFrame) and not result.empty:
                    if table == "Validated":  # Special handling for Validated table
                        total_records = result.iloc[0]["TOTAL_RECORDS"] if "TOTAL_RECORDS" in result.columns else 0
                        total_milliseconds = result.iloc[0]["TOTAL_MILLISECONDS"] if "TOTAL_MILLISECONDS" in result.columns else 0
                        formatted_time = result.iloc[0]["FORMATTED_TIME"] if "FORMATTED_TIME" in result.columns else "0 min 0 sec 0 ms"
        
                        # Add formatted time and seconds to avg_times dictionary
                        avg_times[table] = {
                            "seconds": total_milliseconds / 1000 if total_records > 0 else 0,
                            "formatted": formatted_time,
                        }
                    else:
                        # Standard handling for other tables
                        total_seconds = result.iloc[0]["TOTAL_SECONDS"] if "TOTAL_SECONDS" in result.columns else 0
                        total_records = result.iloc[0]["TOTAL_RECORDS"] if "TOTAL_RECORDS" in result.columns else 0
        
                        # Calculate average time in seconds
                        avg_time_seconds = total_seconds / total_records if total_records > 0 else 0
                        avg_times[table] = avg_time_seconds
                else:
                    st.warning(f"No data returned for {table}. Please check the query or data source.")
                    avg_times[table] = None  # Explicitly set None for tables with no data
            except Exception as e:
                avg_times[table] = None
                st.error(f"Error fetching data for {table}: {str(e)}")
        
        # Define colors for the bar chart
        custom_colors = {
            "Preprocessed": "#1f77b4",  # Blue
            "Extraction": "#87CEFA",    # Light Blue
            "Flattened": "#FFCC00",     # Yellow
            "Validated": "#32CD32",     # Lime Green
        }
        
        # Prepare data for the bar chart
        chart_data = {
            "Table": list(avg_times.keys()),
            "Average Time (Seconds)": [
                avg_times[table]["seconds"] if isinstance(avg_times[table], dict) else avg_times[table]
                for table in avg_times.keys()
            ],
            "Formatted Time": [
                avg_times[table]["formatted"] if isinstance(avg_times[table], dict) else ""
                for table in avg_times.keys()
            ],
        }
        
        df_chart = pd.DataFrame(chart_data)
        
        # Create the bar chart
        fig_chart = px.bar(
            df_chart,
            x="Table",
            y="Average Time (Seconds)",
            title="Average Processing Time per Table",
            text="Formatted Time",  # Display formatted time for Validated table
            color="Table",  # Use the custom color map for the 'Table' column
            color_discrete_map=custom_colors,  # Apply custom colors
        )
        
        # Update chart formatting
        fig_chart.update_traces(texttemplate="%{text}", textposition="outside")
        fig_chart.update_layout(
            xaxis_title="Table",
            yaxis_title="Average Time (Seconds)",
            font=dict(size=14),
        )
        
        # Render the chart
        st.plotly_chart(fig_chart, use_container_width=True)


    except Exception as e:
        st.error(f"Error fetching live data: {str(e)}")

    # Refresh the dashboard
    time.sleep(refresh_interval)
    st.experimental_rerun()



# Convert fractional minutes to "X min Y sec" format
def format_time(time_sec):
    if time_sec==0:
        return 0
    total_seconds = time_sec * 100  # Convert minutes to seconds
    minutes = total_seconds // 60  # Get whole minutes
    seconds = total_seconds % 60   # Get remaining seconds
    return f"{minutes}m {seconds}s"


def manual_review_section():
    # Create tabs for different manual review sections
    tabs = st.tabs(["DocAIExtract ManualReview", "ScoreField ManualReview"])

    ### Tab 1: DocAI Extract Manual Review
    with tabs[0]:
        handle_manual_review(
            query=f"""
                SELECT * 
                FROM {db_name}.{schema_name}.{tables['prefilter']}
                WHERE STATUS = 'FAILED';
            """,
            stage_name="MANUAL_REVIEW",
            tab_key="DocAIExtract"
        )

    ### Tab 2: Score Failed Manual Review
    with tabs[1]:
        st.markdown("## 🛠️ ScoreField Manual Review")
        st.markdown("---")  # Horizontal line for separation

        # Dynamically fetch the table and stage based on the selected form
        if form_selection == "INVOICE MODEL":
            flattened_table = tables['invoice']['flattened']
            stage_name = "INVOICE_DOCS"
        elif form_selection == "PURCHASED MODEL":
            flattened_table = tables['purchase']['flattened']
            stage_name = "INVOICE_DOCS"
        else:
            st.error("Invalid model selection.")
            return

        # Fetch records from the dynamically determined table
        query_score_failed = f"""
        SELECT * FROM {db_name}.{schema_name}.{flattened_table} WHERE status='FAILED';
        """
        score_failed_df = fetch_table_data(query_score_failed)

        if not score_failed_df.empty:
            st.write(f"### 🚩 Score Failed Records for {flattened_table}")

            # Display the dataframe with formatted style
            st.dataframe(
                score_failed_df,
                hide_index=True,
                use_container_width=True
            )

            # Check if the 'RELATIVEPATH' column exists
            if 'RELATIVEPATH' in score_failed_df.columns:
                # Get list of unique PDF files
                pdf_files = score_failed_df['RELATIVEPATH'].unique().tolist()

                # Add spacing for better layout
                st.markdown("### 📄 Select a PDF to View")
                selected_file = st.selectbox(
                    "Choose a PDF file to view",
                    options=pdf_files,
                    index=None,
                    placeholder="Select a PDF...",
                    key="select_pdf_scorefield"
                )

                if selected_file:
                    try:
                        if "pdf_doc" not in st.session_state or st.session_state.get("current_doc") != selected_file:
                            cleanup_temp_files()
                            if load_pdf(selected_file, stage_name):
                                st.session_state["current_doc"] = selected_file

                        if "pdf_doc" in st.session_state:
                            st.markdown("### 📂 PDF Viewer")
                            display_pdf_page()

                            # Navigation controls
                            nav_container = st.container()
                            nav1, nav2, nav3 = nav_container.columns([2, 2, 1])

                            # Previous Button
                            with nav1:
                                if st.button("⏮️ Previous", key=f"prev_page_{selected_file}_1"):
                                    if st.session_state["pdf_page"] > 0:
                                        st.session_state["pdf_page"] -= 1

                            # Current Page Display
                            with nav2:
                                total_pages = len(st.session_state["pdf_doc"])
                                current_page = st.session_state["pdf_page"] + 1
                                st.markdown(f"Page **{current_page}** of **{total_pages}**")

                            # Next Button
                            with nav3:
                                if st.button("Next ⏭️", key=f"next_page_{selected_file}_1"):
                                    if st.session_state["pdf_page"] < total_pages - 1:
                                        st.session_state["pdf_page"] += 1
                    except Exception as e:
                        st.error(f"Error loading PDF: {str(e)}")
            else:
                st.warning("The column 'RELATIVEPATH' does not exist in the data.")
        else:
            st.info(f"No failed records found in the {flattened_table} table.")



def handle_manual_review(query, stage_name, tab_key):
    """
    Handles the manual review logic for each tab.
    """
    # Fetch data
    data_df = fetch_table_data(query)

    if not data_df.empty:
        st.write(f"### {tab_key} Records")

        # Get unique filenames
        all_filenames = data_df['FILENAME'].unique().tolist()

        # Add auto-suggest functionality
        search_query = st.selectbox(
            "Search or select a PDF file",
            options=[""] + all_filenames,
            format_func=lambda x: x if x != "" else "Type to search...",
            index=None,
            key=f"file_search_{tab_key}"
        )

        # Filter the DataFrame based on the search query
        if search_query:
            filtered_df = data_df[data_df['FILENAME'].str.contains(search_query, case=False)]
        else:
            filtered_df = data_df

        # Pagination logic
        paginate_and_display(filtered_df, tab_key, stage_name)
    else:
        st.info(f"No records found for {tab_key}.")

def paginate_and_display(filtered_df, tab_key, stage_name):
    """
    Handles pagination and displays records.
    """
    records_per_page = 10
    total_records = len(filtered_df)
    total_pages = max((total_records + records_per_page - 1) // records_per_page, 1)

    # Initialize session state for pagination
    if f"current_page_{tab_key}" not in st.session_state:
        st.session_state[f"current_page_{tab_key}"] = 1

    # Ensure current page is within bounds
    st.session_state[f"current_page_{tab_key}"] = max(
        1, min(st.session_state[f"current_page_{tab_key}"], total_pages)
    )

    # Pagination controls
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        if st.session_state[f"current_page_{tab_key}"] > 1:
            if st.button("⏮️ Previous", key=f"prev_{tab_key}"):
                st.session_state[f"current_page_{tab_key}"] -= 1
    with col2:
        st.write(f"Page {st.session_state[f'current_page_{tab_key}']} of {total_pages}")
    with col3:
        if st.session_state[f"current_page_{tab_key}"] < total_pages:
            if st.button("Next ⏭️", key=f"next_{tab_key}"):
                st.session_state[f"current_page_{tab_key}"] += 1

    # Slice the DataFrame for the current page
    start_idx = (st.session_state[f"current_page_{tab_key}"] - 1) * records_per_page
    end_idx = min(start_idx + records_per_page, total_records)

    # Paginated DataFrame
    paginated_df = filtered_df.iloc[start_idx:end_idx]
    if 'ROWID' in paginated_df.columns:
        paginated_df = paginated_df.drop(columns=['ROWID'])

    # Display paginated DataFrame
    st.dataframe(paginated_df, hide_index=True, use_container_width=True)

    # Show PDF viewer
    show_pdf_viewer(paginated_df, stage_name, tab_key)


def show_pdf_viewer(data_df, stage_name, tab_key):
    """
    Handles PDF viewing and related actions with proper clickable buttons.
    """
    pdf_files = data_df['FILENAME'].unique().tolist()
    st.write("### Select a PDF to view")
    selected_file = st.selectbox(
        "Choose a PDF file",
        options=pdf_files,
        index=None if pdf_files else None,
        placeholder="Select a PDF...",
        key=f"select_pdf_{tab_key}"
    )

    if selected_file:
        col1, col2 = st.columns([2, 2])

        # Add styled buttons using Streamlit's native `button` function
        with col1:
            revert_button_clicked = st.button(
                label="🔄 Send this document for re-processing",
                key=f"revert_{selected_file}",
                help="Reprocess this document",
                use_container_width=True
            )
        with col2:
            ignore_button_clicked = st.button(
                label="🚫 Remove this document from pipeline",
                key=f"ignore_{selected_file}",
                help="Remove this document from the pipeline",
                use_container_width=True
            )

        # Handle actions for buttons
        if revert_button_clicked:
            process_stage_action(selected_file, stage_name, action="revert")
        if ignore_button_clicked:
            process_stage_action(selected_file, stage_name, action="ignore")

        # Display PDF if selected
        try:
            if "pdf_doc" not in st.session_state or st.session_state.get("current_doc") != selected_file:
                cleanup_temp_files()
                if load_pdf(selected_file, stage_name):
                    st.session_state["current_doc"] = selected_file
                    st.session_state["pdf_page"] = 0

            if "pdf_doc" in st.session_state:
                st.write("### PDF Viewer")
                display_pdf_page()

                # Navigation controls
                nav_container = st.container()
                nav1, nav2, nav3 = nav_container.columns([2, 2, 1])
                with nav1:
                    if st.button("⏮️ Previous", key=f"prev_page_{selected_file}"):
                        previous_pdf_page()
                with nav2:
                    total_pages = len(st.session_state["pdf_doc"])
                    current_page = st.session_state["pdf_page"] + 1
                    st.write(f"Page {current_page} of {total_pages}")
                with nav3:
                    if st.button("Next ⏭️", key=f"next_page_{selected_file}"):
                        next_pdf_page()
        except Exception as e:
            st.error(f"Error loading PDF: {str(e)}")



def process_stage_action(file_name, stage_name, action):
    """
    Handles the revert or ignore actions for a file in a specific stage.
    """
    try:
        list_manual_review_query = f"LIST @{stage_name};"
        manual_review_files = session.sql(list_manual_review_query).collect()
        stage_filenames = [file['name'].split('/')[-1] for file in manual_review_files]

        if file_name not in stage_filenames:
            st.error(f"File {file_name} not found in the {stage_name} stage.")
        else:
            proc_call = f"""
                CALL {db_name}.{schema_name}.{"MOVEFILES_TO_SOURCE" if action == "revert" else "IGNOREFILES_TO_STAGE"}('{file_name}');
            """
            session.sql(proc_call).collect()

            log_query = f"""
                INSERT INTO {db_name}.{schema_name}.manual_review_history_log
                (FILENAME, ACTION, TIMESTAMP, USER_NAME, COMMENTS)
                VALUES ('{file_name}', '{'Moved to INVOICE_DOCS' if action == "revert" else 'Ignored and moved to IGNORED_DOCS'}',
                        CURRENT_TIMESTAMP, CURRENT_USER, 'Action completed successfully.');
            """
            session.sql(log_query).collect()

            delete_query = f"""
                DELETE FROM {db_name}.{schema_name}.{tables['prefilter']}
                WHERE FILENAME = '{file_name}';
            """
            session.sql(delete_query).collect()

            st.success(f"File {file_name} successfully processed.")
            time.sleep(2)
            st.rerun()
    except Exception as e:
        st.error(f"Failed to process file {file_name}: {str(e)}")

# Score Threshold section
def score_threshold_section():
    # Query to fetch data
    query = "SELECT * FROM SCORE_THRESHOLD"
    threshold_df = fetch_table_data(query)

    if not threshold_df.empty:
        # Show the table in a vertical format with a larger height
        st.write("### Editable Score Thresholds")

        editable_df = st.data_editor(
            threshold_df,
            num_rows="dynamic",  # Allows dynamic resizing of rows
            height=600,  # Sets the height of the editor to make it vertical
            use_container_width=True  # Expands to container width
        )

        # Save changes button
        if st.button("Save Changes"):
            for _, row in editable_df.iterrows():
                update_query = f"""
                    UPDATE {db_name}.{schema_name}.SCORE_THRESHOLD
                    SET SCORE_VALUE = {row['SCORE_VALUE']}
                    WHERE SCORE_NAME = '{row['SCORE_NAME']}' AND MODEL_NAME = '{row['MODEL_NAME']}'
                """
                session.sql(update_query).collect()
            st.success("Thresholds updated successfully!")
    else:
        st.info("No thresholds available.")

def metadata_table_selection():
    # Query to fetch data
    query = "SELECT * FROM MODEL_METADATA"
    threshold_df = fetch_table_data(query)

    if not threshold_df.empty:
        # Show the table in a vertical format with a larger height
        st.write("### Editable Model MetaData")

        editable_df = st.data_editor(
            threshold_df,
            num_rows="dynamic",  # Allows dynamic resizing of rows
            height=600,  # Sets the height of the editor to make it vertical
            use_container_width=True  # Expands to container width
        )

        # Save changes button
        if st.button("Save Changes"):
            # Check for changes between original and edited data
            changes_made = False

            for index, row in editable_df.iterrows():
                # Get the original row for comparison
                original_row = threshold_df.loc[index, :]

                # Check if any column value has changed
                if not row.equals(original_row):
                    changes_made = True

                    # Dynamically construct the SET clause for all columns
                    set_clauses = []
                    for col in threshold_df.columns:
                        # Escape values to avoid SQL injection and handle NULL values
                        value = row[col]
                        if pd.isna(value):  # Check for NULL values
                            set_clauses.append(f"{col} = NULL")
                        else:
                            # Check if the value is a string or numeric
                            set_clauses.append(f"{col} = '{value}'" if isinstance(value, str) else f"{col} = {value}")

                    set_clause = ", ".join(set_clauses)

                    # Build the UPDATE query
                    update_query = f"""
                        UPDATE {db_name}.{schema_name}.MODEL_METADATA
                        SET {set_clause}
                        WHERE MODEL_NAME = '{row['MODEL_NAME']}';
                    """
   
                    # Execute the query
                    try:
                        session.sql(update_query).collect()
                    except Exception as e:
                        st.error(f"Failed to update row for MODEL_NAME '{row['MODEL_NAME']}': {str(e)}")

            if changes_made:
                # Commit the changes if necessary
                try:
                    session.sql("COMMIT").collect()
                    st.success("Model Data updated successfully!")
                except Exception as e:
                    st.error(f"Error committing changes: {str(e)}")
            else:
                st.info("No changes detected.")
    else:
        st.info("No Model Data available.")




# Validated Records section
def validated_records_section(form_selection):
    """
    Displays validated records based on the selected form.
    
    Args:
        form_selection (str): The selected form (e.g., "INVOICE MODEL" or "PURCHASED MODEL").
    """
    # Determine the table to query based on form_selection
    if form_selection == "INVOICE MODEL":
        validated_table = tables["invoice"]["validated"]
        stage_name = "INVOICE_DOCS"
    elif form_selection == "PURCHASED MODEL":
        validated_table = tables["purchase"]["validated"]
        stage_name = "INVOICE_DOCS"
    else:
        st.error("Invalid Model selection.")
        return

    # Query validated records from the selected table
    query = f"SELECT * FROM {db_name}.{schema_name}.{validated_table}"
    validated_df = fetch_table_data(query)

    if not validated_df.empty:
        # Select the last 10 rows of the DataFrame
        validated_df = validated_df.tail(10)

        # Display the DataFrame
        st.dataframe(
            validated_df,
            hide_index=True,
            use_container_width=True
        )

        # Get list of unique PDF files from RELATIVEPATH
        if "RELATIVEPATH" in validated_df.columns:
            pdf_files = validated_df['RELATIVEPATH'].unique().tolist()
            
            # Create dropdown for PDF selection
            st.write("### Select a PDF to view")
            selected_file = st.selectbox(
                "Choose a PDF file",
                options=pdf_files,
                index=None,
                placeholder="Select a PDF..."
            )
            
            # Show PDF viewer if a file is selected
            if selected_file:
                try:
                    if "pdf_doc" not in st.session_state or st.session_state.get("current_doc") != selected_file:
                        cleanup_temp_files()
                        if load_pdf(selected_file, stage_name):
                            st.session_state["current_doc"] = selected_file

                    if "pdf_doc" in st.session_state:
                        st.write("### PDF Viewer")
                        display_pdf_page()

                        # Navigation controls
                        nav_container = st.container()
                        nav1, nav2, nav3 = nav_container.columns([2, 2, 1])
                        with nav1:
                            if st.button("⏮️ Previous"):
                                previous_pdf_page()
                        with nav2:
                            total_pages = len(st.session_state["pdf_doc"])
                            current_page = st.session_state["pdf_page"] + 1
                            st.write(f"Page {current_page} of {total_pages}")
                        with nav3:
                            if st.button("Next ⏭️"):
                                next_pdf_page()
                except Exception as e:
                    st.error(f"Error loading PDF: {str(e)}")
        else:
            st.warning("The column 'RELATIVEPATH' does not exist in the data.")
    else:
        st.info(f"No validated records found in the {validated_table} table.")





    # Cleanup when leaving the section
    if selected_tab != "Validated Records":
        cleanup_temp_files()


# Call specific functions for the selected tab
if selected_tab == "Dashboard":
    # Function calls based on form selection
    if form_selection == "INVOICE MODEL":
        I_dashboard_section()  # Call Invoice Dashboard only
    elif form_selection == "PURCHASED MODEL":
        P_dashboard_section()  # Call Purchase Dashboard only
elif selected_tab == "Live view":
    live_view_logic()  # Call Live View
elif selected_tab == "Manual Review":
    manual_review_section()  # Call Manual Review
elif selected_tab == "Score Threshold":
    score_threshold_section()  # Call Score Threshold
elif selected_tab == "Validated Records":
    validated_records_section(form_selection)  # Pass form_selection to the function
elif selected_tab == "Settings":
    metadata_table_selection()  # Call Score Threshold

# Cleanup on session end
if selected_tab != "Manual Review":
    cleanup_temp_files()