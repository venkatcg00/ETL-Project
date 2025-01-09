import pandas as pd
from sqlalchemy.sql import text
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go


def load_html_template(file_path):
    with open(file_path, "r") as file:
        return file.read()


def data_information(session, st, px, go):
    result = session.execute(
        text(
            "SELECT B.SOURCE_NAME, A.* FROM CSD_DATA_MART A JOIN CSD_SOURCES B ON A.SOURCE_ID = B.SOURCE_ID WHERE A.ACTIVE_FLAG = 1"
        )
    )
    rows = result.fetchall()
    columns = result.keys()

    # Convert rows into a pandas DataFrame
    df = pd.DataFrame(rows, columns=columns)

    df["valid"] = df["IS_VALID_DATA"].apply(lambda x: "Valid" if x == 1 else "Invalid")

    # Calculate statistics for each source
    source_stats = (
        df.groupby("SOURCE_NAME")
        .agg(
            total_records=("SOURCE_NAME", "size"),
            valid_records=("valid", lambda x: (x == "Valid").sum()),
            invalid_records=("valid", lambda x: (x == "Invalid").sum()),
        )
        .reset_index()
    )

    # Calculate valid percentage
    source_stats["valid_percentage"] = (
        source_stats["valid_records"] / source_stats["total_records"]
    ) * 100

    # Display Total Records
    total_records = df.shape[0]
    valid_records = source_stats["valid_records"].sum()
    invalid_records = source_stats["invalid_records"].sum()
    valid_percentage = (valid_records / total_records) * 100

    # Load and render the HTML template for total records
    html_template = load_html_template("cards.html")
    source_options = "".join(
        [
            f'<option value="{source}">{source}</option>'
            for source in source_stats["SOURCE_NAME"]
        ]
    )
    rendered_html = html_template.replace("{{ total_records }}", str(total_records))
    rendered_html = rendered_html.replace("{{ selected_source }}", "None")
    rendered_html = rendered_html.replace("{{ valid_records }}", str(valid_records))
    rendered_html = rendered_html.replace("{{ invalid_records }}", str(invalid_records))
    rendered_html = rendered_html.replace(
        "{{ valid_percentage }}", f"{valid_percentage:.2f}"
    )
    rendered_html = rendered_html.replace("{{ records }}", str(total_records))

    st.markdown(rendered_html, unsafe_allow_html=True)

    # Handle source selection
    source_selected = st.selectbox(
        "Select a source", [""] + list(source_stats["SOURCE_NAME"])
    )

    if source_selected:
        source_data = source_stats[source_stats["SOURCE_NAME"] == source_selected]

        # Display the selected source's statistics in cards
        source_total_records = source_data["total_records"].values[0]
        source_valid_records = source_data["valid_records"].values[0]
        source_invalid_records = source_data["invalid_records"].values[0]
        source_valid_percentage = source_data["valid_percentage"].values[0]

        rendered_html = load_html_template("cards.html")
        rendered_html = rendered_html.replace("{{ total_records }}", str(total_records))
        rendered_html = rendered_html.replace("{{ selected_source }}", source_selected)
        rendered_html = rendered_html.replace(
            "{{ valid_records }}", str(source_valid_records)
        )
        rendered_html = rendered_html.replace(
            "{{ invalid_records }}", str(source_invalid_records)
        )
        rendered_html = rendered_html.replace(
            "{{ valid_percentage }}", f"{source_valid_percentage:.2f}"
        )
        rendered_html = rendered_html.replace(
            "{{ records }}", str(source_total_records)
        )

        st.markdown(rendered_html, unsafe_allow_html=True)
    else:
        rendered_html = load_html_template("cards.html")
        rendered_html = rendered_html.replace("{{ total_records }}", str(total_records))
        rendered_html = rendered_html.replace("{{ selected_source }}", "None")
        rendered_html = rendered_html.replace("{{ valid_records }}", str(valid_records))
        rendered_html = rendered_html.replace(
            "{{ invalid_records }}", str(invalid_records)
        )
        rendered_html = rendered_html.replace(
            "{{ valid_percentage }}", f"{valid_percentage:.2f}"
        )
        rendered_html = rendered_html.replace("{{ records }}", str(total_records))

        st.markdown(rendered_html, unsafe_allow_html=True)

    # Modernize the pie chart for record counts
    pie_chart = px.pie(
        source_stats,
        values="total_records",
        names="SOURCE_NAME",
        title="Record Count by Source",
        color_discrete_sequence=px.colors.sequential.RdBu,
    )
    pie_chart.update_traces(hoverinfo="label+percent", textinfo="value")
    pie_chart.update_layout(
        clickmode="event+select",
        template="plotly_white",
        font=dict(family="Roboto, sans-serif", size=14),
        margin=dict(t=30, b=0, l=0, r=0),
    )

    # Modernize the bar chart with different colors for valid and invalid records
    bar_chart = go.Figure(
        data=[
            go.Bar(
                name="Valid Records",
                x=source_stats["SOURCE_NAME"],
                y=source_stats["valid_records"],
                marker_color="green",
            ),
            go.Bar(
                name="Invalid Records",
                x=source_stats["SOURCE_NAME"],
                y=source_stats["invalid_records"],
                marker_color="red",
            ),
        ]
    )
    bar_chart.update_layout(
        barmode="stack",
        title="Valid and Invalid Records by Source",
        clickmode="event+select",
        template="plotly_white",
        font=dict(family="Roboto, sans-serif", size=14),
        margin=dict(t=30, b=0, l=0, r=0),
    )

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(pie_chart, use_container_width=True)
    with col2:
        st.plotly_chart(bar_chart, use_container_width=True)
