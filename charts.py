import os
import plotly.graph_objects as go
import pandas as pd


def plot_energy_data(column_name="Consumption"):
    """
    Generates an interactive plotly chart for energy meter data

    Args:
        column_name (str): The column name from meter_data.csv file

    Saves:
        {column_name}.html file of the energy data chart in current directory
    """

    script_dir = os.path.dirname(os.path.realpath(__file__))
    meter_data_file_path = os.path.join(script_dir, "meter_data.csv")
    html_file_path = os.path.join(script_dir, f"{column_name}.html")

    # Get data
    data = pd.read_csv(meter_data_file_path) 
    data["date"] = pd.to_datetime(data["date"])

    # Convert from watts to kilowatts
    data[column_name] = data[column_name] / 1000

    # Define the time ranges
    today_start = pd.Timestamp.now().normalize()
    today_end = today_start + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    day_data = data[(data["date"] >= today_start) & (data["date"] <= today_end)]
    
    # Aggregating for less granular views
    week_data = data.groupby(data["date"].dt.floor("1h")).agg({column_name: "sum"}).reset_index()
    month_data = data.groupby(data["date"].dt.floor("1D")).agg({column_name: "sum"}).reset_index()
    six_months_data = data.groupby(data["date"].dt.floor("1D")).agg({column_name: "sum"}).reset_index()
    year_data = data.groupby(data["date"].dt.to_period("W").dt.start_time).agg({column_name: "sum"}).reset_index()

    # Create a figure with all traces (one for each time range)
    fig = go.Figure()

    # Add traces for each time range
    ranges = {
        "Day": day_data,
        "Week": week_data,
        "Month": month_data,
        "6 Months": six_months_data,
        "Year": year_data
    }

    for label, range_data in ranges.items():
        fig.add_trace(go.Scatter(
            x=range_data["date"],
            y=range_data[column_name],
            mode="lines+markers",
            name=label,
            visible=(label == "Day")  # Show only the 'Day' trace initially
        ))

    # Add buttons to toggle visibility and adjust x-axis range
    buttons = []
    for i, label in enumerate(ranges.keys()):
        # Determine x-axis range for each time period
        if label == "Day":
            xaxis_range = [today_start, today_end]
        elif label == "Week":
            week_start = pd.Timestamp.now() - pd.Timedelta(weeks=1)
            week_end = pd.Timestamp.now()
            xaxis_range = [week_start, week_end]
        elif label == "Month":
            month_start = pd.Timestamp.now() - pd.Timedelta(days=30)
            month_end = pd.Timestamp.now()
            xaxis_range = [month_start, month_end]
        elif label == "6 Months":
            six_months_start = pd.Timestamp.now() - pd.Timedelta(days=6 * 30)
            six_months_end = pd.Timestamp.now()
            xaxis_range = [six_months_start, six_months_end]
        elif label == "Year":
            year_start = pd.Timestamp.now() - pd.Timedelta(days=365)
            year_end = pd.Timestamp.now()
            xaxis_range = [year_start, year_end]

        # Add button for each time range
        buttons.append(dict(
            label=label,
            method="update",
            args = [
                {"visible": [j == i for j in range(len(ranges))]},  # Toggle visibility
                {"xaxis": {"range": xaxis_range}}  # Adjust x-axis range
            ]
        ))

    # Update layout
    fig.update_layout(
        updatemenus=[
            dict(
                type="buttons",
                buttons=buttons,
                direction="left",
                showactive=True,
                x=0.5,
                xanchor="center",
                y=1.15,
                yanchor="top"
            )
        ],
        xaxis=dict(
            title="Date",
            range=[today_start, today_end]  # Default range for 'Day'
        ),
        yaxis=dict(title=f"{column_name} (kWh)"),
        template="plotly_white"
    )

    # # Show figure
    # fig.show()

    # Save the figure as html file
    fig.write_html(html_file_path)


def update_html_plots():
    """
    Updates the html plots by calling plot_energy_data

    Args:
        None

    Returns:
        None
    """
    plot_energy_data("Consumption")
    plot_energy_data("Production")
