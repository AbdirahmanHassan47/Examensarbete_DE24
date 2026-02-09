import pandas as pd
import streamlit as st
import plotly.express as px

from connect_data_warehouse import (
    query_df,
    has_calendar,
    table_has_column,
    service_cte,
    service_join,
)


def _to_df(rows, cols):
    return pd.DataFrame(rows, columns=cols)


def layout():
    st.set_page_config(page_title="SL Metro Dashboard", layout="wide")
    st.title("SL Metro – Dashboard")
    st.write("Dashboarden bygger på GTFS Static (historisk data).")

    cfg = st.secrets["snowflake"]
    db = cfg["database"]
    mart_schema = "MART"
    calendar_schema = "STATIC"

    overview = f"{db}.{mart_schema}.MART_METRO_OVERVIEW"
    station_departures = f"{db}.{mart_schema}.MART_STATION_DEPARTURES"
    line_stats = f"{db}.{mart_schema}.MART_LINE_STATS"
    timetable = f"{db}.{mart_schema}.MART_TIMETABLE"

    use_calendar = has_calendar(db, calendar_schema)
    has_station_col = table_has_column(db, mart_schema, "MART_TIMETABLE", "STATION")

    with st.sidebar:
        st.header("Filter")
        rows, cols = query_df(
            f"""
            select distinct
                coalesce(route_short_name, route_long_name) as line,
                route_long_name as line_name
            from {line_stats}
            order by
                try_to_number(coalesce(route_short_name, route_long_name)),
                line
            """
        )
        lines_df = _to_df(rows, cols)
        lines_df["LABEL"] = lines_df.apply(
            lambda r: f"{r['LINE']} – {r['LINE_NAME']}"
            if r.get("LINE_NAME") and r.get("LINE") else (r.get("LINE_NAME") or r.get("LINE")),
            axis=1,
        )
        line_labels = lines_df["LABEL"].tolist() if not lines_df.empty else []
        line_choice = st.selectbox("Linje", ["All"] + line_labels)

        rows, cols = query_df(
            f"select distinct stop_name as station from {station_departures} order by station"
        )
        stations_df = _to_df(rows, cols)
        stations = stations_df["STATION"].tolist() if not stations_df.empty else []
        station_choice = st.selectbox("Station", ["All"] + stations)

        hour_range = st.slider("Tid (timme)", 0, 23, (6, 23))
        # Restrict selectable dates to calendar range if available
        if use_calendar:
            rows, cols = query_df(
                f"""
                select
                    min(start_date) as min_d,
                    max(end_date) as max_d
                from {db}.{calendar_schema}.METRO_CALENDAR
                """
            )
            cal_df = _to_df(rows, cols)
            if not cal_df.empty and cal_df.loc[0, "MIN_D"] is not None:
                min_d = pd.to_datetime(cal_df.loc[0, "MIN_D"], format="%Y%m%d").date()
                max_d = pd.to_datetime(cal_df.loc[0, "MAX_D"], format="%Y%m%d").date()
                selected_date = st.date_input("Datum", min_value=min_d, max_value=max_d)
            else:
                selected_date = st.date_input("Datum")
        else:
            selected_date = st.date_input("Datum")

    date_str = selected_date.strftime("%Y%m%d")
    day_cols = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
    day_col = day_cols[selected_date.weekday()]

    filters = []
    line_value = None
    if line_choice != "All":
        line_value = line_choice.split(" – ", 1)[0]
        filters.append(f"t.line = '{line_value}'")
    if has_station_col and station_choice != "All":
        filters.append(f"t.station = '{station_choice}'")
    filters.append(
        f"date_part('hour', try_to_time(t.departure_time)) between {hour_range[0]} and {hour_range[1]}"
    )
    where_sql = "where " + " and ".join(filters) if filters else ""

    tabs = st.tabs(["Översikt", "Stationer", "Linjer", "Tidtabell"])

    with tabs[0]:
        st.markdown("## Översikt")
        if not use_calendar:
            st.info("Kalendertabeller saknas. Visar totalsiffror för hela datasetet.")

        rows, cols = query_df(
            f"""
            {service_cte(db, calendar_schema, date_str, day_col) if use_calendar else ""}
            select
                count(distinct t.route_id) as routes,
                count(distinct t.trip_id) as trips,
                count(*) as stop_times,
                count(distinct t.stop_id) as stops
            from {timetable} t
            {service_join(use_calendar)}
            {where_sql}
            """
        )
        df = _to_df(rows, cols)
        if not df.empty:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Linjer", int(df.loc[0, "ROUTES"]))
            c2.metric("Turer", int(df.loc[0, "TRIPS"]))
            c3.metric("Avgångar", int(df.loc[0, "STOP_TIMES"]))
            c4.metric("Stationer", int(df.loc[0, "STOPS"]))
        else:
            st.warning("Ingen data för valt datum. Välj ett datum inom kalenderns giltighet.")

        st.markdown("### Mest belastade stationer")
        rows, cols = query_df(
            f"""
            select stop_name as station, planned_departures
            from {station_departures}
            {"where " if (station_choice != "All" or line_value) else ""}
            {("stop_name = '" + station_choice + "'") if station_choice != "All" else ""}
            {(" and " if (station_choice != "All" and line_value) else "")}
            {("coalesce(route_short_name, route_long_name) = '" + line_value + "'") if line_value else ""}
            order by planned_departures desc
            limit 15
            """
        )
        st.dataframe(_to_df(rows, cols), width="stretch")

        st.markdown("### Planerad trafik per timme")
        rows, cols = query_df(
            f"""
            {service_cte(db, calendar_schema, date_str, day_col) if use_calendar else ""}
            select
                date_part('hour', try_to_time(t.departure_time)) as hour_of_day,
                count(*) as planned_departures
            from {timetable} t
            {service_join(use_calendar)}
            where try_to_time(t.departure_time) is not null
              and date_part('hour', try_to_time(t.departure_time)) between {hour_range[0]} and {hour_range[1]}
              {"and t.line = '" + line_value + "'" if line_value else ""}
              {("and t.station = '" + station_choice + "'") if (has_station_col and station_choice != "All") else ""}
            group by hour_of_day
            order by hour_of_day
            """
        )
        hour_df = _to_df(rows, cols)
        if not hour_df.empty:
            fig = px.line(hour_df, x="HOUR_OF_DAY", y="PLANNED_DEPARTURES", markers=True)
            st.plotly_chart(fig, width="stretch")

    with tabs[1]:
        st.markdown("## Stationer")
        rows, cols = query_df(
            f"""
            select stop_name as station, planned_departures
            from {station_departures}
            {"where " if (station_choice != "All" or line_value) else ""}
            {("stop_name = '" + station_choice + "'") if station_choice != "All" else ""}
            {(" and " if (station_choice != "All" and line_value) else "")}
            {("coalesce(route_short_name, route_long_name) = '" + line_value + "'") if line_value else ""}
            order by planned_departures desc
            """
        )
        st.dataframe(_to_df(rows, cols), width="stretch")

        st.markdown("### Förseningsanalys")
        st.info("Realtidsförseningar ingår inte enligt avgränsningarna (endast historisk data).")

    with tabs[2]:
        st.markdown("## Linjer")
        rows, cols = query_df(
            f"""
            select
                coalesce(route_short_name, route_long_name) as line,
                stops_count as stations_count,
                stop_times_count as planned_departures
            from {line_stats}
            order by planned_departures desc
            """
        )
        line_stats_df = _to_df(rows, cols)
        if not line_stats_df.empty:
            fig = px.bar(
                line_stats_df,
                x="LINE",
                y="PLANNED_DEPARTURES",
                color="LINE",
                title=None,
            )
            st.plotly_chart(fig, width="stretch")
            st.dataframe(line_stats_df, width="stretch")

        st.markdown("### Förseningsanalys")
        st.info("Realtidsförseningar ingår inte enligt avgränsningarna (endast historisk data).")

    with tabs[3]:
        st.markdown("## Tidtabell")
        if not has_station_col:
            st.warning(
                "MART_TIMETABLE saknar kolumnen STATION. Kör:\n"
                "dbt run --select mart_timetable --full-refresh"
            )
            return
        rows, cols = query_df(
            f"""
            {service_cte(db, calendar_schema, date_str, day_col) if use_calendar else ""}
            select
                line,
                station,
                departure_time as planned_departure,
                arrival_time as planned_arrival,
                stop_sequence
            from {timetable} t
            {service_join(use_calendar)}
            {where_sql}
            order by line, station, try_to_time(departure_time)
            limit 200
            """
        )
        st.dataframe(_to_df(rows, cols), width="stretch")


if __name__ == "__main__":
    layout()
