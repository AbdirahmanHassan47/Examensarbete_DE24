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
                route_short_name as line,
                route_long_name as line_name,
                try_to_number(route_short_name) as line_number,
                case
                    when lower(route_long_name) like '%röda%' then 'Red'
                    when lower(route_long_name) like '%gröna%' then 'Green'
                    when lower(route_long_name) like '%blå%' then 'Blue'
                    else null
                end as line_color
            from {line_stats}
            order by line_number, line
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

        if has_station_col:
            rows, cols = query_df(
                f"select distinct station from {timetable} order by 1"
            )
        else:
            rows, cols = query_df(
                f"select distinct stop_name as station from {station_departures} order by 1"
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

    tabs = st.tabs(["Översikt", "Stationer", "Linjer"])

    with tabs[0]:
        st.markdown("## Översikt")
        if not use_calendar:
            st.info("Kalendertabeller saknas. Visar totalsiffror för hela datasetet.")
        else:
            st.caption("Datumfiltret gäller hela dygnet (00–23) för det valda datumet.")
        st.caption(
            "Planned departures = planerade avgångar enligt tidtabellen (GTFS static), "
            "inte faktiska avgångar."
        )

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
        st.caption(
            "Stationer kan förekomma flera gånger i rådata (flera avgångar). "
            "Här summeras de till totalt antal planerade avgångar."
        )
        rows, cols = query_df(
            f"""
            {service_cte(db, calendar_schema, date_str, day_col) if use_calendar else ""}
            select
                t.station as station,
                case
                    when lower(t.line_name) like '%röda%' then 'Red'
                    when lower(t.line_name) like '%gröna%' then 'Green'
                    when lower(t.line_name) like '%blå%' then 'Blue'
                    else null
                end as line_color,
                count(*) as planned_departures
            from {timetable} t
            {service_join(use_calendar)}
            {where_sql}
            group by t.station, line_color
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
            {service_cte(db, calendar_schema, date_str, day_col) if use_calendar else ""}
            select
                t.station as station,
                case
                    when lower(t.line_name) like '%röda%' then 'Red'
                    when lower(t.line_name) like '%gröna%' then 'Green'
                    when lower(t.line_name) like '%blå%' then 'Blue'
                    else null
                end as line_color,
                count(*) as planned_departures
            from {timetable} t
            {service_join(use_calendar)}
            {where_sql}
            group by t.station, line_color
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
            {service_cte(db, calendar_schema, date_str, day_col) if use_calendar else ""}
            select
                t.line as line,
                case
                    when lower(t.line_name) like '%röda%' then 'Red'
                    when lower(t.line_name) like '%gröna%' then 'Green'
                    when lower(t.line_name) like '%blå%' then 'Blue'
                    else null
                end as line_color,
                count(distinct t.station) as stations_count,
                count(*) as planned_departures
            from {timetable} t
            {service_join(use_calendar)}
            {where_sql}
            group by t.line, line_color
            order by planned_departures desc
            """
        )
        line_stats_df = _to_df(rows, cols)
        if not line_stats_df.empty:
            color_map = {"Red": "#D11F2F", "Green": "#00985F", "Blue": "#0069B4"}
            fig = px.bar(
                line_stats_df,
                x="LINE",
                y="PLANNED_DEPARTURES",
                color="LINE_COLOR",
                color_discrete_map=color_map,
                title=None,
            )
            st.plotly_chart(fig, width="stretch")
            st.dataframe(line_stats_df, width="stretch")

        st.markdown("### Förseningsanalys")
        st.info("Realtidsförseningar ingår inte enligt avgränsningarna (endast historisk data).")

if __name__ == "__main__":
    layout()
