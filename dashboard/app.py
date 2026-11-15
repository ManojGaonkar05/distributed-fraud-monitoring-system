"""Streamlit investigation dashboard backed by Spark Thrift Server/Iceberg."""
from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from pyhive import hive

load_dotenv()
st.set_page_config(page_title="Fraud Monitoring", page_icon="🛡️", layout="wide")
st.title("🛡️ Distributed Fraud Monitoring")
st.caption("Live decisions and account risk alerts from the Iceberg run tables.")


@st.cache_data(ttl=20, show_spinner="Loading Iceberg tables...")
def load_tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    host = os.getenv("SPARK_THRIFT_HOST", "vm4")
    port = int(os.getenv("SPARK_THRIFT_PORT", "10000"))
    user = os.getenv("SPARK_THRIFT_USER", os.getenv("USER", "streamlit"))
    decisions = os.getenv("TRANSACTION_TABLE", f"lab.fraud.transaction_decisions_{os.getenv('RUN_ID','run1')}")
    alerts = os.getenv("ALERT_TABLE", f"lab.fraud.account_risk_alerts_{os.getenv('RUN_ID','run1')}")
    connection = hive.Connection(host=host, port=port, username=user, auth="NONE", database="default")
    try:
        def query(table: str) -> pd.DataFrame:
            cursor = connection.cursor()
            try:
                cursor.execute(f"SELECT * FROM {table}")
                names = [item[0].split(".")[-1] for item in cursor.description]
                return pd.DataFrame(cursor.fetchall(), columns=names)
            finally:
                cursor.close()
        return query(decisions), query(alerts)
    finally:
        connection.close()


try:
    decisions, alerts = load_tables()
    if not decisions.empty:
        decisions["event_timestamp"] = pd.to_datetime(decisions.event_timestamp, errors="coerce")
    if not alerts.empty:
        alerts["window_start"] = pd.to_datetime(alerts.window_start, errors="coerce")
    if decisions.empty and alerts.empty:
        st.info("The selected run has no output yet. Confirm the streaming application is running and has passed its event-time windows.")
        st.stop()
except Exception as exc:
    st.error(f"Could not load the Iceberg tables through Spark Thrift Server. Check SPARK_THRIFT_HOST/PORT, table names, and the Thrift Server logs. Details: {exc}")
    st.stop()

with st.sidebar:
    st.header("Filters")
    available_ts = decisions.event_timestamp.dropna() if not decisions.empty else pd.Series(dtype="datetime64[ns]")
    if len(available_ts):
        minimum, maximum = available_ts.min().date(), available_ts.max().date()
    else:
        minimum = maximum = date.today()
    selected_dates = st.date_input("Event date range", value=(minimum, maximum), min_value=minimum, max_value=maximum)
    accounts = sorted(set(decisions.source_account.dropna().astype(str)) if not decisions.empty else set())
    account = st.selectbox("Source account", ["All accounts", *accounts])
    rules = sorted(alerts.rule_id.dropna().astype(str).unique()) if not alerts.empty else []
    rule = st.selectbox("Alert rule", ["All rules", *rules])
    if st.button("Refresh data"):
        load_tables.clear()
        st.rerun()

filtered_decisions = decisions.copy()
filtered_alerts = alerts.copy()
if len(selected_dates) == 2:
    start = pd.Timestamp(selected_dates[0])
    end = pd.Timestamp(selected_dates[1]) + pd.Timedelta(days=1)
    if not filtered_decisions.empty:
        filtered_decisions = filtered_decisions[filtered_decisions.event_timestamp.between(start, end, inclusive="left")]
    if not filtered_alerts.empty:
        filtered_alerts = filtered_alerts[filtered_alerts.window_start.between(start, end, inclusive="left")]
if account != "All accounts":
    filtered_decisions = filtered_decisions[filtered_decisions.source_account.astype(str) == account]
    filtered_alerts = filtered_alerts[filtered_alerts.source_account.astype(str) == account]
if rule != "All rules":
    filtered_alerts = filtered_alerts[filtered_alerts.rule_id.astype(str) == rule]

flagged_accounts = filtered_alerts.source_account.nunique() if not filtered_alerts.empty else 0
k1, k2, k3 = st.columns(3)
k1.metric("Transactions", f"{len(filtered_decisions):,}")
k2.metric("Fraud alerts", f"{len(filtered_alerts):,}")
k3.metric("Flagged accounts", f"{flagged_accounts:,}")

left, right = st.columns(2)
with left:
    st.subheader("Alerts by rule")
    if filtered_alerts.empty:
        st.info("No alerts match the selected filters.")
    else:
        st.bar_chart(filtered_alerts.groupby("rule_id").size().rename("alerts"))
with right:
    st.subheader("Transaction volume per minute")
    if filtered_decisions.empty:
        st.info("No transactions match the selected filters.")
    else:
        per_minute = filtered_decisions.set_index("event_timestamp").resample("1min").size().rename("transactions")
        st.line_chart(per_minute)

left, right = st.columns(2)
with left:
    st.subheader("Alert volume per minute")
    if filtered_alerts.empty:
        st.info("No alert volume to chart.")
    else:
        st.line_chart(filtered_alerts.set_index("window_start").resample("1min").size().rename("alerts"))
with right:
    st.subheader("Flagged accounts")
    if filtered_alerts.empty:
        st.info("No flagged accounts in this range.")
    else:
        account_summary = filtered_alerts.groupby("source_account").agg(alerts=("alert_id", "count"), rules=("rule_id", lambda s: ", ".join(sorted(set(s))))).sort_values("alerts", ascending=False)
        st.dataframe(account_summary, use_container_width=True)

st.subheader("Transaction decisions")
st.dataframe(filtered_decisions.sort_values("event_timestamp", ascending=False), use_container_width=True, hide_index=True)
st.subheader("Alert details")
st.dataframe(filtered_alerts.sort_values("window_start", ascending=False), use_container_width=True, hide_index=True)
if not filtered_alerts.empty:
    st.subheader("Investigate an alert")
    alert_ids = filtered_alerts.alert_id.astype(str).tolist()
    selected_alert_id = st.selectbox("Alert", alert_ids)
    selected = filtered_alerts[filtered_alerts.alert_id.astype(str) == selected_alert_id].iloc[0]
    st.write(f"**Explanation:** {selected.get('explanation', 'No explanation stored.')}")
    related_id = selected.get("trigger_transaction_id")
    related = decisions[decisions.transaction_id.astype(str) == str(related_id)] if not decisions.empty else pd.DataFrame()
    st.write("**Related transaction**")
    st.dataframe(related, use_container_width=True, hide_index=True)
    account_rows = decisions[decisions.source_account.astype(str) == str(selected.source_account)].sort_values("event_timestamp", ascending=False).head(20)
    st.write(f"**Last 20 transactions for account {selected.source_account}**")
    st.dataframe(account_rows, use_container_width=True, hide_index=True)
