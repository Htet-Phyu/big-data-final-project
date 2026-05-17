import streamlit as st
import pandas as pd
import glob
import time

st.set_page_config(page_title="Big Data Dashboard", layout="wide")

st.title("Wikimedia Real-Time Analytics Dashboard")
st.caption("Updating every 1 second | Includes Spark SQL static dataset join bonus")

def read_csv_folder(path):
    files = glob.glob(f"{path}/part-*.csv")
    if not files:
        return None

    dfs = []
    for file in files:
        try:
            dfs.append(pd.read_csv(file))
        except Exception:
            pass

    if not dfs:
        return None

    return pd.concat(dfs, ignore_index=True)

placeholder = st.empty()

while True:
    wiki_df = read_csv_folder("dashboard_data/wiki_counts")
    type_df = read_csv_folder("dashboard_data/type_counts")
    user_df = read_csv_folder("dashboard_data/top_users")
    activity_df = read_csv_folder("dashboard_data/wiki_activity")
    language_df = read_csv_folder("dashboard_data/language_counts")
    category_df = read_csv_folder("dashboard_data/category_counts")

    with placeholder.container():
        if wiki_df is None:
            st.warning("No dashboard data yet. Make sure Spark and producer are running.")
        else:
            col1, col2 = st.columns(2)

            with col1:
                if user_df is not None:
                    st.subheader("Top Active Users")
                    st.dataframe(user_df, use_container_width=True)

            with col2:
                if activity_df is not None:
                    st.subheader("Wiki Activity Status")
                    st.dataframe(activity_df, use_container_width=True)

            st.subheader("Bonus: Events by Language Searching the language from static (csv file) and joining with dynamic real time stream data")
            if language_df is not None:
                st.dataframe(language_df, use_container_width=True)
                st.bar_chart(language_df.set_index("language"))

            st.subheader("Bonus: Events by Category")
            if category_df is not None:
                st.dataframe(category_df, use_container_width=True)
                st.bar_chart(category_df.set_index("category"))

            st.subheader("Events by Wiki, Language, Category, and Actor Type")
            st.dataframe(wiki_df, use_container_width=True)
            st.bar_chart(wiki_df.groupby("wiki")["event_count"].sum())

            if type_df is not None:
                st.subheader("Events by Type")
                st.dataframe(type_df, use_container_width=True)
                st.bar_chart(type_df.set_index("type"))

    time.sleep(1)