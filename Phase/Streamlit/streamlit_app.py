import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Global Partners Business Insights", layout="wide")

# -----------------------------
# CONFIG
# -----------------------------
ATHENA_S3_STAGING = "s3://globalpartner-athena-results/"
AWS_REGION = "us-east-1"

# Replace with your values if needed
# You can also switch to PyAthena if that's what you're using
ATHENA_CONN_STR = (
    f"awsathena+rest://@athena.{AWS_REGION}.amazonaws.com:443/"
    f"globalpartners_gold?s3_staging_dir={ATHENA_S3_STAGING}&work_group=primary"
)

# -----------------------------
# CONNECTION
# -----------------------------
@st.cache_resource
def get_engine():
    return create_engine(ATHENA_CONN_STR)

@st.cache_data(ttl=600)
def load_data(query: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(query, engine)

# -----------------------------
# LOAD DATA
# -----------------------------
sales_df = load_data("SELECT * FROM globalpartners_gold.sales")
clv_df = load_data("SELECT * FROM globalpartners_gold.daily_clv")
rfm_df = load_data("SELECT * FROM globalpartners_gold.rfm")
churn_df = load_data("SELECT * FROM globalpartners_gold.churn")
loyalty_df = load_data("SELECT * FROM globalpartners_gold.loyalty")
location_df = load_data("SELECT * FROM globalpartners_gold.location_performance")
discounts_df = load_data("SELECT * FROM globalpartners_gold.discounts")

# -----------------------------
# CLEAN TYPES
# -----------------------------
for col in ["order_date"]:
    if col in sales_df.columns:
        sales_df[col] = pd.to_datetime(sales_df[col])
    if col in location_df.columns:
        location_df[col] = pd.to_datetime(location_df[col])
    if col in discounts_df.columns:
        discounts_df[col] = pd.to_datetime(discounts_df[col])

for col in ["first_order_ts", "last_order_ts"]:
    if col in clv_df.columns:
        clv_df[col] = pd.to_datetime(clv_df[col], errors="coerce")

for col in ["last_order_ts", "last_order_date"]:
    if col in churn_df.columns:
        churn_df[col] = pd.to_datetime(churn_df[col], errors="coerce")

# -----------------------------
# SIDEBAR FILTERS
# -----------------------------
st.sidebar.title("Filters")

page = st.sidebar.radio(
    "Go to",
    [
        "Executive Overview",
        "Sales Trends",
        "Customer Lifetime Value",
        "RFM Segmentation",
        "Churn Risk",
        "Loyalty & Location",
        "Discount Impact",
    ],
)

# Global date filter where applicable
min_date = None
max_date = None
if not sales_df.empty:
    min_date = sales_df["order_date"].min().date()
    max_date = sales_df["order_date"].max().date()

if min_date and max_date:
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if len(date_range) == 2:
        start_date, end_date = date_range
        sales_filtered = sales_df[
            (sales_df["order_date"].dt.date >= start_date)
            & (sales_df["order_date"].dt.date <= end_date)
        ].copy()
        location_filtered = location_df[
            (location_df["order_date"].dt.date >= start_date)
            & (location_df["order_date"].dt.date <= end_date)
        ].copy()
        discounts_filtered = discounts_df[
            (discounts_df["order_date"].dt.date >= start_date)
            & (discounts_df["order_date"].dt.date <= end_date)
        ].copy()
    else:
        sales_filtered = sales_df.copy()
        location_filtered = location_df.copy()
        discounts_filtered = discounts_df.copy()
else:
    sales_filtered = sales_df.copy()
    location_filtered = location_df.copy()
    discounts_filtered = discounts_df.copy()

# -----------------------------
# EXECUTIVE OVERVIEW
# -----------------------------
if page == "Executive Overview":
    st.title("Global Partners Business Insights Dashboard")

    total_revenue = sales_filtered["total_revenue"].sum() if not sales_filtered.empty else 0
    total_orders = sales_filtered["total_orders"].sum() if not sales_filtered.empty else 0
    unique_customers = clv_df["user_id"].nunique() if not clv_df.empty else 0
    at_risk_customers = (
        churn_df[churn_df["churn_status"] == "At Risk"].shape[0] if not churn_df.empty else 0
    )
    vip_customers = (
        rfm_df[rfm_df["rfm_segment"] == "VIP"].shape[0] if not rfm_df.empty else 0
    )

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Revenue", f"${total_revenue:,.2f}")
    c2.metric("Total Orders", f"{int(total_orders):,}")
    c3.metric("Unique Customers", f"{unique_customers:,}")
    c4.metric("At Risk Customers", f"{at_risk_customers:,}")
    c5.metric("VIP Customers", f"{vip_customers:,}")

    left, right = st.columns(2)

    if not sales_filtered.empty:
        daily_rev = sales_filtered.groupby("order_date", as_index=False)["total_revenue"].sum()
        fig = px.line(daily_rev, x="order_date", y="total_revenue", title="Revenue Over Time")
        left.plotly_chart(fig, use_container_width=True)

    if not rfm_df.empty:
        seg = rfm_df["rfm_segment"].value_counts().reset_index()
        seg.columns = ["rfm_segment", "count"]
        fig = px.bar(seg, x="rfm_segment", y="count", title="RFM Segment Distribution")
        right.plotly_chart(fig, use_container_width=True)

    left2, right2 = st.columns(2)

    if not churn_df.empty:
        churn_counts = churn_df["churn_status"].value_counts().reset_index()
        churn_counts.columns = ["churn_status", "count"]
        fig = px.pie(churn_counts, names="churn_status", values="count", title="Churn Status")
        left2.plotly_chart(fig, use_container_width=True)

    if not location_filtered.empty:
        top_locations = (
            location_filtered.groupby("restaurant_id", as_index=False)["total_revenue"]
            .sum()
            .sort_values("total_revenue", ascending=False)
            .head(10)
        )
        fig = px.bar(
            top_locations,
            x="restaurant_id",
            y="total_revenue",
            title="Top 10 Locations by Revenue"
        )
        right2.plotly_chart(fig, use_container_width=True)

# -----------------------------
# SALES TRENDS
# -----------------------------
elif page == "Sales Trends":
    st.title("Sales Trends and Seasonality")

    if sales_filtered.empty:
        st.warning("No sales data available for the selected filters.")
    else:
        col1, col2 = st.columns(2)

        daily = sales_filtered.groupby("order_date", as_index=False)["total_revenue"].sum()
        fig1 = px.line(daily, x="order_date", y="total_revenue", title="Daily Revenue")
        col1.plotly_chart(fig1, use_container_width=True)

        monthly = sales_filtered.copy()
        monthly["month_label"] = monthly["order_date"].dt.to_period("M").astype(str)
        monthly = monthly.groupby("month_label", as_index=False)["total_revenue"].sum()
        fig2 = px.bar(monthly, x="month_label", y="total_revenue", title="Monthly Revenue")
        col2.plotly_chart(fig2, use_container_width=True)

        col3, col4 = st.columns(2)

        by_category = (
            sales_filtered.groupby("item_category", as_index=False)["total_revenue"]
            .sum()
            .sort_values("total_revenue", ascending=False)
            .head(15)
        )
        fig3 = px.bar(by_category, x="item_category", y="total_revenue", title="Revenue by Category")
        col3.plotly_chart(fig3, use_container_width=True)

        by_location = (
            sales_filtered.groupby("restaurant_id", as_index=False)["total_revenue"]
            .sum()
            .sort_values("total_revenue", ascending=False)
            .head(15)
        )
        fig4 = px.bar(by_location, x="restaurant_id", y="total_revenue", title="Revenue by Location")
        col4.plotly_chart(fig4, use_container_width=True)

# -----------------------------
# CLV
# -----------------------------
elif page == "Customer Lifetime Value":
    st.title("Customer Lifetime Value")

    if clv_df.empty:
        st.warning("No CLV data available.")
    else:
        col1, col2 = st.columns(2)

        clv_counts = clv_df["clv_segment"].value_counts().reset_index()
        clv_counts.columns = ["clv_segment", "count"]
        fig1 = px.pie(clv_counts, names="clv_segment", values="count", title="CLV Segments")
        col1.plotly_chart(fig1, use_container_width=True)

        top_clv = clv_df.sort_values("total_spent", ascending=False).head(20)
        fig2 = px.bar(top_clv, x="user_id", y="total_spent", color="clv_segment", title="Top 20 Customers by Spend")
        col2.plotly_chart(fig2, use_container_width=True)

        fig3 = px.scatter(
            clv_df,
            x="total_orders",
            y="total_spent",
            color="clv_segment",
            title="CLV: Orders vs Spend",
            hover_data=["user_id", "avg_order_value"]
        )
        st.plotly_chart(fig3, use_container_width=True)

        st.dataframe(
            clv_df.sort_values("total_spent", ascending=False).head(20),
            use_container_width=True
        )

# -----------------------------
# RFM
# -----------------------------
elif page == "RFM Segmentation":
    st.title("RFM Segmentation")

    if rfm_df.empty:
        st.warning("No RFM data available.")
    else:
        col1, col2 = st.columns(2)

        rfm_counts = rfm_df["rfm_segment"].value_counts().reset_index()
        rfm_counts.columns = ["rfm_segment", "count"]
        fig1 = px.bar(rfm_counts, x="rfm_segment", y="count", title="RFM Segment Counts")
        col1.plotly_chart(fig1, use_container_width=True)

        fig2 = px.scatter(
            rfm_df,
            x="frequency_orders",
            y="monetary_value",
            color="rfm_segment",
            title="Frequency vs Monetary",
            hover_data=["user_id", "recency_days"]
        )
        col2.plotly_chart(fig2, use_container_width=True)

        st.subheader("Top VIP Customers")
        vip_df = rfm_df[rfm_df["rfm_segment"] == "VIP"].sort_values("monetary_value", ascending=False).head(20)
        st.dataframe(vip_df, use_container_width=True)

# -----------------------------
# CHURN
# -----------------------------
elif page == "Churn Risk":
    st.title("Churn Risk Indicators")

    if churn_df.empty:
        st.warning("No churn data available.")
    else:
        col1, col2 = st.columns(2)

        churn_counts = churn_df["churn_status"].value_counts().reset_index()
        churn_counts.columns = ["churn_status", "count"]
        fig1 = px.pie(churn_counts, names="churn_status", values="count", title="Churn Status")
        col1.plotly_chart(fig1, use_container_width=True)

        fig2 = px.histogram(churn_df, x="days_since_last_order", nbins=40, title="Days Since Last Order")
        col2.plotly_chart(fig2, use_container_width=True)

        fig3 = px.scatter(
            churn_df,
            x="avg_gap_days",
            y="spend_change_pct",
            color="churn_status",
            title="Gap Days vs Spend Change",
            hover_data=["user_id", "days_since_last_order"]
        )
        st.plotly_chart(fig3, use_container_width=True)

        st.subheader("Top At-Risk Customers")
        st.dataframe(
            churn_df[churn_df["churn_status"] == "At Risk"]
            .sort_values("days_since_last_order", ascending=False)
            .head(20),
            use_container_width=True
        )

# -----------------------------
# LOYALTY & LOCATION
# -----------------------------
elif page == "Loyalty & Location":
    st.title("Loyalty and Location Performance")

    tab1, tab2 = st.tabs(["Loyalty", "Location"])

    with tab1:
        if loyalty_df.empty:
            st.warning("No loyalty data available.")
        else:
            fig1 = px.bar(
                loyalty_df,
                x="cohort_label",
                y="total_revenue",
                title="Revenue by Loyalty Cohort"
            )
            st.plotly_chart(fig1, use_container_width=True)

            fig2 = px.bar(
                loyalty_df,
                x="cohort_label",
                y="avg_order_value",
                title="Average Order Value by Loyalty Cohort"
            )
            st.plotly_chart(fig2, use_container_width=True)

            st.dataframe(loyalty_df, use_container_width=True)

    with tab2:
        if location_filtered.empty:
            st.warning("No location data available.")
        else:
            top_locations = (
                location_filtered.groupby("restaurant_id", as_index=False)
                .agg({"total_revenue": "sum", "total_orders": "sum"})
                .sort_values("total_revenue", ascending=False)
                .head(10)
            )
            top_locations["avg_order_value"] = (
                top_locations["total_revenue"] / top_locations["total_orders"]
            ).round(2)

            bottom_locations = (
                location_filtered.groupby("restaurant_id", as_index=False)
                .agg({"total_revenue": "sum", "total_orders": "sum"})
                .sort_values("total_revenue", ascending=True)
                .head(10)
            )
            bottom_locations["avg_order_value"] = (
                bottom_locations["total_revenue"] / bottom_locations["total_orders"]
            ).round(2)

            col1, col2 = st.columns(2)

            fig1 = px.bar(top_locations, x="restaurant_id", y="total_revenue", title="Top 10 Locations")
            col1.plotly_chart(fig1, use_container_width=True)

            fig2 = px.bar(bottom_locations, x="restaurant_id", y="total_revenue", title="Bottom 10 Locations")
            col2.plotly_chart(fig2, use_container_width=True)

            st.subheader("Top Locations Table")
            st.dataframe(top_locations, use_container_width=True)

# -----------------------------
# DISCOUNTS
# -----------------------------
elif page == "Discount Impact":
    st.title("Pricing and Discount Effectiveness")

    if discounts_filtered.empty:
        st.warning("No discount summary data available.")
    else:
        discount_summary = (
            discounts_filtered.groupby("discount_status", as_index=False)
            .agg({
                "total_orders": "sum",
                "gross_revenue": "sum",
                "discount_amount": "sum",
                "net_revenue": "sum"
            })
        )

        has_discounted = (discount_summary["discount_status"] == "Discounted").any()

        if not has_discounted:
            st.info(
                "Discount detection logic is implemented, but no discounted transactions "
                "were found in the current dataset (option_price < 0 = 0)."
            )

        fig1 = px.bar(discount_summary, x="discount_status", y="total_orders", title="Orders by Discount Status")
        st.plotly_chart(fig1, use_container_width=True)

        fig2 = px.bar(discount_summary, x="discount_status", y="net_revenue", title="Net Revenue by Discount Status")
        st.plotly_chart(fig2, use_container_width=True)

        st.dataframe(discount_summary, use_container_width=True)
