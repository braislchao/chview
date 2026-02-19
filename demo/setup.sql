-- CHView Demo Schema
-- Creates a realistic e-commerce analytics pipeline with multiple MVs and connections

-- =====================================================================
-- Database: ecommerce
-- =====================================================================
CREATE DATABASE IF NOT EXISTS ecommerce;

-- Raw events table (source for everything)
CREATE TABLE IF NOT EXISTS ecommerce.raw_events (
    event_id UUID DEFAULT generateUUIDv4(),
    event_time DateTime DEFAULT now(),
    user_id UInt64,
    session_id String,
    event_type Enum8('page_view'=1, 'add_to_cart'=2, 'purchase'=3, 'refund'=4, 'search'=5),
    product_id UInt32,
    category String,
    price Decimal64(2),
    quantity UInt16 DEFAULT 1,
    country LowCardinality(String),
    device LowCardinality(String),
    referrer LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
PARTITION BY toYYYYMM(event_time);

-- Hourly aggregated page views
CREATE TABLE IF NOT EXISTS ecommerce.pageviews_hourly (
    hour DateTime,
    country LowCardinality(String),
    device LowCardinality(String),
    views UInt64,
    unique_users UInt64
) ENGINE = SummingMergeTree()
ORDER BY (hour, country, device);

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_pageviews_hourly
TO ecommerce.pageviews_hourly AS
SELECT
    toStartOfHour(event_time) AS hour,
    country,
    device,
    count() AS views,
    uniqExact(user_id) AS unique_users
FROM ecommerce.raw_events
WHERE event_type = 'page_view'
GROUP BY hour, country, device;

-- Revenue per hour per category
CREATE TABLE IF NOT EXISTS ecommerce.revenue_hourly (
    hour DateTime,
    category String,
    revenue Decimal64(2),
    orders UInt64,
    items_sold UInt64
) ENGINE = SummingMergeTree()
ORDER BY (hour, category);

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_revenue_hourly
TO ecommerce.revenue_hourly AS
SELECT
    toStartOfHour(event_time) AS hour,
    category,
    sum(price * quantity) AS revenue,
    count() AS orders,
    sum(quantity) AS items_sold
FROM ecommerce.raw_events
WHERE event_type = 'purchase'
GROUP BY hour, category;

-- Cart abandonment tracking
CREATE TABLE IF NOT EXISTS ecommerce.cart_abandonment (
    hour DateTime,
    category String,
    carts_created UInt64,
    carts_purchased UInt64
) ENGINE = SummingMergeTree()
ORDER BY (hour, category);

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_cart_abandonment
TO ecommerce.cart_abandonment AS
SELECT
    toStartOfHour(event_time) AS hour,
    category,
    countIf(event_type = 'add_to_cart') AS carts_created,
    countIf(event_type = 'purchase') AS carts_purchased
FROM ecommerce.raw_events
WHERE event_type IN ('add_to_cart', 'purchase')
GROUP BY hour, category;

-- Top products (real-time leaderboard)
CREATE TABLE IF NOT EXISTS ecommerce.top_products (
    product_id UInt32,
    category String,
    total_revenue Decimal64(2),
    total_orders UInt64,
    last_sold DateTime
) ENGINE = ReplacingMergeTree(last_sold)
ORDER BY (product_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_top_products
TO ecommerce.top_products AS
SELECT
    product_id,
    any(category) AS category,
    sum(price * quantity) AS total_revenue,
    count() AS total_orders,
    max(event_time) AS last_sold
FROM ecommerce.raw_events
WHERE event_type = 'purchase'
GROUP BY product_id;

-- =====================================================================
-- Database: analytics
-- =====================================================================
CREATE DATABASE IF NOT EXISTS analytics;

-- User sessions (built from raw events)
CREATE TABLE IF NOT EXISTS analytics.user_sessions (
    session_id String,
    user_id UInt64,
    session_start DateTime,
    country LowCardinality(String),
    device LowCardinality(String),
    referrer LowCardinality(String),
    page_views UInt32,
    events UInt32
) ENGINE = ReplacingMergeTree(session_start)
ORDER BY (session_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_user_sessions
TO analytics.user_sessions AS
SELECT
    session_id,
    any(user_id) AS user_id,
    min(event_time) AS session_start,
    any(country) AS country,
    any(device) AS device,
    any(referrer) AS referrer,
    countIf(event_type = 'page_view') AS page_views,
    count() AS events
FROM ecommerce.raw_events
GROUP BY session_id;

-- Search analytics
CREATE TABLE IF NOT EXISTS analytics.search_metrics (
    hour DateTime,
    category String,
    searches UInt64,
    unique_searchers UInt64
) ENGINE = SummingMergeTree()
ORDER BY (hour, category);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_search_metrics
TO analytics.search_metrics AS
SELECT
    toStartOfHour(event_time) AS hour,
    category,
    count() AS searches,
    uniqExact(user_id) AS unique_searchers
FROM ecommerce.raw_events
WHERE event_type = 'search'
GROUP BY hour, category;

-- Refund tracking
CREATE TABLE IF NOT EXISTS analytics.refund_tracking (
    hour DateTime,
    category String,
    refunds UInt64,
    refund_amount Decimal64(2)
) ENGINE = SummingMergeTree()
ORDER BY (hour, category);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_refund_tracking
TO analytics.refund_tracking AS
SELECT
    toStartOfHour(event_time) AS hour,
    category,
    count() AS refunds,
    sum(price * quantity) AS refund_amount
FROM ecommerce.raw_events
WHERE event_type = 'refund'
GROUP BY hour, category;

-- Daily country rollup (reads from pageviews_hourly — second-level MV)
CREATE TABLE IF NOT EXISTS analytics.daily_country_stats (
    day Date,
    country LowCardinality(String),
    total_views UInt64,
    total_users UInt64
) ENGINE = SummingMergeTree()
ORDER BY (day, country);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_daily_country_stats
TO analytics.daily_country_stats AS
SELECT
    toDate(hour) AS day,
    country,
    sum(views) AS total_views,
    sum(unique_users) AS total_users
FROM ecommerce.pageviews_hourly
GROUP BY day, country;

-- Revenue alerts (reads from revenue_hourly — second-level MV)
CREATE TABLE IF NOT EXISTS analytics.revenue_alerts (
    hour DateTime,
    category String,
    revenue Decimal64(2),
    alert_type LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (hour, category);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_revenue_alerts
TO analytics.revenue_alerts AS
SELECT
    hour,
    category,
    revenue,
    'high_revenue' AS alert_type
FROM ecommerce.revenue_hourly
WHERE revenue > 500;
