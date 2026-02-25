"""CSS injection for the CHView Streamlit app."""

import streamlit as st


def inject_custom_css() -> None:
    """Inject global custom CSS for a clean, theme-respecting UI."""
    st.markdown(
        """
    <style>
    /* --- F0 Design System Tokens --- */
    :root {
        /* Primitive palette */
        --f0-accent-50: hsl(348 80% 50%);
        --f0-viridian-50: hsl(184 92% 35%);
        --f0-camel-50: hsl(25 46% 53%);
        --f0-positive-50: hsl(160 84% 39%);
        --f0-critical-50: hsl(5 100% 65%);
        --f0-warning-50: hsl(25 95% 53%);

        /* Grey scale */
        --f0-grey-0: hsl(0 0% 100%);
        --f0-grey-5: hsl(220 20% 98%);
        --f0-grey-10: hsl(216 89% 18% / 0.06);
        --f0-grey-20: hsl(213 87% 15% / 0.20);
        --f0-grey-solid-40: hsl(219 18% 69%);
        --f0-grey-60: hsl(220 15% 50%);
        --f0-grey-80: hsl(218 30% 30%);
        --f0-grey-100: hsl(218 48% 10%);

        /* Semantic: foreground */
        --fg-default: var(--f0-grey-100);
        --fg-secondary: hsl(220 12% 45%);
        --fg-tertiary: var(--f0-grey-solid-40);

        /* Semantic: background */
        --bg-default: var(--f0-grey-0);
        --bg-secondary: var(--f0-grey-10);
        --bg-hover: hsl(216 89% 18% / 0.08);

        /* Semantic: border & shadow */
        --border-default: var(--f0-grey-20);
        --border-secondary: hsl(216 89% 18% / 0.12);
        --shadow: 0 1px 3px hsl(218 48% 10% / 0.06);

        /* Semantic: surface (light tints for banners/badges) */
        --accent-surface: hsl(348 80% 50% / 0.08);
        --positive-surface: hsl(160 84% 39% / 0.08);
        --critical-surface: hsl(5 100% 65% / 0.08);
        --warning-surface: hsl(25 95% 53% / 0.08);
        --camel-surface: hsl(25 46% 53% / 0.08);

        /* Shorthand aliases */
        --accent: var(--f0-accent-50);
        --viridian: var(--f0-viridian-50);
        --camel: var(--f0-camel-50);
        --positive: var(--f0-positive-50);
        --critical: var(--f0-critical-50);
        --warning: var(--f0-warning-50);

        /* Typography scale */
        --f0-text-xs: 0.75rem;
        --f0-text-sm: 0.8125rem;
        --f0-text-base: 0.875rem;
        --f0-text-lg: 1rem;
        --f0-text-xl: 1.25rem;
        --f0-text-2xl: 1.5rem;

        /* Radius scale */
        --f0-radius-2xs: 4px;
        --f0-radius-xs: 6px;
        --f0-radius-sm: 8px;
        --f0-radius-md: 10px;
        --f0-radius-lg: 12px;
        --f0-radius-xl: 16px;
    }

    /* --- Fonts --- */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    code, pre, .stCode {
        font-family: 'JetBrains Mono', monospace;
    }

    /* --- Hide Streamlit footer & auto-generated multipage nav --- */
    footer {visibility: hidden;}
    [data-testid="stSidebarNav"] {display: none;}

    /* --- Match header background to sidebar --- */
    header[data-testid="stHeader"] {
        background: var(--secondary-background-color) !important;
    }

    /* --- Top padding to clear Streamlit header --- */
    .block-container {
        padding-top: 3rem !important;
    }

    /* --- Nav buttons in sidebar --- */
    .stSidebar button[kind="primary"] {
        background: var(--accent-surface) !important;
        border: none !important;
        border-radius: var(--f0-radius-xs) !important;
        color: var(--accent) !important;
        text-align: center !important;
        font-size: var(--f0-text-sm) !important;
        font-weight: 600 !important;
        padding: 0.5rem 0.8rem !important;
    }
    .stSidebar button[kind="secondary"] {
        background: transparent !important;
        border: none !important;
        border-radius: var(--f0-radius-xs) !important;
        color: var(--fg-secondary);
        text-align: center !important;
        font-size: var(--f0-text-sm) !important;
        font-weight: 500 !important;
        padding: 0.5rem 0.8rem !important;
    }
    .stSidebar button[kind="secondary"]:hover {
        background: var(--bg-hover) !important;
        color: var(--fg-default);
    }

    /* --- Card title --- */
    .lenses-card-title {
        font-size: var(--f0-text-xs);
        font-weight: 600;
        color: var(--fg-secondary);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.5rem;
    }

    /* --- Metric cards --- */
    div[data-testid="stMetric"] {
        border: 1px solid var(--border-default);
        border-radius: var(--f0-radius-md);
        padding: 1rem 1.25rem;
        box-shadow: var(--shadow);
    }
    div[data-testid="stMetric"] label {
        font-size: 0.7rem !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--fg-tertiary) !important;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        font-size: var(--f0-text-2xl) !important;
        font-weight: 600 !important;
    }

    /* --- Dataframe borders --- */
    .stDataFrame {
        border: 1px solid var(--border-default);
        border-radius: var(--f0-radius-sm);
    }

    /* --- Expander headers in sidebar --- */
    .stSidebar details summary {
        font-weight: 600 !important;
        font-size: var(--f0-text-sm) !important;
    }

    /* --- Alert banners --- */
    .chview-alert-healthy {
        background: var(--positive-surface);
        border-left: 4px solid var(--positive);
        border-radius: var(--f0-radius-xs);
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        color: var(--fg-default);
        font-size: var(--f0-text-base);
    }
    .chview-alert-error {
        background: var(--critical-surface);
        border-left: 4px solid var(--critical);
        border-radius: var(--f0-radius-xs);
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        color: var(--fg-default);
        font-size: var(--f0-text-base);
    }
    .chview-alert-warning {
        background: var(--warning-surface);
        border-left: 4px solid var(--warning);
        border-radius: var(--f0-radius-xs);
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        color: var(--fg-default);
        font-size: var(--f0-text-base);
    }

    /* --- Streamlit Flow container (parent-level only; component internals
         are in an iframe and styled via inline node styles) --- */
    iframe[title="streamlit_flow.streamlit_flow"] {
        border: none !important;
        border-radius: var(--f0-radius-lg);
        background: var(--bg-default);
        box-shadow: var(--shadow);
        height: calc(100vh - 320px) !important;
        min-height: 400px;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )
