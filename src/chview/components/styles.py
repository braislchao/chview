"""CSS injection for the CHView Streamlit app."""

import streamlit as st


def inject_custom_css() -> None:
    """Inject global custom CSS for a clean, theme-respecting UI."""
    st.markdown(
        """
    <style>
    /* --- Factorial Design System Palette --- */
    :root {
        --radical-red: #E51943;
        --viridian-green: #0E9AA7;
        --ebony-clay: #25253D;
        --red-light: #FDE8EA;
        --viridian-light: #EEF8F8;
        --target-color: #D4956F;
        --target-light: #FBF4EE;
        --implicit-color: #B8B0B0;
        --implicit-light: #F5F4F4;
        --warning: #E5A019;
        --warning-light: #FFF5E0;
        --error-light: #FDE8EA;
        --success-light: #EEF8F8;
    }

    /* --- Fonts --- */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    code, pre, .stCode {
        font-family: 'JetBrains Mono', monospace;
    }

    /* --- Hide Streamlit footer --- */
    footer {visibility: hidden;}

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
        background: rgba(229, 25, 67, 0.10) !important;
        border: none !important;
        border-radius: 6px !important;
        color: var(--radical-red) !important;
        text-align: center !important;
        font-size: 0.85rem !important;
        font-weight: 600 !important;
        padding: 0.5rem 0.8rem !important;
    }
    .stSidebar button[kind="secondary"] {
        background: transparent !important;
        border: none !important;
        border-radius: 6px !important;
        opacity: 0.6;
        text-align: center !important;
        font-size: 0.85rem !important;
        font-weight: 500 !important;
        padding: 0.5rem 0.8rem !important;
    }
    .stSidebar button[kind="secondary"]:hover {
        opacity: 1;
        background: rgba(229, 25, 67, 0.08) !important;
    }

    /* --- Card title --- */
    .lenses-card-title {
        font-size: 0.75rem;
        font-weight: 600;
        color: var(--ebony-clay);
        opacity: 0.6;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.5rem;
    }

    /* --- Metric cards --- */
    div[data-testid="stMetric"] {
        border: 1px solid rgba(128, 128, 128, 0.15);
        border-radius: 10px;
        padding: 1rem 1.25rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }
    div[data-testid="stMetric"] label {
        font-size: 0.7rem !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        opacity: 0.6;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        font-size: 1.5rem !important;
        font-weight: 600 !important;
    }

    /* --- Dataframe borders --- */
    .stDataFrame {
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-radius: 8px;
    }

    /* --- Expander headers in sidebar --- */
    .stSidebar details summary {
        font-weight: 600 !important;
        font-size: 0.85rem !important;
    }

    /* --- Alert banners --- */
    .chview-alert-healthy {
        background: var(--viridian-light);
        border-left: 4px solid var(--viridian-green);
        border-radius: 6px;
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        color: var(--ebony-clay);
        font-size: 0.9rem;
    }
    .chview-alert-error {
        background: var(--red-light);
        border-left: 4px solid var(--radical-red);
        border-radius: 6px;
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        color: var(--ebony-clay);
        font-size: 0.9rem;
    }
    .chview-alert-warning {
        background: var(--warning-light);
        border-left: 4px solid var(--warning);
        border-radius: 6px;
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        color: var(--ebony-clay);
        font-size: 0.9rem;
    }

    /* --- Streamlit Flow container (parent-level only; component internals
         are in an iframe and styled via inline node styles) --- */
    iframe[title="streamlit_flow.streamlit_flow"] {
        border: none !important;
        border-radius: 12px;
        background: #fafbfc;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        height: calc(100vh - 320px) !important;
        min-height: 400px;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )
