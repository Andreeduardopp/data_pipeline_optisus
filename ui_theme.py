"""
Shared UI theme and CSS injection for the Optisus Data Pipeline.

Color Palette:
  - Primary Dark / Backgrounds: #00002B
  - Light Accents / Highlights:  #86DEEE
  - Teal Shade 1:                #0F766E
  - Teal Shade 2:                #134E4A
  - Teal Shade 3:                #31929c
  - Blue Accent:                 #2563eb
"""

import streamlit as st

# ─── Color Tokens ────────────────────────────────────────────────────────
PRIMARY_DARK = "#00002B"
LIGHT_ACCENT = "#86DEEE"
TEAL_1 = "#0F766E"
TEAL_2 = "#134E4A"
TEAL_3 = "#31929c"
BLUE_ACCENT = "#2563eb"

# Derived shades
SURFACE = "#050535"
SURFACE_RAISED = "#0d0d4a"
SURFACE_HOVER = "#12124f"
BORDER_DEFAULT = "#1a1a5c"
BORDER_ACCENT = "#31929c55"
TEXT_PRIMARY = "#e2e8f0"
TEXT_SECONDARY = "#94a3b8"
TEXT_MUTED = "#64748b"
SUCCESS = "#22c55e"
WARNING = "#f59e0b"
ERROR = "#ef4444"


def inject_custom_css() -> None:
    """Inject the full custom CSS theme into the Streamlit page."""
    st.markdown(_CUSTOM_CSS, unsafe_allow_html=True)


def render_logo_header() -> None:
    """Render the app logo and header with gradient styling."""
    st.markdown(
        """
        <div class="optisus-header">
            <div class="optisus-logo">
                <div class="logo-icon">
                    <svg width="36" height="36" viewBox="0 0 36 36" fill="none">
                        <rect width="36" height="36" rx="8" fill="url(#logo-grad)"/>
                        <path d="M8 18h6l3-8 4 16 3-8h4" stroke="white" stroke-width="2.5"
                              stroke-linecap="round" stroke-linejoin="round" fill="none"/>
                        <defs>
                            <linearGradient id="logo-grad" x1="0" y1="0" x2="36" y2="36">
                                <stop offset="0%" stop-color="#2563eb"/>
                                <stop offset="100%" stop-color="#0F766E"/>
                            </linearGradient>
                        </defs>
                    </svg>
                </div>
                <div class="logo-text">
                    <span class="logo-name">Optisus</span>
                    <span class="logo-subtitle">Data Pipeline</span>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


_CUSTOM_CSS = f"""
<style>
/* ═══════════════════════════════════════════════════════════════════════
   Google Fonts — Inter
   ═══════════════════════════════════════════════════════════════════════ */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

/* ═══════════════════════════════════════════════════════════════════════
   Root & Global Overrides
   ═══════════════════════════════════════════════════════════════════════ */
html, body, [class*="css"] {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
}}

.stApp {{
    background: linear-gradient(168deg, {PRIMARY_DARK} 0%, {SURFACE} 50%, #030320 100%) !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Scrollbar
   ═══════════════════════════════════════════════════════════════════════ */
::-webkit-scrollbar {{
    width: 6px;
    height: 6px;
}}
::-webkit-scrollbar-track {{
    background: {PRIMARY_DARK};
}}
::-webkit-scrollbar-thumb {{
    background: {TEAL_2};
    border-radius: 3px;
}}
::-webkit-scrollbar-thumb:hover {{
    background: {TEAL_3};
}}

/* ═══════════════════════════════════════════════════════════════════════
   Logo Header
   ═══════════════════════════════════════════════════════════════════════ */
.optisus-header {{
    margin-bottom: 1.5rem;
    padding-bottom: 1rem;
    border-bottom: 1px solid {BORDER_DEFAULT};
}}
.optisus-logo {{
    display: flex;
    align-items: center;
    gap: 14px;
}}
.logo-icon {{
    flex-shrink: 0;
    filter: drop-shadow(0 0 12px rgba(37, 99, 235, 0.4));
}}
.logo-text {{
    display: flex;
    flex-direction: column;
    gap: 0px;
}}
.logo-name {{
    font-size: 1.6rem;
    font-weight: 800;
    background: linear-gradient(135deg, {LIGHT_ACCENT}, {BLUE_ACCENT});
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    letter-spacing: -0.02em;
    line-height: 1.2;
}}
.logo-subtitle {{
    font-size: 0.8rem;
    font-weight: 400;
    color: {TEXT_MUTED};
    letter-spacing: 0.08em;
    text-transform: uppercase;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Sidebar
   ═══════════════════════════════════════════════════════════════════════ */
section[data-testid="stSidebar"] {{
    background: linear-gradient(180deg, {SURFACE} 0%, {PRIMARY_DARK} 100%) !important;
    border-right: 1px solid {BORDER_DEFAULT} !important;
}}
section[data-testid="stSidebar"] .stMarkdown p,
section[data-testid="stSidebar"] .stMarkdown span {{
    color: {TEXT_SECONDARY} !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Headings
   ═══════════════════════════════════════════════════════════════════════ */
h1 {{
    color: {TEXT_PRIMARY} !important;
    font-weight: 700 !important;
    letter-spacing: -0.03em !important;
    font-size: 2rem !important;
}}
h2 {{
    color: {LIGHT_ACCENT} !important;
    font-weight: 600 !important;
    letter-spacing: -0.02em !important;
    margin-top: 1.5rem !important;
}}
h3 {{
    color: {TEXT_PRIMARY} !important;
    font-weight: 600 !important;
    letter-spacing: -0.01em !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Primary Buttons
   ═══════════════════════════════════════════════════════════════════════ */
.stButton > button[kind="primary"],
button[data-testid="stBaseButton-primary"] {{
    background: linear-gradient(135deg, {BLUE_ACCENT} 0%, {TEAL_1} 100%) !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    padding: 0.55rem 1.4rem !important;
    font-weight: 600 !important;
    font-size: 0.875rem !important;
    letter-spacing: 0.01em !important;
    transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
    box-shadow: 0 2px 8px rgba(37, 99, 235, 0.25) !important;
}}
.stButton > button[kind="primary"]:hover,
button[data-testid="stBaseButton-primary"]:hover {{
    box-shadow: 0 4px 20px rgba(37, 99, 235, 0.4) !important;
    transform: translateY(-1px) !important;
    filter: brightness(1.08) !important;
}}

/* Secondary / Default Buttons */
.stButton > button[kind="secondary"],
.stButton > button:not([kind]),
button[data-testid="stBaseButton-secondary"] {{
    background: {SURFACE_RAISED} !important;
    color: {TEXT_PRIMARY} !important;
    border: 1px solid {BORDER_DEFAULT} !important;
    border-radius: 8px !important;
    padding: 0.55rem 1.4rem !important;
    font-weight: 500 !important;
    font-size: 0.875rem !important;
    transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
}}
.stButton > button[kind="secondary"]:hover,
.stButton > button:not([kind]):hover,
button[data-testid="stBaseButton-secondary"]:hover {{
    background: {SURFACE_HOVER} !important;
    border-color: {TEAL_3} !important;
    box-shadow: 0 2px 12px rgba(49, 146, 156, 0.2) !important;
    transform: translateY(-1px) !important;
}}

/* Disabled Buttons */
.stButton > button:disabled {{
    background: {SURFACE} !important;
    color: {TEXT_MUTED} !important;
    border-color: {BORDER_DEFAULT} !important;
    opacity: 0.5 !important;
    cursor: not-allowed !important;
    box-shadow: none !important;
    transform: none !important;
}}

/* Download Buttons */
.stDownloadButton > button {{
    background: transparent !important;
    color: {LIGHT_ACCENT} !important;
    border: 1px solid {TEAL_3} !important;
    border-radius: 8px !important;
    font-weight: 500 !important;
    transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
}}
.stDownloadButton > button:hover {{
    background: {TEAL_2}33 !important;
    border-color: {LIGHT_ACCENT} !important;
    box-shadow: 0 2px 12px rgba(134, 222, 238, 0.15) !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Inputs: Text, Selectbox, Multiselect
   ═══════════════════════════════════════════════════════════════════════ */
.stTextInput > div > div > input,
.stTextArea > div > div > textarea {{
    background-color: {SURFACE} !important;
    color: {TEXT_PRIMARY} !important;
    border: 1px solid {BORDER_DEFAULT} !important;
    border-radius: 8px !important;
    padding: 0.6rem 0.9rem !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
}}
.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {{
    border-color: {BLUE_ACCENT} !important;
    box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.15) !important;
}}
.stTextInput > div > div > input::placeholder {{
    color: {TEXT_MUTED} !important;
}}

div[data-baseweb="select"] > div {{
    background-color: {SURFACE} !important;
    border: 1px solid {BORDER_DEFAULT} !important;
    border-radius: 8px !important;
    transition: border-color 0.2s ease !important;
}}
div[data-baseweb="select"] > div:hover {{
    border-color: {TEAL_3} !important;
}}

/* Multiselect tags */
span[data-baseweb="tag"] {{
    background: linear-gradient(135deg, {TEAL_2}, {TEAL_1}) !important;
    border: none !important;
    border-radius: 6px !important;
    color: white !important;
    font-weight: 500 !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Containers & Cards
   ═══════════════════════════════════════════════════════════════════════ */
div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"] {{
    background: {SURFACE_RAISED} !important;
    border: 1px solid {BORDER_DEFAULT} !important;
    border-radius: 12px !important;
    transition: border-color 0.3s ease, box-shadow 0.3s ease !important;
}}
div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"]:hover {{
    border-color: {TEAL_3}88 !important;
    box-shadow: 0 4px 24px rgba(15, 118, 110, 0.08) !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Expanders
   ═══════════════════════════════════════════════════════════════════════ */
.streamlit-expanderHeader {{
    background: {SURFACE_RAISED} !important;
    border-radius: 8px !important;
    color: {TEXT_PRIMARY} !important;
    font-weight: 500 !important;
    transition: background 0.2s ease !important;
}}
.streamlit-expanderHeader:hover {{
    background: {SURFACE_HOVER} !important;
}}
div[data-testid="stExpander"] {{
    border: 1px solid {BORDER_DEFAULT} !important;
    border-radius: 10px !important;
    overflow: hidden;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Tabs
   ═══════════════════════════════════════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {{
    gap: 0px !important;
    background: {SURFACE} !important;
    border-radius: 10px !important;
    padding: 4px !important;
    border: 1px solid {BORDER_DEFAULT} !important;
}}
.stTabs [data-baseweb="tab"] {{
    border-radius: 8px !important;
    color: {TEXT_SECONDARY} !important;
    font-weight: 500 !important;
    padding: 8px 20px !important;
    transition: all 0.2s ease !important;
}}
.stTabs [aria-selected="true"] {{
    background: linear-gradient(135deg, {TEAL_2}, {TEAL_1}) !important;
    color: white !important;
    font-weight: 600 !important;
    box-shadow: 0 2px 8px rgba(15, 118, 110, 0.3) !important;
}}
.stTabs [data-baseweb="tab"]:hover:not([aria-selected="true"]) {{
    color: {LIGHT_ACCENT} !important;
    background: {SURFACE_HOVER} !important;
}}
.stTabs [data-baseweb="tab-highlight"] {{
    display: none !important;
}}
.stTabs [data-baseweb="tab-border"] {{
    display: none !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Dataframes / Tables
   ═══════════════════════════════════════════════════════════════════════ */
.stDataFrame {{
    border: 1px solid {BORDER_DEFAULT} !important;
    border-radius: 10px !important;
    overflow: hidden !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Alerts: Success, Error, Warning, Info
   ═══════════════════════════════════════════════════════════════════════ */
div[data-testid="stAlert"] {{
    border-radius: 10px !important;
    border-left-width: 4px !important;
    backdrop-filter: blur(4px) !important;
}}

/* Dividers */
hr {{
    border-color: {BORDER_DEFAULT} !important;
    opacity: 0.5 !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Radio Buttons (horizontal mode selector)
   ═══════════════════════════════════════════════════════════════════════ */
div[data-testid="stRadio"] > div {{
    gap: 0.5rem !important;
}}
div[data-testid="stRadio"] label {{
    background: {SURFACE} !important;
    border: 1px solid {BORDER_DEFAULT} !important;
    border-radius: 8px !important;
    padding: 6px 16px !important;
    transition: all 0.2s ease !important;
    cursor: pointer !important;
}}
div[data-testid="stRadio"] label:hover {{
    border-color: {TEAL_3} !important;
    background: {SURFACE_RAISED} !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   File Uploader
   ═══════════════════════════════════════════════════════════════════════ */
section[data-testid="stFileUploader"] {{
    border: 2px dashed {BORDER_DEFAULT} !important;
    border-radius: 12px !important;
    transition: border-color 0.3s ease !important;
    padding: 1rem !important;
}}
section[data-testid="stFileUploader"]:hover {{
    border-color: {TEAL_3} !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Code Blocks
   ═══════════════════════════════════════════════════════════════════════ */
.stCode, code, pre {{
    background-color: {SURFACE} !important;
    border: 1px solid {BORDER_DEFAULT} !important;
    border-radius: 8px !important;
    color: {LIGHT_ACCENT} !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Captions & Labels
   ═══════════════════════════════════════════════════════════════════════ */
.stCaption, [data-testid="stCaptionContainer"] {{
    color: {TEXT_MUTED} !important;
}}
label, .stFormLabel {{
    color: {TEXT_SECONDARY} !important;
    font-weight: 500 !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   JSON Viewer
   ═══════════════════════════════════════════════════════════════════════ */
.react-json-view {{
    background: {SURFACE} !important;
    border-radius: 10px !important;
    padding: 1rem !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Metric cards (for future use)
   ═══════════════════════════════════════════════════════════════════════ */
div[data-testid="stMetric"] {{
    background: {SURFACE_RAISED} !important;
    border: 1px solid {BORDER_DEFAULT} !important;
    border-radius: 10px !important;
    padding: 1rem !important;
}}
div[data-testid="stMetric"] label {{
    color: {TEXT_SECONDARY} !important;
}}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {{
    color: {LIGHT_ACCENT} !important;
    font-weight: 700 !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Spinner
   ═══════════════════════════════════════════════════════════════════════ */
.stSpinner > div {{
    border-top-color: {BLUE_ACCENT} !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Toast / Notifications
   ═══════════════════════════════════════════════════════════════════════ */
div[data-testid="stToast"] {{
    background: {SURFACE_RAISED} !important;
    border: 1px solid {BORDER_DEFAULT} !important;
    border-radius: 10px !important;
}}

/* ═══════════════════════════════════════════════════════════════════════
   Micro-animations & Polish
   ═══════════════════════════════════════════════════════════════════════ */
@keyframes fadeIn {{
    from {{ opacity: 0; transform: translateY(8px); }}
    to   {{ opacity: 1; transform: translateY(0); }}
}}
.main .block-container {{
    animation: fadeIn 0.35s ease-out;
    max-width: 1100px !important;
    padding-top: 2rem !important;
}}
</style>
"""
