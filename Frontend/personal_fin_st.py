import numpy as np
import plotly.express as px
import pandas as pd
from pathlib import Path
from PIL import Image
import streamlit as st

from src.data.dataframe_generator import balance, total_cred_balance, total_deb_balance \
    ,total_all_data


#Setting favicon
def get_file_icon_path() -> str:
        """This function use to get the file"""
        curr_wd = Path(Path.cwd())
        for file in curr_wd.glob('*.ico'):
            return str(file)
ICON = Image.open(get_file_icon_path())

#Setting Page config
st.set_page_config(
    page_title = 'Finance_Analytics',
    page_icon = ICON,
    layout = 'wide',
    initial_sidebar_state="expanded",
)
#Fetching css file
with open('style.css')as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html = True)
# Page title
st.title('MY PERSONAL FINANCE DASHBOARD')
st.header("KPI section")

#Metric
col1, col2, col3 = st.columns(3)
col1.metric(label="Total Current Balance",value=str(balance()))
col2.metric(label="Total Credit Amount",value=str(total_cred_balance()))
col3.metric(label="Total Debit Amount",value=str(total_deb_balance()))

#Graph
bank_df = total_all_data()
print(bank_df)
fig = px.line(bank_df, x='transaction_date', y='balance')
## Display the chart
st.plotly_chart(fig, use_container_width=True)