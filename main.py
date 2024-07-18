# TODO
# ADD IMAGE UPLOAD FUNCTION

#--------------------------------------------------------#
# Imports
#--------------------------------------------------------#
import streamlit as st
import time

import requests
import time
import pandas as pd
import re

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.pandas_tools import pd_writer
from snowflake.connector.pandas_tools import write_pandas

import os
from supabase import create_client, Client
from PIL import Image


#--------------------------------------------------------#
# Snowflake
#--------------------------------------------------------#
SNOWFLAKE_USER = st.secrets["user"]
SNOWFLAKE_PASS = st.secrets["password"]
SNOWFLAKE_ACCOUNT = st.secrets["account"]
SNOWFLAKE_WAREHOUSE = st.secrets["warehouse"]

def update_table(project_dict):
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASS,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE, 
        database="ARBIGRANTS",
        schema="DBT"
    )

    cursor = conn.cursor()

    # Update ARBIGRANTS_LABELS_PROJECT_CONTRACTS table
    table_name_contracts = "ARBIGRANTS_LABELS_PROJECT_CONTRACTS"

    name = project_dict['name']
    if project_dict.get('contracts'):
        contracts = re.split(r'[,\s/\n]+', str(project_dict['contracts']))

        for contract in contracts:
            contract = contract.lower()
            merge_query_contracts = f"""
            MERGE INTO {table_name_contracts} AS target
            USING (SELECT '{name}' AS NAME, '{contract}' AS CONTRACT_ADDRESS) AS source
            ON target.NAME = source.NAME AND target.CONTRACT_ADDRESS = source.CONTRACT_ADDRESS
            WHEN NOT MATCHED THEN
                INSERT (NAME, CONTRACT_ADDRESS)
                VALUES (source.NAME, source.CONTRACT_ADDRESS);
            """
            cursor.execute(merge_query_contracts)

    # Update ARBIGRANTS_LABELS_PROJECT_METADATA table
    table_name_metadata = "ARBIGRANTS_LABELS_PROJECT_METADATA"

    category = project_dict.get('category', '')
    grant_date = ''  
    llama_slug = ''
    llama_name = ''
    if project_dict.get('defillama'):
        llama_page = project_dict['defillama']
        llama_slug = llama_page.split('/')[-1].split('#')[0]
        llama_name = get_llama_name(llama_slug)
    chain = project_dict.get('chain', '')
    description = project_dict.get('description', '')[:249]
    website = project_dict.get('website', '')
    twitter = project_dict.get('twitter', '')
    dune = project_dict.get('dune', '')

    logo = project_dict['logo']
    file_bytes = logo.getvalue()
    path_on_supastorage = logo.name
    supabase.storage.from_("arb_logos").upload(
        file=file_bytes,
        path=path_on_supastorage, 
        file_options={"content-type": "image/*"}
        )
    logo_link = supabase.storage.from_('arb_logos').get_public_url(path_on_supastorage)

    merge_query_metadata = f"""
    MERGE INTO {table_name_metadata} AS target
    USING (
        SELECT
            %s AS NAME,
            %s AS CATEGORY,
            %s AS GRANT_DATE,
            %s AS LLAMA_SLUG,
            %s AS LLAMA_NAME,
            %s AS CHAIN,
            %s AS DESCRIPTION,
            %s AS LOGO,
            %s AS WEBSITE,
            %s AS TWITTER,
            %s AS DUNE
    ) AS source
    ON target.NAME = source.NAME
    WHEN NOT MATCHED THEN
        INSERT (NAME, CATEGORY, GRANT_DATE, LLAMA_SLUG, LLAMA_NAME, CHAIN, DESCRIPTION, LOGO, WEBSITE, TWITTER, DUNE)
        VALUES (source.NAME, source.CATEGORY, source.GRANT_DATE, source.LLAMA_SLUG, source.LLAMA_NAME, source.CHAIN, source.DESCRIPTION, source.LOGO, source.WEBSITE,source.TWITTER,source.DUNE);
    """
    cursor.execute(merge_query_metadata, (name, category, grant_date, llama_slug, llama_name, chain, description, logo_link, website, twitter, dune))

    conn.commit()
    cursor.close()
    conn.close()

#--------------------------------------------------------#
# Supabase
#--------------------------------------------------------#

url: str = st.secrets["supabase_url"]
key: str = st.secrets["supabase_key"]
supabase: Client = create_client(url, key)

#--------------------------------------------------------#
# Helper Functions
#--------------------------------------------------------#
def get_llama_name(llama_slug):
    if llama_slug:
        api_url = f"https://api.llama.fi/protocol/{llama_slug}"
        response = requests.get(api_url)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("name", "")
    
    return ""

def validate_evm_addresses(contracts_string):
    addresses = re.split(r'[,\s/\n]+', contracts_string)
    addresses = [addr.strip() for addr in addresses if addr.strip()]
    evm_address_pattern = re.compile(r'^0x[a-fA-F0-9]{40}$')
    return all(evm_address_pattern.match(addr) for addr in addresses)

#--------------------------------------------------------#
# Main Body
#--------------------------------------------------------#

st.set_page_config(
  page_title="Arbigrants Project Submission",
  page_icon="📝",
  layout="wide",
)

# Create the title at the tp of page
st.title('Arbigrants Project Submission')

with st.spinner():
    time.sleep(1.5)

if "new_project" in st.session_state:
    with st.spinner(text="Uploading data..."):
        try:
            update_table(st.session_state.new_project)
            result = st.session_state.pop("new_project")
            st.success("We have recorded your project, thank you!")
        except Exception as e:
            error_message = f"An error occurred while trying to record your project: {str(e)}"
            st.error(error_message)
            st.error(f"Full error details:\n\n{traceback.format_exc()}")

@st.experimental_fragment
def get_project_submission():
    with st.container(border=True):
        st.subheader("Enter your project's details")

        github = None
        defillama = None
        contracts = None
        dune = None
        name = st.text_input("Name of Project")
        description = st.text_input("Description of Project", placeholder="1 to 2 sentences")
        chain = st.selectbox("Which chain are you deployed on?", ["Arbitrum One", "Arbitrum Orbit", "Arbitrum Nova", "Offchain"])
        website = st.text_input("Link to Project Website") 
        twitter = st.text_input("Link to Project Twitter")
        logo = st.file_uploader("Upload your logo (PNG/JPG/JPEG)", type=['png', 'jpg', 'jpeg'], help="For the best results, use a square image of just your logo's icon")
        validate_github = st.selectbox("Does your project have a public GitHub", ["","yes", "no"])
        if validate_github == "yes":
            github = st.text_input("Link to Project GitHub") 
        category = st.selectbox("Select the project's category", ["","DeFi", "Gaming", "Infra", "RWA", "Social", "NFT", "Other"])
        validate_llama = st.selectbox("Does your project have a DefiLlama page", ["","yes", "no"])
        if validate_llama == "yes":
            defillama = st.text_input("Link to DefiLlama page") 
        validate_contracts = st.selectbox("Does your project have smart contracts", ["","yes", "no"])
        if validate_contracts == "yes":
            contracts = st.text_input("Comma-separated list of contracts", placeholder="0x077ab174ac10c904c5393f65fade8279dfbd3779, 0xa0b9ebd2cc138e0748c69baf66df2e01c57521ec, 0xb890844b1efd59d04d69cfb50a7ea984df94b143 ...")
        validate_dune = st.selectbox("Does your project have a Dune dashboard", ["","yes", "no"]) 
        if validate_dune == "yes":
            dune = st.text_input("Link to Dune dashboard") 
        
        submit_enabled = name and description and chain and website and twitter and category and validate_github and validate_llama and validate_contracts and validate_dune and logo
        if st.button("Submit", type="primary", disabled=not submit_enabled):
            if len(description) > 250:
                st.warning(f"Description is too long. Please shorten it.")
            elif contracts and not validate_evm_addresses(contracts):
                st.warning("Invalid contract address. Please check your contract addresses.")
            else:
                st.session_state.new_project = dict(name=name, 
                                                    description=description, 
                                                    chain=chain, 
                                                    website=website,
                                                    twitter=twitter,
                                                    github=github,
                                                    category=category,
                                                    defillama=defillama,
                                                    contracts=contracts,
                                                    dune=dune,
                                                    logo=logo
                                                    )
                st.rerun()


get_project_submission()


