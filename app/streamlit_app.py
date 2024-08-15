import streamlit as st
import os
import sys
import tempfile
import json
import pandas as pd
import traceback
from xml.etree import ElementTree as ET

# Add the parent directory to the path to import the mogwai package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from mogwai.parser import PDFGraph, powerpoint_converter
from mogwai.core.steps.statics import *
from mogwai.parser.graphml_converter import graphml_to_mogwaigraph
from mogwai.parser.excel_converter import EXCELGraph
import mogwai.core.traversal as Trav
from mogwai.core import MogwaiGraph

from slides.slidewalker import PPT

def extract_graphml_keys(file_path):
    ns = {'graphml': 'http://graphml.graphdrawing.org/xmlns'}
    keys = {"node": [], "edge": []}
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    for key in root.findall("graphml:key", ns):
        if key.get("for") in keys:
            keys[key.get("for")].append(key.get("id"))
    return keys

def parse_file(file, node_label_key=None, node_name_key=None, edge_label_key=None):
    if file.name.endswith('.xlsx'):
        temp_path = os.path.join(tempfile.gettempdir(), file.name)
        with open(temp_path, 'wb') as f:
            f.write(file.getbuffer())
        xlsx_graph = EXCELGraph(temp_path)
        return xlsx_graph
    
    elif file.name.endswith('.pdf'):
        temp_path = os.path.join(tempfile.gettempdir(), file.name)
        with open(temp_path, 'wb') as f:
            f.write(file.getbuffer())
        pdf_graph = PDFGraph(temp_path)
        return pdf_graph
    
    elif file.name.endswith('.pptx'):
        temp_path = os.path.join(tempfile.gettempdir(), file.name)
        with open(temp_path, 'wb') as f:
            f.write(file.getbuffer())
        pp_graph = powerpoint_converter.PPGraph(file=temp_path)
        return pp_graph
    
    elif file.name.endswith('.graphml'):
        temp_path = os.path.join(tempfile.gettempdir(), file.name)
        with open(temp_path, 'wb') as f:
            f.write(file.getbuffer())
        
        if node_label_key and node_name_key and edge_label_key:
            ml_graph = graphml_to_mogwaigraph(file=temp_path, node_label_key=node_label_key, node_name_key=node_name_key, edge_label_key=edge_label_key)
            return ml_graph
        else:
            st.error("Node and edge keys are required for processing GraphML files.")
            return None
    
    else:
        st.error(f"Unsupported file type: {file.name}")
        return None
    
def is_valid_gremlin_query(query):
    if query.strip() and query.strip().startswith("g."):
        return True
    return False

def main():
    st.title("PymMogwai - Demo Webapp")
    st.write("Upload a document to begin processing:")

    uploaded_file = st.file_uploader("Choose a file", type=["xlsx", "pdf", "pptx", "graphml"])

    if 'graph' not in st.session_state:
        st.session_state.graph = None

    if uploaded_file is not None:
        st.write(f"Uploaded file name: {uploaded_file.name}")
        st.write(f"Uploaded file type: {uploaded_file.type}")

        if uploaded_file.name.endswith('.graphml'):
            temp_path = os.path.join(tempfile.gettempdir(), uploaded_file.name)
            with open(temp_path, 'wb') as f:
                f.write(uploaded_file.getbuffer())

            keys = extract_graphml_keys(temp_path)
            
            if not keys["node"] and not keys["edge"]:
                st.error("The GraphML file does not contain any keys.")
            else:
                st.write("Please select the node and edge keys for GraphML processing:")
                
                node_label_key = st.selectbox("Select node label key", options=keys["node"])
                node_name_key = st.selectbox("Select node name key", options=keys["node"])
                edge_label_key = st.selectbox("Select edge label key", options=keys["edge"])
                
                if st.button("Process File"):
                    st.session_state.graph = parse_file(uploaded_file, node_label_key, node_name_key, edge_label_key)
        else:
            st.session_state.graph = parse_file(uploaded_file)

        if st.session_state.graph is not None:
            st.write("File processed successfully.")
            st.write(f"Imported a graph with {len(st.session_state.graph.nodes)} nodes and {len(st.session_state.graph.edges)} edges.")
            g = Trav.MogwaiGraphTraversalSource(st.session_state.graph)

            st.subheader("Enter your query:")
            st.write("Query has to start with 'g.'")
            query = st.text_area("Query")

            if st.button("Run Query"):
                if is_valid_gremlin_query(query):
                    st.write(f"Running query: {query}")
                    try:
                        result = eval(query)
                        res = result.run()
                        st.write("Result:", res)
                    except Exception as e:
                        st.error(f"Error executing query: {e}")
                        with st.expander("See full traceback"):
                            st.error(traceback.format_exc())
                else:
                    st.error("Invalid Gremlin query. Please provide a valid query starting with 'g.'")

if __name__ == "__main__":
    main()