#!/bin/bash

cron -f &
streamlit run main_config.py --server.port=8501 --server.address=0.0.0.0