#!/bin/bash
# Start the data streamer and the analysis script in the background
nohup python data_streamer.py &
python crypto_app.py
