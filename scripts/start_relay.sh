#!/bin/bash
# MCX Relay launcher for launchd
# Activates the correct Python environment and runs the relay loop

cd "/Users/pranayagarwal/Dropbox/My Mac (Pranay's MacBook Air)/Documents/MCX/mcx-vercel"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

exec /opt/homebrew/bin/python3 scripts/mcx_relay.py --loop
