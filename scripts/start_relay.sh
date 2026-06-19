#!/bin/bash
# MCX Relay launcher for launchd
# Activates the correct Python environment and runs the relay loop.
# Wraps in `caffeinate -is` to keep the Mac awake (no idle sleep, no system
# sleep) during the entire 14.5h trading session. `-i` alone blocks idle sleep
# only — adding `-s` also blocks system sleep so lid-close mid-session can't
# silently kill the loop (this happened Jun 10).

cd "/Users/pranayagarwal/Dropbox/My Mac (Pranay's MacBook Air)/Documents/MCX/mcx-vercel"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

exec /usr/bin/caffeinate -is /opt/homebrew/bin/python3 scripts/mcx_relay.py --loop
