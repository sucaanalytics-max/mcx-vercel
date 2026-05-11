#!/bin/bash
# MCX Relay launcher for launchd
# Activates the correct Python environment and runs the relay loop.
# Wraps in `caffeinate -i` to keep the Mac awake (no idle sleep) during the
# entire 14.5h trading session — otherwise time.sleep() pauses on lid-close.

cd "/Users/pranayagarwal/Dropbox/My Mac (Pranay's MacBook Air)/Documents/MCX/mcx-vercel"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

exec /usr/bin/caffeinate -i /opt/homebrew/bin/python3 scripts/mcx_relay.py --loop
