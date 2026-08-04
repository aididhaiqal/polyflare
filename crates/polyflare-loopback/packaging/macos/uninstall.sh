#!/bin/sh
set -eu

agent="$HOME/Library/LaunchAgents/com.polyflare.loopback.plist"
launchctl bootout "gui/$(id -u)/com.polyflare.loopback" 2>/dev/null || true
rm -f "$agent" "$HOME/.local/bin/polyflare-loopback"
echo "Removed com.polyflare.loopback (logs were preserved)"
