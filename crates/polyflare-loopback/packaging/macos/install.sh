#!/bin/sh
set -eu

source_binary=${1:?usage: install.sh /path/to/polyflare-loopback https://remote-origin}
upstream_origin=${2:?usage: install.sh /path/to/polyflare-loopback https://remote-origin}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
destination_dir="$HOME/.local/bin"
destination="$destination_dir/polyflare-loopback"
agent_dir="$HOME/Library/LaunchAgents"
agent="$agent_dir/com.polyflare.loopback.plist"
log_dir="$HOME/Library/Logs/PolyFlare"
log_path="$log_dir/loopback.log"

"$source_binary" --upstream-origin "$upstream_origin" --check-config
mkdir -p "$destination_dir" "$agent_dir" "$log_dir"
launchctl bootout "gui/$(id -u)/com.polyflare.loopback" 2>/dev/null || true
install -m 755 "$source_binary" "$destination"
cp "$script_dir/com.polyflare.loopback.plist" "$agent"
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:0 $destination" "$agent"
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:2 $upstream_origin" "$agent"
plutil -replace StandardOutPath -string "$log_path" "$agent"
plutil -replace StandardErrorPath -string "$log_path" "$agent"
plutil -lint "$agent"
launchctl bootstrap "gui/$(id -u)" "$agent"
attempt=0
healthy=false
while [ "$attempt" -lt 30 ]; do
  health=$(curl -fsS --max-time 4 http://127.0.0.1:8080/_polyflare-loopback/health 2>/dev/null || true)
  case "$health" in
    *'"status":"ok"'*'"mode":"remote-polyflare-loopback"'*) healthy=true; break ;;
  esac
  attempt=$((attempt + 1))
  sleep 0.5
done
if [ "$healthy" != true ]; then
  echo "Installed the agent, but its loopback health check failed" >&2
  exit 1
fi
echo "Installed com.polyflare.loopback"
