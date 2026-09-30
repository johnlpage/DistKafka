#!/bin/bash
# Temporarily block/unblock outbound access from THIS host to the local
# region's mongos endpoint(s), to simulate a regional PrivateLink/network
# outage and observe the MongoDB driver's cross-region failover behaviour
# (see ARCHITECTURE.md "Combined seed-list failover").
#
# Blocks at the IP layer with iptables DROP (silent packet loss), NOT by
# rewriting /etc/hosts or using REJECT - a DROP is what an actual TGW
# routing gap / security-group denial looks like on the wire (connection
# hangs until serverSelectionTimeoutMS/connectTimeoutMS elapses), whereas
# REJECT or an /etc/hosts redirect to loopback produces an immediate
# ECONNREFUSED, which is a different (and more forgiving) failure mode
# for the driver's topology monitoring than what you'd see in production.
#
# Usage (run as root, e.g. via sudo):
#   ./toggle-mongos-block.sh block [region-filter]
#   ./toggle-mongos-block.sh unblock
#   ./toggle-mongos-block.sh status
#
# region-filter defaults to "eu-west-2" (London). It's matched against the
# SRV target hostnames returned for the SRV record embedded in MONGO_URI
# (sourced from /home/ec2-user/.env), e.g. "pl-0-eu-west-2.qcpeq8.mongodb.net"
# - so this works unmodified against either the public
# (mongodb+srv://distkafka-cluster.<id>.mongodb.net) or PrivateLink
# (mongodb+srv://distkafka-cluster-pl-0.<id>.mongodb.net) connection
# string, and re-resolves current IPs every time you call `block`, so it
# tolerates Atlas changing the underlying mongos topology between runs.
set -euo pipefail

CHAIN="MONGOS_REGION_BLOCK"
ACTION="${1:-status}"
REGION_FILTER="${2:-eu-west-2}"
ENV_FILE="/home/ec2-user/.env"

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: must run as root (sudo $0 $*)" >&2
  exit 1
fi

if ! command -v iptables >/dev/null 2>&1; then
  (dnf install -y iptables || yum install -y iptables)
fi

if ! command -v dig >/dev/null 2>&1; then
  (dnf install -y bind-utils || yum install -y bind-utils)
fi

# Idempotent: create the dedicated chain and hook it into OUTPUT exactly
# once, regardless of how many times this script runs.
iptables -N "${CHAIN}" 2>/dev/null || true
iptables -C OUTPUT -j "${CHAIN}" 2>/dev/null || iptables -I OUTPUT -j "${CHAIN}"

extract_srv_host() {
  # MONGO_URI="mongodb+srv://user:pass@HOST/?readPreference=..." -> HOST
  local uri="$1"
  uri="${uri#*://}"
  uri="${uri#*@}"
  uri="${uri%%/*}"
  uri="${uri%%\?*}"
  echo "$uri"
}

case "${ACTION}" in
  block)
    if [ ! -f "${ENV_FILE}" ]; then
      echo "ERROR: ${ENV_FILE} not found" >&2
      exit 1
    fi
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    if [ -z "${MONGO_URI:-}" ]; then
      echo "ERROR: MONGO_URI not set in ${ENV_FILE}" >&2
      exit 1
    fi

    SRV_HOST=$(extract_srv_host "${MONGO_URI}")
    echo "SRV host: ${SRV_HOST}"
    echo "Region filter: ${REGION_FILTER}"

    # Clear any previous rules before adding the current resolution -
    # handles both re-running `block` after IPs changed, and the
    # first-ever invocation (chain starts empty).
    iptables -F "${CHAIN}"

    MATCHED=0
    while read -r _prio _weight port target; do
      [ -z "${target:-}" ] && continue
      target="${target%.}" # strip trailing dot from SRV target
      case "${target}" in
        *"${REGION_FILTER}"*) ;;
        *) continue ;;
      esac
      MATCHED=1
      IPS=$(dig +short "${target}" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' || true)
      if [ -z "${IPS}" ]; then
        echo "WARNING: could not resolve ${target}, skipping" >&2
        continue
      fi
      for ip in ${IPS}; do
        echo "Blocking ${target} (${ip}:${port})"
        iptables -A "${CHAIN}" -d "${ip}" -p tcp --dport "${port}" -j DROP
      done
    done < <(dig +short SRV "_mongodb._tcp.${SRV_HOST}")

    if [ "${MATCHED}" -eq 0 ]; then
      echo "WARNING: no SRV targets matched region filter '${REGION_FILTER}' - nothing blocked." >&2
      echo "SRV targets seen:" >&2
      dig +short SRV "_mongodb._tcp.${SRV_HOST}" >&2
      exit 1
    fi

    echo "--- Active block rules ---"
    iptables -L "${CHAIN}" -n -v
    ;;

  unblock)
    iptables -F "${CHAIN}"
    echo "Unblocked - ${CHAIN} chain flushed."
    ;;

  status)
    echo "--- ${CHAIN} rules (empty = not currently blocking) ---"
    iptables -L "${CHAIN}" -n -v
    ;;

  *)
    echo "Usage: $0 {block|unblock|status} [region-filter]" >&2
    exit 1
    ;;
esac
