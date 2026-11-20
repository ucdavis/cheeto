#!/usr/bin/env bash
# Query the cheeto REST API.
#
#   query-api.sh [-c config] [-r profile] [-a address] [-p port] [-P prefix]
#                [-k api-key] endpoint
#
# The defaults for address, port, prefix and API key come from the `api`
# section of the config (read with mikefarah yq). The section is profiled:
# `-r` selects the profile, and a missing profile falls back to `default`,
# as in cheeto/config.py. `endpoint` is relative to the prefix, for example
# `sites/farm/users`. Extra curl options go in $CURL_OPTS.

set -euo pipefail

usage() {
    sed -n '4,5p' "$0" | sed 's/^# \{0,1\}//' >&2
    exit "${1:-2}"
}

config="$HOME/.config/cheeto/config.yaml"
profile=default
address='' port='' prefix='' key=''
prefix_set=0 key_set=0

while getopts 'c:r:a:p:P:k:h' opt; do
    case "$opt" in
        c) config=$OPTARG ;;
        r) profile=$OPTARG ;;
        a) address=$OPTARG ;;
        p) port=$OPTARG ;;
        P) prefix=$OPTARG; prefix_set=1 ;;
        k) key=$OPTARG; key_set=1 ;;
        h) usage 0 ;;
        *) usage ;;
    esac
done
shift $((OPTIND - 1))
[[ $# -eq 1 ]] || usage
endpoint=$1

# Read one field of the api section: the selected profile, else `default`.
cfg() {
    [[ -r $config ]] || return 0
    PROFILE=$profile yq -r \
        "(.api[strenv(PROFILE)].$1 // .api.default.$1) // \"\"" "$config"
}

[[ -n $address ]] || address=$(cfg host)
[[ -n $port ]] || port=$(cfg port)
(( prefix_set )) || prefix=$(cfg prefix)
(( key_set )) || key=$(cfg api_key)

address=${address:-127.0.0.1}
port=${port:-8000}

# Join with single slashes: "/cheeto/api/v1/" + "/sites" -> "/cheeto/api/v1/sites".
prefix=${prefix#/}
prefix=${prefix%/}
endpoint=${endpoint#/}
path=/${prefix:+$prefix/}$endpoint

[[ $address == *://* ]] || address=http://$address

headers=()
[[ -z $key ]] || headers=(-H "X-API-Key: $key")

# shellcheck disable=SC2086  # CURL_OPTS is split on purpose.
exec curl -sS --fail-with-body "${headers[@]}" ${CURL_OPTS:-} \
    "$address:$port$path"
