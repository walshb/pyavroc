#!/bin/sh

set -eu

for TRY in $(seq 5)
do
    PANDOC_META=$(curl --retry 5 https://api.github.com/repos/jgm/pandoc/releases/latest)
    [ -z "$PANDOC_META" ] || break
    sleep 10
done

[ -n "$PANDOC_META" ] || exit 1

PANDOC_URL=$(echo "$PANDOC_META" | jq -r '.assets[] | select(.name|contains(".deb")) | .browser_download_url')
curl -Lo pandoc.deb $PANDOC_URL
dpkg-deb -x pandoc.deb pandoc
