#!/bin/sh

# Copyright 2017 Ben Walsh
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ux

set | grep '^TRAVIS' >&2

SUBDIR=pandoc_download

cd $TRAVIS_BUILD_DIR
mkdir -p $SUBDIR

for TRY in $(seq 10)
do
    curl --retry 5 https://api.github.com/repos/jgm/pandoc/releases/latest >$SUBDIR/meta.json

    PANDOC_URL=$(jq -r '.assets[] | select(.name|contains(".deb")) | .browser_download_url' <$SUBDIR/meta.json)

    case "$PANDOC_URL" in
        http*)
            curl -Lo $SUBDIR/pandoc.deb $PANDOC_URL
            ;;
        *)
            cat $SUBDIR/meta.json >&2
            ;;
    esac

    if [ -f $SUBDIR/pandoc.deb ]
    then
        dpkg-deb -x $SUBDIR/pandoc.deb pandoc
        exit 0
    fi

    [ "$TRAVIS_BRANCH" != 'master' ] && [ -z "${TRAVIS_TAG:-}" ] && exit 0

    sleep 60
done

exit 1
