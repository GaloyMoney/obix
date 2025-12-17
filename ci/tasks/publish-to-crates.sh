#!/bin/bash

set -e

pushd repo

cat <<EOF | cargo login
${CRATES_API_TOKEN}
EOF

cargo publish -p obix-macros --all-features --no-verify
cargo publish -p obix --all-features --no-verify
