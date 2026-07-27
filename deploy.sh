#!/bin/sh
set -eu

image_repo="registry.digitalocean.com/globalcomix/mariadb-mysql-cdc"
tag="${1:-$(git rev-parse --short HEAD)}"
ops_repo="${OPS_REPO:-/syncthing/Sync/Projects/globalcomix/ops}"
image="${image_repo}:${tag}"

stream_manifest="${ops_repo}/infrastructure/ops/mariadb-mysql-cdc-stream.yaml"
catchup_manifest="${ops_repo}/infrastructure/ops/mariadb-mysql-cdc-catchup-existing-tables.yaml"
repair_manifest="${ops_repo}/infrastructure/ops/mariadb-mysql-cdc-repair-drift.yaml"

require_clean_tree() {
    repo="$1"
    label="$2"
    if [ -n "$(git -C "$repo" status --short)" ]; then
        echo "$label has uncommitted changes; commit or stash them first" >&2
        git -C "$repo" status --short >&2
        exit 1
    fi
}

update_image_tag() {
    manifest="$1"
    perl -0pi -e "s#image: ${image_repo}:[^\\s]+#image: ${image}#g" "$manifest"
}

remove_file_checkpoint_arg() {
    perl -0pi -e 's/\n        - --checkpoint-file\n        - \/var\/lib\/mariadb-mysql-cdc\/stream-checkpoint\.json//g' "$stream_manifest"
}

require_clean_tree "." "mariadb-mysql-cdc repo"
require_clean_tree "$ops_repo" "ops repo"

if [ "${SKIP_VERIFIED_CHECKS:-0}" != "1" ]; then
    cargo fmt --check
    cargo test
    cargo clippy --all-targets --all-features -- -D warnings
fi
cargo install --force --path .

docker build -t "$image" .
docker push "$image"

update_image_tag "$stream_manifest"
update_image_tag "$catchup_manifest"
update_image_tag "$repair_manifest"
remove_file_checkpoint_arg

git -C "$ops_repo" add "$stream_manifest" "$catchup_manifest" "$repair_manifest"
if git -C "$ops_repo" diff --cached --quiet; then
    echo "ops manifests already use $image"
else
    git -C "$ops_repo" commit -m "Deploy CDC image ${tag}"
fi

if [ "${PUSH_OPS:-1}" = "1" ]; then
    ops_branch="$(git -C "$ops_repo" branch --show-current)"
    git -C "$ops_repo" push -u origin "${ops_branch}:${ops_branch}"
else
    echo "PUSH_OPS=0; not pushing ops commit"
fi
