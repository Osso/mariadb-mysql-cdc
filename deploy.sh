#!/bin/sh
set -eu

image_repo="${IMAGE_REPO:?IMAGE_REPO is required}"
base_image="${BASE_IMAGE:?BASE_IMAGE is required}"
depot_project_id="${DEPOT_PROJECT_ID:-jnnl97r4s7}"
tag="${1:-$(git rev-parse --short HEAD)}"
ops_repo="${OPS_REPO:-../ops}"
image="${image_repo}:${tag}"

stream_manifest="${ops_repo}/infrastructure/ops/mariadb-mysql-cdc-stream.yaml"

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

require_clean_tree "." "mariadb-mysql-cdc repo"
require_clean_tree "$ops_repo" "ops repo"

if [ "${SKIP_VERIFIED_CHECKS:-0}" != "1" ]; then
    cargo fmt --check
    cargo test
    cargo clippy --all-targets --all-features -- -D warnings
fi
depot build \
    --project "$depot_project_id" \
    --platform linux/amd64 \
    --build-arg "BASE_IMAGE=$base_image" \
    --tag "$image" \
    --push \
    .

update_image_tag "$stream_manifest"

git -C "$ops_repo" add "$stream_manifest"
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
