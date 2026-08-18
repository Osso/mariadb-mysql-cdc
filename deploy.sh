#!/bin/sh
set -eu

image_repo="${IMAGE_REPO:?IMAGE_REPO is required}"
depot_project_id="${DEPOT_PROJECT_ID:-jnnl97r4s7}"
tag="${1:-$(git rev-parse --short HEAD)}"
ops_repo="${OPS_REPO:-../ops}"
image="${image_repo}:${tag}"
trivy_image="ghcr.io/aquasecurity/trivy@sha256:7cced7cae583819fc7806d4cbc0dbbc7cad18b99f7d3e235192e6da8c091045c"
script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

stream_manifest_relative="infrastructure/ops/mariadb-mysql-cdc-stream.yaml"
stream_manifest="${ops_repo}/${stream_manifest_relative}"

require_clean_tree() {
    repo="$1"
    label="$2"
    if [ -n "$(git -C "$repo" status --short)" ]; then
        echo "$label has uncommitted changes; commit or stash them first" >&2
        git -C "$repo" status --short >&2
        exit 1
    fi
}

update_image_reference() {
    manifest="$1"
    IMAGE_REPO="$image_repo" IMMUTABLE_IMAGE="$immutable_image" \
        perl -0pi -e 's#image: \Q$ENV{IMAGE_REPO}\E:[^\s]+#image: $ENV{IMMUTABLE_IMAGE}#g' "$manifest"
}

require_clean_tree "." "mariadb-mysql-cdc repo"
require_clean_tree "$ops_repo" "ops repo"

if [ "${SKIP_VERIFIED_CHECKS:-0}" != "1" ]; then
    cargo fmt --check
    ./run-tests.sh
    cargo clippy --all-targets --all-features -- -D warnings
fi
depot build \
    --project "$depot_project_id" \
    --platform linux/amd64 \
    --tag "$image" \
    --push \
    .

digest="$(docker buildx imagetools inspect --format '{{.Manifest.Digest}}' "$image")"
if ! printf '%s\n' "$digest" | grep -Eq '^sha256:[0-9a-f]{64}$'; then
    echo "published image returned invalid digest: $digest" >&2
    exit 1
fi
immutable_image="${image}@${digest}"

if [ "${SKIP_RUNTIME_VERIFICATION:-0}" != "1" ]; then
    docker pull "$immutable_image"
    python3 "$script_dir/tests/verify_runtime_image.py" "$immutable_image"
    docker run --rm \
        --volume /var/run/docker.sock:/var/run/docker.sock \
        "$trivy_image" \
        image \
        --scanners vuln \
        --severity HIGH,CRITICAL \
        --ignore-unfixed \
        --skip-version-check \
        --exit-code 1 \
        "$immutable_image"
fi

update_image_reference "$stream_manifest"

git -C "$ops_repo" add "$stream_manifest_relative"
if git -C "$ops_repo" diff --cached --quiet; then
    echo "ops manifests already use $immutable_image"
else
    git -C "$ops_repo" commit -m "Deploy CDC image ${tag}"
fi

if [ "${PUSH_OPS:-1}" = "1" ]; then
    ops_branch="$(git -C "$ops_repo" branch --show-current)"
    git -C "$ops_repo" push -u origin "${ops_branch}:${ops_branch}"
else
    echo "PUSH_OPS=0; not pushing ops commit"
fi
