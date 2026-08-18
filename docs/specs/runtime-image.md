# Runtime container image

The production binary is packaged in a fixed, minimal Ubuntu runtime independent of the MariaDB server image. Build and operator usage are documented in the [README](../../README.md#runtime-image-verification).

## What it must do

### Runtime contract

- [x] Use Ubuntu 24.04 as the final runtime operating system, pinned to verified OCI index digest `sha256:561618e2c15bf2397621dd04f96926663a3b5616c189cf7e38db7e82f5c538ea`.
- [x] Run by default as fixed numeric UID/GID `65532:65532`, never root.
- [x] Preserve direct `mariadb-mysql-cdc` entrypoint execution without a privilege-drop wrapper.
- [x] Exclude the unused `gosu` executable.
- [x] Provide a non-empty system CA certificate bundle and permit UID/GID `65532:65532` to read a separately mounted read-only CA file without write access.
- [x] Install the runtime packages required by the built binary: `ca-certificates`, `libc6`, `libgcc-s1`, `libmariadb3`, `libssl3t64`, and `zlib1g`, plus package-manager dependencies.
- [x] Resolve every dynamic library required by `/usr/local/bin/mariadb-mysql-cdc` and execute the binary successfully.
- [x] Update the Ubuntu package index, upgrade installed runtime packages, install without recommended extras, and remove package-list cache from the final layer.

### Build and deployment contract

- [x] Build the final runtime from the Dockerfile's fixed base; no `BASE_IMAGE` argument or environment variable selects the runtime.
- [x] Keep the direct image build compatible with Docker BuildKit cache mounts used by the Rust builder.
- [x] Require `IMAGE_REPO` for `deploy.sh` while allowing the existing tag, Depot project, ops checkout, check, and push controls.
- [x] Unless `SKIP_VERIFIED_CHECKS=1`, run `cargo fmt --check`, the repository `./run-tests.sh` path, and Clippy with warnings denied before building. The repository test path runs both `cargo test` and `python3 -m unittest tests/test_deploy_script.py`.
- [x] Do not forward a `BASE_IMAGE` build argument to Depot.
- [x] After Depot publishes the candidate tag, resolve its immutable digest with `docker buildx imagetools inspect --format '{{.Manifest.Digest}}'`, reject an invalid digest, and pull the exact `tag@digest`.
- [x] Run `tests/verify_runtime_image.py` against the pulled `tag@digest` before any ops manifest mutation.
- [x] Scan the exact digest through the Docker socket with pinned Trivy 0.73.0 image `ghcr.io/aquasecurity/trivy@sha256:7cced7cae583819fc7806d4cbc0dbbc7cad18b99f7d3e235192e6da8c091045c`, scanners `vuln`, severities `HIGH,CRITICAL`, `--ignore-unfixed`, `--skip-version-check`, and a nonzero exit-code gate.
- [x] Permit registry publication before verification, but edit, commit, and push ops only after both runtime verification and Trivy succeed.
- [x] Write the verified immutable `repo:tag@sha256:...` reference to the live stream manifest; never admit the mutable tag alone.
- [x] Leave the ops manifest and commit unchanged when the Trivy gate fails.

## How it works

- [README runtime image verification](../../README.md#runtime-image-verification)
- [README deployment](../../README.md#deployment)

## Implementation inventory

- `Dockerfile` — builds the Rust binary, upgrades the Ubuntu 24.04 runtime, installs runtime dependencies, and selects numeric UID/GID `65532:65532`.
- `deploy.sh` — runs the repository verification path, publishes the fixed-base image, resolves and verifies its immutable digest, runs the pinned Trivy gate, and only then admits the digest-pinned reference to ops reconciliation.
- `run-tests.sh` — runs Rust tests and the Python deploy-contract suite through the repository test path.
- `tests/verify_runtime_image.py` — inspects and executes a built image at the container boundary.
- `tests/test_deploy_script.py` — executes `deploy.sh` with isolated Git repositories and a fake Depot CLI to assert its external build contract.

## Tests asserting this spec

- `tests/verify_runtime_image.py` asserts operating system identity, image user/entrypoint metadata, runtime UID/GID, CA bundle, read-only mounted CA-file readability, required packages, dynamic-link resolution, binary execution, and `gosu` absence.
- `tests/test_deploy_script.py` asserts deployment succeeds without `BASE_IMAGE`, runs repository tests between formatting and Clippy, includes itself through `run-tests.sh`, verifies and scans the exact published digest before ops mutation, writes the immutable reference, and proves a failed Trivy gate leaves the ops manifest and commit unchanged.

## Known gaps (current cycle)

- [ ] Execute the new published-digest verification and Trivy gate against the next real Depot candidate.
- [ ] After deployment, prove the live stream reads its mounted target CA, starts successfully, and advances its checkpoint under UID/GID `65532:65532`.

## Out of scope

- Publishing, deployment, ops manifest changes, production access, or registry mutation.
- Changing Rust linkage, CDC behavior, stream arguments, or database privileges.
- Adding a shell entrypoint or runtime privilege-escalation path.
