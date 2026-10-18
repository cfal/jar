# Dependency Notices

`licenses/dependencies.txt` contains license texts and attributions for jar's
production and build dependencies, including transitive dependencies across
target platforms. Each binary may link only a subset of these dependencies.
Binary release archives include this notice bundle and jar's GPL-3.0-only license.

Regenerate the bundle after changing dependencies:

```sh
cargo install --locked cargo-about@0.9.2
python3 scripts/generate-release-notices.py
```

CI verifies that regeneration leaves no changes. The generator rejects warnings
without replacing the existing bundle. `scripts/check-release-notices.py` checks
that a binary archive contains the exact notice files before it is uploaded.

Each GitHub release also includes `jar-source.tar.gz`, containing the matching
source, Cargo.lock, vendored dependencies, and Cargo configuration for offline
builds. Extract it and run `cargo build --release --locked --offline --bin jr`.
Keep these notices when redistributing binaries and provide the corresponding
source under the applicable license terms.
