## [unreleased]

### 🚀 Features

- Todo
- Generating CHANGELOG.md file

### 🗑️ Removal

- *(git-cliff)* Temporary version override
- Todo comment about CI/CD variables

### ♻️ Refactor

- Move crate-level dependency declarations to top-level workspace

### 🐛 Bug Fixes

- Compare against lum instead of lum_boxtypes in release CI/CD workflow

### ⬆️ Dependency Updates

- *(deps)* Bump tokio from 1.52.3 to 1.53.1 (#13)
- *(deps)* Bump serde from 1.0.228 to 1.0.229 (#12)
- *(deps)* Bump actions/checkout from 6.0.2 to 7.0.1 (#11)
- *(deps)* Bump uuid from 1.23.4 to 1.24.0 (#8)
- *(deps)* Bump rustls from 0.23.41 to 0.23.42 (#7)
- *(deps)* Bump humantime from 2.3.0 to 2.4.0 (#5)
- *(deps)* Bump actions-rust-lang/setup-rust-toolchain from 1.16.1 to 1.17.0 (#4)
## [0.4.0] - 2026-06-25

### 🚀 Features

- *(lum)* Import lum project from archived repo (lum-archived)
- *(git-cliff)* Temporary initial tag definition

### 🗑️ Removal

- Crate-level files that are now top-level in workspace
- Crate-level .gitignore files
- *(github-workflow)* Unneeded step in job

### ♻️ Refactor

- *(lum)* Move projects to monorepo
- Workflows for workspace
- *(git-cliff)* Regexes for parsing commit messages
- *(versions)* Temporarily use 0.3.0 as version for first release CI/CD job

### ⬆️ Dependency Updates

- *(deps)* Bump uuid from 1.23.3 to 1.23.4 (#2)

### ⚙️ Other

- *(Cargo.toml)* Version 0.3.0 -> 0.4.0
<!-- Developed with ❤️ -->
