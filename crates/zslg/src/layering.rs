//! The layering test: every workspace crate declares a role, and its
//! dependencies must be ones that role allows.
//!
//! ```text
//!   library < types < ports < procedure < implementation < assembly < facade
//!                  \ config /     fake (beside ports, test-only)
//! ```
//!
//! A crate's role is `[package.metadata.layer] role = "..."` in its
//! `Cargo.toml`; names suggest it, they do not decide it. See
//! `docs/logs/2026-11-07_design_crate_split.md` §2.1 and §8.3.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Role {
    Library,
    Types,
    Ports,
    Procedure,
    Config,
    Fake,
    Implementation,
    Assembly,
    Facade,
    Application,
}

impl Role {
    fn parse(s: &str) -> Option<Role> {
        Some(match s {
            "library" => Role::Library,
            "types" => Role::Types,
            "ports" => Role::Ports,
            "procedure" => Role::Procedure,
            "config" => Role::Config,
            "fake" => Role::Fake,
            "implementation" => Role::Implementation,
            "assembly" => Role::Assembly,
            "facade" => Role::Facade,
            "application" => Role::Application,
            _ => return None,
        })
    }

    /// Roles a crate of this role may depend on (normal dependencies).
    fn may_depend_on(self) -> &'static [Role] {
        use Role::*;
        match self {
            Library => &[Library],
            Types => &[Library, Types],
            Ports => &[Library, Types],
            Procedure => &[Library, Types, Ports],
            Config => &[Library, Types],
            Fake => &[Library, Types, Ports],
            Implementation => &[Library, Types, Ports, Procedure, Config],
            Assembly => &[
                Library,
                Types,
                Ports,
                Procedure,
                Config,
                Implementation,
                Assembly,
            ],
            Facade => &[Library, Ports, Config, Assembly, Facade],
            Application => &[
                Library,
                Types,
                Ports,
                Procedure,
                Config,
                Fake,
                Implementation,
                Assembly,
                Facade,
                Application,
            ],
        }
    }

    /// Roles a crate of this role may use as dev-dependency, beyond the normal ones.
    fn dev_extra(self) -> &'static [Role] {
        use Role::*;
        match self {
            Library | Types | Ports | Procedure | Config | Fake | Implementation => &[Fake],
            Assembly | Facade | Application => {
                &[Fake, Implementation, Assembly, Facade, Procedure, Types]
            }
        }
    }
}

struct Krate {
    name: String,
    role: Role,
    deps: Vec<Dep>,
}

struct Dep {
    name: String,
    dev: bool,
    optional: bool,
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn workspace() -> Vec<Krate> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let out = Command::new(cargo)
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(root().join("Cargo.toml"))
        .output()
        .expect("run cargo metadata");
    assert!(out.status.success(), "cargo metadata failed");
    let v: Value = serde_json::from_slice(&out.stdout).expect("parse metadata");
    let members: BTreeSet<String> = v["packages"]
        .as_array()
        .expect("packages")
        .iter()
        .map(|p| p["name"].as_str().expect("name").to_string())
        .collect();
    let mut result = Vec::new();
    for p in v["packages"].as_array().expect("packages") {
        let name = p["name"].as_str().expect("name").to_string();
        let role_str = p["metadata"]["layer"]["role"].as_str();
        let role = match role_str.and_then(Role::parse) {
            Some(r) => r,
            None => panic!("{name}: missing or unknown [package.metadata.layer] role"),
        };
        let deps = p["dependencies"]
            .as_array()
            .expect("dependencies")
            .iter()
            .filter(|d| members.contains(d["name"].as_str().expect("dep name")))
            .filter(|d| d["kind"].as_str() != Some("build"))
            .map(|d| Dep {
                name: d["name"].as_str().expect("dep name").to_string(),
                dev: d["kind"].as_str() == Some("dev"),
                optional: d["optional"].as_bool().unwrap_or(false),
            })
            .collect();
        result.push(Krate { name, role, deps });
    }
    result
}

/// `zslg`, `nvzn` for `zslg-*`/`nvzn-*` crates and their facades; `None` for
/// unprefixed crates (libraries and the application).
fn family(name: &str) -> Option<&str> {
    let head = name.split('-').next().expect("name");
    matches!(head, "zslg" | "nvzn").then_some(head)
}

#[test]
fn every_dependency_is_allowed_by_the_role_table() {
    let krates = workspace();
    let roles: BTreeMap<&str, Role> = krates.iter().map(|k| (k.name.as_str(), k.role)).collect();
    let mut violations = Vec::new();
    for k in &krates {
        for d in &k.deps {
            // From the nvzn family's point of view the `zslg` facade is a library
            // (D11); `families_do_not_reach_into_each_other` pins that nothing else
            // of zslg is reachable.
            let to = if family(&k.name) == Some("nvzn") && d.name == "zslg" {
                Role::Library
            } else {
                roles[d.name.as_str()]
            };
            let mut allowed: Vec<Role> = k.role.may_depend_on().to_vec();
            if d.dev {
                allowed.extend_from_slice(k.role.dev_extra());
            }
            // An assembly or facade may use fakes behind a feature (`testing`),
            // never otherwise: the feature is how it exports test support for
            // the crates above it.
            if matches!(k.role, Role::Assembly | Role::Facade) && to == Role::Fake && d.optional {
                continue;
            }
            // The ports -> ports edges: `ReplicaClient` bundles four groups, and
            // the nvzn store port is written against the projection's read port.
            // `only_declared_ports_build_on_ports` pins who may have one.
            if to == Role::Ports
                && (k.name == "zslg-net"
                    || PORTS_ON_PORTS.contains(&(k.name.as_str(), d.name.as_str())))
            {
                continue;
            }
            if !allowed.contains(&to) {
                violations.push(format!(
                    "{} ({:?}) -> {} ({:?}){}",
                    k.name,
                    k.role,
                    d.name,
                    to,
                    if d.dev { " [dev]" } else { "" }
                ));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "layering violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn families_do_not_reach_into_each_other() {
    let krates = workspace();
    let mut violations = Vec::new();
    for k in &krates {
        for d in &k.deps {
            let (from, to) = (family(&k.name), family(&d.name));
            let ok = match (from, to) {
                (_, None) => true,
                (None, Some(_)) => k.role == Role::Application,
                (Some(a), Some(b)) if a == b => true,
                // nvzn sees zslg only through its facade.
                (Some("nvzn"), Some("zslg")) => d.name == "zslg",
                _ => false,
            };
            if !ok {
                violations.push(format!("{} -> {}", k.name, d.name));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "family violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn no_cross_group_dependency_on_a_types_crate_from_above_the_ports_layer() {
    let krates = workspace();
    let above = [
        Role::Procedure,
        Role::Fake,
        Role::Implementation,
        Role::Assembly,
        Role::Facade,
    ];
    let mut violations = Vec::new();
    for k in krates.iter().filter(|k| above.contains(&k.role)) {
        for d in &k.deps {
            if d.name.ends_with("-types")
                && d.name != "zslg-lifecycle-types"
                && !d.name.starts_with("nvzn-")
            {
                violations.push(format!(
                    "{} -> {} (import through the ports crate)",
                    k.name, d.name
                ));
            }
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

/// The ports crates that may depend on another ports crate, besides `zslg-net`
/// (which bundles four groups): `nvzn-store` is the write side of persistence
/// and builds on the read port and projection model of `nvzn-projection`.
const PORTS_ON_PORTS: &[(&str, &str)] = &[("nvzn-store", "nvzn-projection")];

#[test]
fn only_declared_ports_build_on_ports() {
    let krates = workspace();
    let roles: BTreeMap<&str, Role> = krates.iter().map(|k| (k.name.as_str(), k.role)).collect();
    let mut violations = Vec::new();
    for k in krates.iter().filter(|k| k.role == Role::Ports) {
        for d in k.deps.iter().filter(|d| !d.dev) {
            if roles[d.name.as_str()] == Role::Ports
                && !(k.name == "zslg-net"
                    || PORTS_ON_PORTS.contains(&(k.name.as_str(), d.name.as_str())))
            {
                violations.push(format!("{} -> {}", k.name, d.name));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "ports -> ports edges:\n{}",
        violations.join("\n")
    );
}

#[test]
fn workspace_libraries_do_not_share_a_name_with_a_registry_package() {
    let krates = workspace();
    let libs: BTreeSet<&str> = krates
        .iter()
        .filter(|k| k.role == Role::Library)
        .map(|k| k.name.as_str())
        .collect();
    let lock = std::fs::read_to_string(root().join("Cargo.lock")).expect("Cargo.lock");
    let mut clashes = Vec::new();
    let mut name = None::<String>;
    for line in lock.lines() {
        if let Some(n) = line.strip_prefix("name = \"") {
            name = Some(n.trim_end_matches('"').to_string());
        } else if line.starts_with("source = \"registry+")
            && let Some(n) = &name
            && libs.contains(n.as_str())
        {
            clashes.push(n.clone());
        }
    }
    assert!(
        clashes.is_empty(),
        "registry packages named like workspace libraries: {clashes:?}"
    );
}
