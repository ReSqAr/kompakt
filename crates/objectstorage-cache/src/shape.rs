//! The two-level weights: what each app and each of its kinds should hold.
//!
//! Weights are **targets for eviction**, not reservations. The cache is
//! filled by whatever is used; only when it is over its soft limit do the
//! weights decide who leaves: the group (app, kind) furthest above its
//! target. An app that is idle holds nothing and so lends its share; the
//! first reads of an app that was crowded out make its neighbours pay until
//! it is back at its target.
//!
//! ```text
//!   target(app)        = total × weight(app) / Σ weight(apps with kinds)
//!   target(app, kind)  = target(app) × weight(kind) / Σ weight(kinds of app)
//! ```
//!
//! `total` is what the cache holds when it evicts (at least the soft limit).

use crate::disk::Group;
use parking_lot::Mutex;
use std::collections::HashMap;

#[derive(Default)]
pub(crate) struct Shape {
    apps: Mutex<Vec<AppShape>>,
}

struct AppShape {
    name: &'static str,
    weight: u32,
    kinds: Vec<KindShape>,
}

struct KindShape {
    name: &'static str,
    weight: u32,
}

/// A frozen copy of the weights, for one eviction decision.
#[derive(Default)]
pub(crate) struct Targets {
    app_total: u128,
    apps: HashMap<&'static str, (u32, u128)>,
    kinds: HashMap<Group, u32>,
}

impl Shape {
    /// An app takes part once it has a kind; its weight is 1 until the
    /// environment says otherwise.
    pub(crate) fn add_kind(&self, app: &'static str, kind: &'static str, weight: u32) -> bool {
        let mut apps = self.apps.lock();
        let at = match apps.iter().position(|a| a.name == app) {
            Some(at) => at,
            None => {
                apps.push(AppShape {
                    name: app,
                    weight: 1,
                    kinds: Vec::new(),
                });
                apps.len().saturating_sub(1)
            }
        };
        let Some(shape) = apps.get_mut(at) else {
            return false;
        };
        if shape.kinds.iter().any(|k| k.name == kind) {
            return false;
        }
        shape.kinds.push(KindShape { name: kind, weight });
        true
    }

    pub(crate) fn set_kind_weight(&self, app: &str, kind: &str, weight: u32) {
        let mut apps = self.apps.lock();
        if let Some(k) = apps
            .iter_mut()
            .filter(|a| a.name == app)
            .flat_map(|a| a.kinds.iter_mut())
            .find(|k| k.name == kind)
        {
            k.weight = weight;
        }
    }

    pub(crate) fn kind_weight(&self, app: &str, kind: &str) -> u32 {
        self.apps
            .lock()
            .iter()
            .filter(|a| a.name == app)
            .flat_map(|a| a.kinds.iter())
            .find(|k| k.name == kind)
            .map_or(0, |k| k.weight)
    }

    /// The environment's weight of an app (activation only).
    pub(crate) fn set_app_weight(&self, app: &str, weight: u32) {
        if let Some(a) = self.apps.lock().iter_mut().find(|a| a.name == app) {
            a.weight = weight;
        }
    }

    pub(crate) fn app_weight(&self, app: &str) -> u32 {
        self.apps
            .lock()
            .iter()
            .find(|a| a.name == app)
            .map_or(0, |a| a.weight)
    }

    /// The apps that registered kinds.
    pub(crate) fn apps(&self) -> Vec<&'static str> {
        self.apps
            .lock()
            .iter()
            .filter(|a| !a.kinds.is_empty())
            .map(|a| a.name)
            .collect()
    }

    pub(crate) fn groups(&self) -> Vec<Group> {
        self.apps
            .lock()
            .iter()
            .flat_map(|a| {
                a.kinds.iter().map(|k| Group {
                    app: a.name,
                    kind: k.name,
                })
            })
            .collect()
    }

    pub(crate) fn targets(&self) -> Targets {
        let apps = self.apps.lock();
        let mut t = Targets::default();
        for a in apps.iter().filter(|a| !a.kinds.is_empty()) {
            let kind_total: u128 = a.kinds.iter().map(|k| u128::from(k.weight)).sum();
            t.app_total = t.app_total.saturating_add(u128::from(a.weight));
            t.apps.insert(a.name, (a.weight, kind_total));
            for k in &a.kinds {
                t.kinds.insert(
                    Group {
                        app: a.name,
                        kind: k.name,
                    },
                    k.weight,
                );
            }
        }
        t
    }
}

impl Targets {
    /// What `group` should hold of `total` bytes (0 for a weightless one).
    pub(crate) fn target(&self, group: Group, total: u64) -> u64 {
        let (Some(&(app_w, kind_total)), Some(&kind_w)) =
            (self.apps.get(group.app), self.kinds.get(&group))
        else {
            return 0;
        };
        let numerator = u128::from(total)
            .saturating_mul(u128::from(app_w))
            .saturating_mul(u128::from(kind_w));
        let denominator = self.app_total.saturating_mul(kind_total);
        numerator
            .checked_div(denominator)
            .map_or(0, |t| u64::try_from(t).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;

    fn g(app: &'static str, kind: &'static str) -> Group {
        Group { app, kind }
    }

    #[test]
    fn targets_follow_the_two_levels_and_only_apps_with_kinds_count() {
        let s = Shape::default();
        s.add_kind("app", "thumbnail", 75);
        s.add_kind("app", "preview", 25);
        s.add_kind("other", "frame", 1);
        s.set_app_weight("app", 90);
        s.set_app_weight("other", 10);
        let t = s.targets();
        assert_eq!(t.target(g("app", "thumbnail"), 1000), 675);
        assert_eq!(t.target(g("app", "preview"), 1000), 225);
        assert_eq!(t.target(g("other", "frame"), 1000), 100);
        assert_eq!(s.apps(), vec!["app", "other"]);
    }

    #[test]
    fn a_kind_is_registered_once() {
        let s = Shape::default();
        assert!(s.add_kind("a", "k", 1));
        assert!(!s.add_kind("a", "k", 2));
        assert_eq!(s.kind_weight("a", "k"), 1);
    }

    #[test]
    fn weight_zero_and_all_zero_have_no_target() {
        let s = Shape::default();
        s.add_kind("a", "k", 0);
        assert_eq!(s.targets().target(g("a", "k"), 1000), 0);
    }
}
