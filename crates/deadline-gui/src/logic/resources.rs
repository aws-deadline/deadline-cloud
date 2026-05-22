//! Resource loading logic for the config dialog.
//!
//! Pure functions that fetch and transform farm/queue/storage profile lists.
//! Testable without Qt — async functions use deadline-lib's API client.

use crate::logic::ResourceEntry;
use deadline_lib::api::{client, session};

/// Fetch farms for the given profile. Returns empty vec on API error.
pub async fn fetch_farms(profile: Option<&str>) -> Vec<ResourceEntry> {
    let dl = session::deadline_client(profile).await;
    let builder = client::apply_dcm_principal(dl.list_farms(), profile);
    let pages = match client::collect_paginated(builder.into_paginator().send()).await {
        Ok(p) => p,
        Err(_) => return Vec::new(),
    };
    let json_pages: Vec<serde_json::Value> = pages
        .iter()
        .flat_map(|p| p.farms())
        .map(|f| {
            serde_json::json!({
                "farmId": f.farm_id(),
                "displayName": f.display_name(),
            })
        })
        .collect();
    crate::logic::extract_farms(&[serde_json::json!({ "farms": json_pages })])
}

/// Fetch queues for the given farm. Returns empty vec if farm_id is empty or on API error.
pub async fn fetch_queues(profile: Option<&str>, farm_id: &str) -> Vec<ResourceEntry> {
    if farm_id.is_empty() {
        return Vec::new();
    }
    let dl = session::deadline_client(profile).await;
    let builder = client::apply_dcm_principal(dl.list_queues().farm_id(farm_id), profile);
    let pages = match client::collect_paginated(builder.into_paginator().send()).await {
        Ok(p) => p,
        Err(_) => return Vec::new(),
    };
    let json_pages: Vec<serde_json::Value> = pages
        .iter()
        .flat_map(|p| p.queues())
        .map(|q| {
            serde_json::json!({
                "queueId": q.queue_id(),
                "displayName": q.display_name(),
            })
        })
        .collect();
    crate::logic::extract_queues(&[serde_json::json!({ "queues": json_pages })])
}

/// Fetch storage profiles for the given queue, filtered to current OS.
/// Returns empty vec if farm_id or queue_id is empty, or on API error.
pub async fn fetch_storage_profiles(
    profile: Option<&str>,
    farm_id: &str,
    queue_id: &str,
) -> Vec<ResourceEntry> {
    if farm_id.is_empty() || queue_id.is_empty() {
        return Vec::new();
    }
    let dl = session::deadline_client(profile).await;
    let pages = match client::collect_paginated(
        dl.list_storage_profiles_for_queue()
            .farm_id(farm_id)
            .queue_id(queue_id)
            .into_paginator()
            .send(),
    )
    .await
    {
        Ok(p) => p,
        Err(_) => return Vec::new(),
    };
    let current_os = if cfg!(target_os = "linux") {
        "linux"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else {
        "windows"
    };
    let json_pages: Vec<serde_json::Value> = pages
        .iter()
        .flat_map(|p| p.storage_profiles())
        .map(|sp| {
            serde_json::json!({
                "storageProfileId": sp.storage_profile_id(),
                "displayName": sp.display_name(),
                "osFamily": sp.os_family().as_str(),
            })
        })
        .collect();
    crate::logic::extract_storage_profiles(
        &[serde_json::json!({ "storageProfiles": json_pages })],
        current_os,
    )
}

/// Determine the selected index for a resource list given a configured ID.
/// Returns the index of the matching entry, or 0 if not found (first entry).
/// If the ID is non-empty but not in the list, inserts it as a raw ID fallback
/// and returns its index.
pub fn resolve_selected_index(entries: &mut Vec<ResourceEntry>, configured_id: &str) -> usize {
    if configured_id.is_empty() {
        return 0;
    }
    if let Some(idx) = entries.iter().position(|e| e.id == configured_id) {
        return idx;
    }
    // Fallback: insert raw ID at position 0
    entries.insert(
        0,
        ResourceEntry {
            display_name: configured_id.to_string(),
            id: configured_id.to_string(),
        },
    );
    0
}

/// After resolving the selected index, return the ID at that index.
/// Used for cascading: after farms load, get the selected farm_id to fetch queues.
pub fn selected_id_at(entries: &[ResourceEntry], index: usize) -> &str {
    entries.get(index).map(|e| e.id.as_str()).unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─────────────────────────────────────────────────────────────
    // fetch_farms (async, uses test server)
    // ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn fetch_farms_empty_farm_id_not_required() {
        let result = fetch_farms(Some("nonexistent-profile")).await;
        assert!(result.is_empty());
    }

    // ─────────────────────────────────────────────────────────────
    // fetch_queues
    // ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn fetch_queues_empty_farm_id_returns_empty() {
        let result = fetch_queues(Some("any"), "").await;
        assert!(result.is_empty());
    }

    // ─────────────────────────────────────────────────────────────
    // fetch_storage_profiles
    // ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn fetch_storage_profiles_empty_farm_id_returns_empty() {
        let result = fetch_storage_profiles(Some("any"), "", "queue-123").await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn fetch_storage_profiles_empty_queue_id_returns_empty() {
        let result = fetch_storage_profiles(Some("any"), "farm-123", "").await;
        assert!(result.is_empty());
    }

    // ─────────────────────────────────────────────────────────────
    // resolve_selected_index
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn resolve_selected_index_finds_matching_id() {
        let mut entries = vec![
            ResourceEntry {
                display_name: "Farm A".to_string(),
                id: "farm-aaa".to_string(),
            },
            ResourceEntry {
                display_name: "Farm B".to_string(),
                id: "farm-bbb".to_string(),
            },
        ];
        let idx = resolve_selected_index(&mut entries, "farm-bbb");
        assert_eq!(idx, 1);
    }

    #[test]
    fn resolve_selected_index_empty_id_returns_zero() {
        let mut entries = vec![ResourceEntry {
            display_name: "Farm A".to_string(),
            id: "farm-aaa".to_string(),
        }];
        let idx = resolve_selected_index(&mut entries, "");
        assert_eq!(idx, 0);
    }

    #[test]
    fn resolve_selected_index_missing_id_inserts_raw_fallback() {
        let mut entries = vec![ResourceEntry {
            display_name: "Farm A".to_string(),
            id: "farm-aaa".to_string(),
        }];
        let idx = resolve_selected_index(&mut entries, "farm-unknown");
        assert_eq!(entries[idx].id, "farm-unknown");
        assert_eq!(entries[idx].display_name, "farm-unknown");
    }

    #[test]
    fn resolve_selected_index_none_selected_entry() {
        let mut entries = vec![
            ResourceEntry {
                display_name: "<none selected>".to_string(),
                id: "".to_string(),
            },
            ResourceEntry {
                display_name: "Profile X".to_string(),
                id: "sp-xxx".to_string(),
            },
        ];
        let idx = resolve_selected_index(&mut entries, "");
        assert_eq!(idx, 0);
        assert_eq!(entries[idx].display_name, "<none selected>");
    }

    // ─────────────────────────────────────────────────────────────
    // selected_id_at
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn selected_id_at_valid_index() {
        let entries = vec![
            ResourceEntry {
                display_name: "A".to_string(),
                id: "farm-aaa".to_string(),
            },
            ResourceEntry {
                display_name: "B".to_string(),
                id: "farm-bbb".to_string(),
            },
        ];
        assert_eq!(selected_id_at(&entries, 1), "farm-bbb");
    }

    #[test]
    fn selected_id_at_out_of_bounds_returns_empty() {
        let entries = vec![ResourceEntry {
            display_name: "A".to_string(),
            id: "farm-aaa".to_string(),
        }];
        assert_eq!(selected_id_at(&entries, 5), "");
    }

    #[test]
    fn selected_id_at_empty_list_returns_empty() {
        let entries: Vec<ResourceEntry> = vec![];
        assert_eq!(selected_id_at(&entries, 0), "");
    }
}
