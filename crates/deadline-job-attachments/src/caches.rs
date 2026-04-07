use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use deadline_models::errors::JobAttachmentsError;
use rusqlite::Connection;

use crate::asset_manifests::HashAlgorithm;

const CONFIG_ROOT: &str = ".deadline";
const COMPONENT_NAME: &str = "job_attachments";

pub fn default_cache_dir() -> Option<String> {
    std::env::var("HOME").ok().map(|home| {
        std::path::Path::new(&home)
            .join(CONFIG_ROOT)
            .join(COMPONENT_NAME)
            .to_string_lossy()
            .into_owned()
    })
}

/// Number of retry attempts for database lock contention.
const RETRY_ATTEMPTS: usize = 3;

/// Opens a SQLite database, sets WAL journal mode, and creates the table if missing.
/// Retries up to RETRY_ATTEMPTS times with jittered delay on lock contention.
fn open_db(db_path: &str, table_name: &str, create_query: &str) -> Result<Connection, JobAttachmentsError> {
    let mut last_err = None;
    for attempt in 0..RETRY_ATTEMPTS {
        match Connection::open(db_path) {
            Ok(conn) => {
                conn.execute_batch("PRAGMA journal_mode=WAL;")
                    .map_err(|e| JobAttachmentsError::AssetSync(format!("WAL mode failed: {e}")))?;
                if conn
                    .execute(&format!("SELECT * FROM {table_name} LIMIT 0"), [])
                    .is_err()
                {
                    conn.execute_batch(create_query)
                        .map_err(|e| JobAttachmentsError::AssetSync(format!("Create table failed: {e}")))?;
                }
                return Ok(conn);
            }
            Err(e) => {
                last_err = Some(e);
                if attempt < RETRY_ATTEMPTS - 1 {
                    let delay = 0.5 + rand_jitter();
                    std::thread::sleep(std::time::Duration::from_secs_f64(delay));
                }
            }
        }
    }
    Err(JobAttachmentsError::AssetSync(format!(
        "Could not access cache file after {RETRY_ATTEMPTS} retry attempts: {db_path}: {}",
        last_err.unwrap()
    )))
}

/// Jitter between 0.0 and 1.0 seconds, derived from system clock nanoseconds.
fn rand_jitter() -> f64 {
    // Simple jitter between 0.0 and 1.0 using system time nanoseconds
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos % 1000) as f64 / 1000.0
}

// --- HashCacheEntry ---

/// An entry in the hash cache. Represents either a whole-file hash
/// (range_start=0, range_end=-1) or a byte-range hash.
#[derive(Debug, Clone)]
pub struct HashCacheEntry {
    pub file_path: String,
    pub hash_algorithm: HashAlgorithm,
    pub file_hash: String,
    pub last_modified_time: i64, // nanoseconds since epoch
    pub range_start: i64,
    pub range_end: i64,
}

impl HashCacheEntry {
    pub fn new(
        file_path: String,
        hash_algorithm: HashAlgorithm,
        file_hash: String,
        last_modified_time: i64,
        range_start: i64,
        range_end: i64,
    ) -> Result<Self, JobAttachmentsError> {
        if range_end != -1 && range_end <= range_start {
            return Err(JobAttachmentsError::AssetSync(format!(
                "For byte-range entries, range_end ({range_end}) must be greater than range_start ({range_start})"
            )));
        }
        Ok(Self {
            file_path,
            hash_algorithm,
            file_hash,
            last_modified_time,
            range_start,
            range_end,
        })
    }
}

// --- HashCache ---

/// SQLite-backed cache for file hashes. Uses `hashesV5` table with integer
/// nanosecond timestamps (improvement over Python's `hashesV4` string timestamps).
/// Thread-safe via `Mutex<Connection>`. WAL journal mode for concurrent reads.
pub struct HashCache {
    conn: Mutex<Connection>,
}

const HASH_TABLE: &str = "hashesV5";

impl HashCache {
    pub fn new(cache_dir: &str) -> Result<Self, JobAttachmentsError> {
        std::fs::create_dir_all(cache_dir)
            .map_err(|e| JobAttachmentsError::AssetSync(format!("Cannot create cache dir: {e}")))?;
        let db_path = std::path::Path::new(cache_dir)
            .join("hash_cache.db")
            .to_string_lossy()
            .into_owned();
        let create_query = format!(
            "CREATE TABLE {HASH_TABLE}(\
             file_path BLOB, \
             hash_algorithm TEXT, \
             range_start INTEGER, \
             range_end INTEGER, \
             file_hash TEXT, \
             last_modified_time INTEGER, \
             PRIMARY KEY (file_path, hash_algorithm, range_start, range_end))"
        );
        let conn = open_db(&db_path, HASH_TABLE, &create_query)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn get_entry(
        &self,
        file_path: &str,
        hash_algorithm: HashAlgorithm,
        range_start: i64,
        range_end: i64,
    ) -> Option<HashCacheEntry> {
        let conn = self.conn.lock().unwrap();
        let encoded_path = file_path.as_bytes();
        let mut stmt = conn
            .prepare(&format!(
                "SELECT file_path, hash_algorithm, range_start, range_end, file_hash, last_modified_time \
                 FROM {HASH_TABLE} \
                 WHERE file_path=?1 AND hash_algorithm=?2 AND range_start=?3 AND range_end=?4"
            ))
            .ok()?;
        stmt.query_row(
            rusqlite::params![
                encoded_path,
                hash_algorithm.to_string(),
                range_start,
                range_end,
            ],
            |row| {
                let path_bytes: Vec<u8> = row.get(0)?;
                let file_path = String::from_utf8_lossy(&path_bytes).into_owned();
                let alg_str: String = row.get(1)?;
                let hash_algorithm: HashAlgorithm = alg_str.parse().unwrap_or(HashAlgorithm::Xxh128);
                Ok(HashCacheEntry {
                    file_path,
                    hash_algorithm,
                    file_hash: row.get(4)?,
                    last_modified_time: row.get(5)?,
                    range_start: row.get(2)?,
                    range_end: row.get(3)?,
                })
            },
        )
        .ok()
    }

    pub fn put_entry(&self, entry: &HashCacheEntry) {
        let conn = self.conn.lock().unwrap();
        let encoded_path = entry.file_path.as_bytes();
        conn.execute(
            &format!(
                "INSERT OR REPLACE INTO {HASH_TABLE} \
                 (file_path, hash_algorithm, range_start, range_end, file_hash, last_modified_time) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
            ),
            rusqlite::params![
                encoded_path,
                entry.hash_algorithm.to_string(),
                entry.range_start,
                entry.range_end,
                entry.file_hash,
                entry.last_modified_time,
            ],
        )
        .unwrap_or_else(|e| {
            log::warn!("Failed to write hash cache entry: {e}");
            0
        });
    }
}

// --- S3CheckCacheEntry ---

/// An entry in the S3 check cache. Records when an S3 key was last confirmed to exist.
#[derive(Debug, Clone)]
pub struct S3CheckCacheEntry {
    pub s3_key: String,
    pub last_seen_time: String, // Unix timestamp as float string (Python compat)
}

/// SQLite-backed cache tracking which S3 object keys exist in content-addressed storage.
/// Entries expire after 30 days, evaluated at lookup time. Uses `s3checkV1` table
/// (same schema as Python for cross-tool compatibility).
pub struct S3CheckCache {
    conn: Mutex<Connection>,
}

const S3_CHECK_TABLE: &str = "s3checkV1";
const ENTRY_EXPIRY_DAYS: f64 = 30.0;

impl S3CheckCache {
    pub fn new(cache_dir: &str) -> Result<Self, JobAttachmentsError> {
        std::fs::create_dir_all(cache_dir)
            .map_err(|e| JobAttachmentsError::AssetSync(format!("Cannot create cache dir: {e}")))?;
        let db_path = std::path::Path::new(cache_dir)
            .join("s3_check_cache.db")
            .to_string_lossy()
            .into_owned();
        let create_query = format!(
            "CREATE TABLE {S3_CHECK_TABLE}(s3_key TEXT PRIMARY KEY, last_seen_time TEXT)"
        );
        let conn = open_db(&db_path, S3_CHECK_TABLE, &create_query)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn get_entry(&self, s3_key: &str) -> Option<S3CheckCacheEntry> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(&format!(
                "SELECT s3_key, last_seen_time FROM {S3_CHECK_TABLE} WHERE s3_key=?1"
            ))
            .ok()?;
        let entry = stmt
            .query_row(rusqlite::params![s3_key], |row| {
                Ok(S3CheckCacheEntry {
                    s3_key: row.get(0)?,
                    last_seen_time: row.get(1)?,
                })
            })
            .ok()?;

        // Check expiry
        let last_seen: f64 = match entry.last_seen_time.parse() {
            Ok(v) => v,
            Err(_) => {
                log::warn!(
                    "Timestamp for S3 key {} is not valid. Ignoring.",
                    entry.s3_key
                );
                return None;
            }
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let age_days = (now - last_seen) / 86400.0;
        if age_days >= ENTRY_EXPIRY_DAYS {
            return None;
        }

        Some(entry)
    }

    pub fn put_entry(&self, entry: &S3CheckCacheEntry) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            &format!(
                "INSERT OR REPLACE INTO {S3_CHECK_TABLE} (s3_key, last_seen_time) VALUES (?1, ?2)"
            ),
            rusqlite::params![entry.s3_key, entry.last_seen_time],
        )
        .unwrap_or_else(|e| {
            log::warn!("Failed to write S3 check cache entry: {e}");
            0
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // === §24: HashCache construction ===

    #[test]
    fn hash_cache_new_creates_db() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HashCache::new(dir.path().to_str().unwrap()).unwrap();
        drop(cache);
        assert!(dir.path().join("hash_cache.db").exists());
    }

    #[test]
    fn hash_cache_new_custom_dir() {
        let dir = tempfile::tempdir().unwrap();
        let custom = dir.path().join("custom");
        let cache = HashCache::new(custom.to_str().unwrap()).unwrap();
        drop(cache);
        assert!(custom.join("hash_cache.db").exists());
    }

    #[test]
    fn hash_cache_table_created_automatically() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HashCache::new(dir.path().to_str().unwrap()).unwrap();
        let entry = cache.get_entry("/nonexistent", HashAlgorithm::Xxh128, 0, -1);
        assert!(entry.is_none());
    }

    // === §24: HashCache put_entry / get_entry ===

    #[test]
    fn hash_cache_put_and_get_whole_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HashCache::new(dir.path().to_str().unwrap()).unwrap();

        let entry = HashCacheEntry {
            file_path: "/tmp/test.txt".into(),
            hash_algorithm: HashAlgorithm::Xxh128,
            file_hash: "abcdef1234567890".into(),
            last_modified_time: 1710000000_000_000_000,
            range_start: 0,
            range_end: -1,
        };
        cache.put_entry(&entry);

        let result = cache
            .get_entry("/tmp/test.txt", HashAlgorithm::Xxh128, 0, -1)
            .unwrap();
        assert_eq!(result.file_path, "/tmp/test.txt");
        assert_eq!(result.file_hash, "abcdef1234567890");
        assert_eq!(result.range_start, 0);
        assert_eq!(result.range_end, -1);
    }

    #[test]
    fn hash_cache_put_and_get_byte_range() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HashCache::new(dir.path().to_str().unwrap()).unwrap();

        let entry = HashCacheEntry {
            file_path: "/tmp/test.txt".into(),
            hash_algorithm: HashAlgorithm::Xxh128,
            file_hash: "rangehash123".into(),
            last_modified_time: 1710000000_000_000_000,
            range_start: 100,
            range_end: 200,
        };
        cache.put_entry(&entry);

        let result = cache
            .get_entry("/tmp/test.txt", HashAlgorithm::Xxh128, 100, 200)
            .unwrap();
        assert_eq!(result.file_hash, "rangehash123");
        assert_eq!(result.range_start, 100);
        assert_eq!(result.range_end, 200);
    }

    #[test]
    fn hash_cache_replace_existing_entry() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HashCache::new(dir.path().to_str().unwrap()).unwrap();

        let entry1 = HashCacheEntry {
            file_path: "/tmp/test.txt".into(),
            hash_algorithm: HashAlgorithm::Xxh128,
            file_hash: "old_hash".into(),
            last_modified_time: 1000,
            range_start: 0,
            range_end: -1,
        };
        cache.put_entry(&entry1);

        let entry2 = HashCacheEntry {
            file_path: "/tmp/test.txt".into(),
            hash_algorithm: HashAlgorithm::Xxh128,
            file_hash: "new_hash".into(),
            last_modified_time: 2000,
            range_start: 0,
            range_end: -1,
        };
        cache.put_entry(&entry2);

        let result = cache
            .get_entry("/tmp/test.txt", HashAlgorithm::Xxh128, 0, -1)
            .unwrap();
        assert_eq!(result.file_hash, "new_hash");
    }

    #[test]
    fn hash_cache_get_nonexistent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HashCache::new(dir.path().to_str().unwrap()).unwrap();

        let result = cache.get_entry("/no/such/file", HashAlgorithm::Xxh128, 0, -1);
        assert!(result.is_none());
    }

    #[test]
    fn hash_cache_whole_file_vs_byte_range_different_keys() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HashCache::new(dir.path().to_str().unwrap()).unwrap();

        let whole = HashCacheEntry {
            file_path: "/tmp/test.txt".into(),
            hash_algorithm: HashAlgorithm::Xxh128,
            file_hash: "whole_hash".into(),
            last_modified_time: 1000,
            range_start: 0,
            range_end: -1,
        };
        cache.put_entry(&whole);

        let result = cache.get_entry("/tmp/test.txt", HashAlgorithm::Xxh128, 0, 100);
        assert!(result.is_none());

        let result = cache
            .get_entry("/tmp/test.txt", HashAlgorithm::Xxh128, 0, -1)
            .unwrap();
        assert_eq!(result.file_hash, "whole_hash");
    }

    // === §24: HashCacheEntry validation ===

    #[test]
    fn hash_cache_entry_byte_range_end_must_exceed_start() {
        let result = HashCacheEntry::new(
            "/tmp/test.txt".into(),
            HashAlgorithm::Xxh128,
            "hash".into(),
            1000,
            50,
            10,
        );
        assert!(result.is_err());
    }

    // === §24: S3CheckCache construction ===

    #[test]
    fn s3_check_cache_new_creates_db() {
        let dir = tempfile::tempdir().unwrap();
        let cache = S3CheckCache::new(dir.path().to_str().unwrap()).unwrap();
        drop(cache);
        assert!(dir.path().join("s3_check_cache.db").exists());
    }

    // === §24: S3CheckCache put_entry / get_entry ===

    #[test]
    fn s3_check_cache_put_and_get_fresh_entry() {
        let dir = tempfile::tempdir().unwrap();
        let cache = S3CheckCache::new(dir.path().to_str().unwrap()).unwrap();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let entry = S3CheckCacheEntry {
            s3_key: "prefix/Data/abcdef.xxh128".into(),
            last_seen_time: now.to_string(),
        };
        cache.put_entry(&entry);

        let result = cache
            .get_entry("prefix/Data/abcdef.xxh128")
            .unwrap();
        assert_eq!(result.s3_key, "prefix/Data/abcdef.xxh128");
    }

    #[test]
    fn s3_check_cache_expired_entry_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let cache = S3CheckCache::new(dir.path().to_str().unwrap()).unwrap();

        let old_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            - (31.0 * 86400.0);
        let entry = S3CheckCacheEntry {
            s3_key: "old-key".into(),
            last_seen_time: old_time.to_string(),
        };
        cache.put_entry(&entry);

        let result = cache.get_entry("old-key");
        assert!(result.is_none());
    }

    #[test]
    fn s3_check_cache_nonexistent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let cache = S3CheckCache::new(dir.path().to_str().unwrap()).unwrap();

        let result = cache.get_entry("no-such-key");
        assert!(result.is_none());
    }

    #[test]
    fn s3_check_cache_replace_existing() {
        let dir = tempfile::tempdir().unwrap();
        let cache = S3CheckCache::new(dir.path().to_str().unwrap()).unwrap();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        let entry1 = S3CheckCacheEntry {
            s3_key: "key1".into(),
            last_seen_time: (now - 86400.0).to_string(),
        };
        cache.put_entry(&entry1);

        let entry2 = S3CheckCacheEntry {
            s3_key: "key1".into(),
            last_seen_time: now.to_string(),
        };
        cache.put_entry(&entry2);

        let result = cache.get_entry("key1").unwrap();
        assert_eq!(result.s3_key, "key1");
    }

    #[test]
    fn s3_check_cache_invalid_timestamp_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let cache = S3CheckCache::new(dir.path().to_str().unwrap()).unwrap();

        let entry = S3CheckCacheEntry {
            s3_key: "bad-ts".into(),
            last_seen_time: "not-a-number".into(),
        };
        cache.put_entry(&entry);

        let result = cache.get_entry("bad-ts");
        assert!(result.is_none());
    }

    // === §24: default_cache_dir ===

    #[test]
    fn default_cache_dir_uses_home() {
        if let Some(home) = std::env::var_os("HOME") {
            let expected = std::path::Path::new(&home)
                .join(".deadline")
                .join("job_attachments");
            let result = default_cache_dir();
            assert_eq!(result, Some(expected.to_string_lossy().into_owned()));
        }
    }
}
