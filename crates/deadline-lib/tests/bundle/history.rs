//! : Job bundle — history directory

use deadline_lib::bundle::history::create_job_history_bundle_dir;
use std::fs;
use tempfile::TempDir;

// .1/9: First submission → creates -01- directory
#[test]
fn history_first_submission_creates_01_dir() {
    let base = TempDir::new().unwrap();
    let result =
        create_job_history_bundle_dir("TestSubmitter", "TestJob", base.path().to_str().unwrap())
            .unwrap();
    let dirname = std::path::Path::new(&result)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    assert!(std::path::Path::new(&result).is_dir());
    assert!(dirname.contains("-01-"), "Expected '-01-', got: {dirname}");
    assert!(dirname.contains("TestSubmitter"));
    assert!(dirname.contains("TestJob"));
}

// .2: Second submission → creates -02- directory
#[test]
fn history_second_submission_creates_02_dir() {
    let base = TempDir::new().unwrap();
    create_job_history_bundle_dir("Sub", "Job", base.path().to_str().unwrap()).unwrap();
    let second =
        create_job_history_bundle_dir("Sub", "Job", base.path().to_str().unwrap()).unwrap();
    let dirname = std::path::Path::new(&second)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    assert!(dirname.contains("-02-"), "Expected '-02-', got: {dirname}");
}

// .3: Special characters stripped (keep alnum, space, hyphen, underscore)
#[test]
fn history_submitter_special_chars_stripped() {
    let base = TempDir::new().unwrap();
    let result =
        create_job_history_bundle_dir("Test@#$Submitter", "Job", base.path().to_str().unwrap())
            .unwrap();
    let dirname = std::path::Path::new(&result)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    assert!(!dirname.contains('@'), "got: {dirname}");
    assert!(dirname.contains("TestSubmitter"), "got: {dirname}");
}

// .4: Job name truncated to 128 characters
#[test]
fn history_job_name_truncated_to_128() {
    let base = TempDir::new().unwrap();
    let long_name = "A".repeat(200);
    let result =
        create_job_history_bundle_dir("Sub", &long_name, base.path().to_str().unwrap()).unwrap();
    let dirname = std::path::Path::new(&result)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    // Extract job name portion: everything after "Sub-"
    let job_part = dirname.split("Sub-").last().unwrap();
    assert!(
        job_part.len() <= 128,
        "Expected ≤128 chars, got {}",
        job_part.len()
    );
}

// .5: Job name special characters cleaned
#[test]
fn history_job_name_special_chars_cleaned() {
    let base = TempDir::new().unwrap();
    let result =
        create_job_history_bundle_dir("Sub", "My!@#Job", base.path().to_str().unwrap()).unwrap();
    let dirname = std::path::Path::new(&result)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    assert!(!dirname.contains('!'), "got: {dirname}");
    assert!(dirname.contains("MyJob"), "got: {dirname}");
}

// .6/7: Month directory created (and reused on second call)
#[test]
fn history_month_dir_created_and_reused() {
    let base = TempDir::new().unwrap();
    let first =
        create_job_history_bundle_dir("Sub", "Job1", base.path().to_str().unwrap()).unwrap();
    let parent = std::path::Path::new(&first).parent().unwrap();
    let month_name = parent.file_name().unwrap().to_str().unwrap();
    assert!(
        month_name.len() == 7 && month_name.contains('-'),
        "Expected YYYY-MM, got: {month_name}"
    );

    // Second call reuses the same month dir
    let second =
        create_job_history_bundle_dir("Sub", "Job2", base.path().to_str().unwrap()).unwrap();
    assert_eq!(std::path::Path::new(&second).parent().unwrap(), parent);
}

// .8: Gaps in numbering → uses max+1
#[test]
fn history_numbering_gaps_uses_max_plus_one() {
    let base = TempDir::new().unwrap();
    let first = create_job_history_bundle_dir("Sub", "Job", base.path().to_str().unwrap()).unwrap();
    // Rename sequence number 01 → 03 to create a gap (skip the YYYY-MM-DD- prefix
    // to avoid matching day-of-month on dates like May 1st)
    let parent = std::path::Path::new(&first).parent().unwrap();
    let first_name = std::path::Path::new(&first)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    let prefix = &first_name[..11]; // "YYYY-MM-DD-"
    let rest = &first_name[11..]; // "01-Sub-Job"
    let renamed = format!("{prefix}{}", rest.replacen("01-", "03-", 1));
    fs::rename(&first, parent.join(&renamed)).unwrap();

    let next = create_job_history_bundle_dir("Sub", "Job", base.path().to_str().unwrap()).unwrap();
    let dirname = std::path::Path::new(&next)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        dirname.contains("-04-"),
        "Expected '-04-' (max+1), got: {dirname}"
    );
}

// .10: Uses provided base_dir
#[test]
fn history_uses_provided_base_dir() {
    let base = TempDir::new().unwrap();
    let custom = base.path().join("custom");
    fs::create_dir(&custom).unwrap();
    let result = create_job_history_bundle_dir("Sub", "Job", custom.to_str().unwrap()).unwrap();
    assert!(result.starts_with(custom.to_str().unwrap()));
}

// Additional: spaces, hyphens, underscores preserved in names
#[test]
fn history_preserves_spaces_hyphens_underscores() {
    let base = TempDir::new().unwrap();
    let result =
        create_job_history_bundle_dir("My Sub-Name_1", "Job", base.path().to_str().unwrap())
            .unwrap();
    let dirname = std::path::Path::new(&result)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    assert!(dirname.contains("My Sub-Name_1"), "got: {dirname}");
}
