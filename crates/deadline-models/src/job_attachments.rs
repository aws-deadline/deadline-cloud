#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileConflictResolution {
    CreateCopy,
    Overwrite,
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileSystemLocationType {
    Shared,
    Local,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobAttachmentsFileSystem {
    Copied,
    Virtual,
}
