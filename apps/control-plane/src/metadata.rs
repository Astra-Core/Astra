#[derive(Debug, Clone, Copy)]
pub struct MetadataModule;

impl MetadataModule {
    pub const fn new() -> Self {
        Self
    }

    pub const fn status(&self) -> &'static str {
        "stubbed"
    }
}
