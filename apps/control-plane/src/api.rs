#[derive(Debug, Clone, Copy)]
pub struct ApiModule;

impl ApiModule {
    pub const fn new() -> Self {
        Self
    }

    pub const fn status(&self) -> &'static str {
        "stubbed"
    }
}
