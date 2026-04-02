#[derive(Debug, Clone, Copy, Default)]
pub struct SchedulerModule;

impl SchedulerModule {
    pub const fn new() -> Self {
        Self
    }

    pub const fn status(&self) -> &'static str {
        "stubbed"
    }
}
