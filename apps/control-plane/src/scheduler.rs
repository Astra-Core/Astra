#[derive(Debug, Clone, Copy)]
pub struct SchedulerModule;

impl SchedulerModule {
    pub const fn new() -> Self {
        Self
    }

    pub const fn status(&self) -> &'static str {
        "stubbed"
    }
}
