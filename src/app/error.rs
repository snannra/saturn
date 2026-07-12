pub enum JobError {
    Retryable(String),
    Permanent(String),
}

pub enum JobOutcome {
    Success,
    Failed(JobError),
    LeaseLost,
}
