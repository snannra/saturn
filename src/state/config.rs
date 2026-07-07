#[derive(Clone)]
pub struct Config {
    pub postgres_url: String,
    pub redis_url: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            postgres_url: std::env::var("POSTGRES_CONNECTION_URL").unwrap(),
            redis_url: std::env::var("REDIS_CONNECTION_URL").unwrap(),
        }
    }
}
