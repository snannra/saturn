use serde::Deserialize;

#[derive(Deserialize)]
pub struct User {
    pub username: String,
}
