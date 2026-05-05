use serde::Deserialize;

#[derive(Deserialize)]
pub struct User {
    username: String,
}
