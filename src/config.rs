use std::env;

pub struct Config {
    pub bucket: String,
    pub key_prefix: String,
}

pub fn get_config_from_env() -> Config {
    Config {
        bucket: env::var("ENV_BUCKET_NAME").expect("failed to fetch bucket name from process env"),
        key_prefix: env::var("ENV_S3_KEY_PREFIX")
            .expect("failed to fetch s3 key prefix from process env"),
    }
}
