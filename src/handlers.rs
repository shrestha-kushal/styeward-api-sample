use crate::config::Config;
use rocket::http::Status;
use rocket::State;
use rocket::{futures::stream::StreamExt, response::stream::TextStream};

#[get("/api/v1/data/json/<schema>/<table_file>")]
pub async fn get_json_file(
    schema: String,
    table_file: String,
    config: &State<Config>,
) -> Result<TextStream![String], Status> {
    let shared_config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&shared_config);
    let file_s3_path = format!("{}/{}/{}", &config.key_prefix, &schema, &table_file);
    let operation_result = client
        .get_object()
        .bucket(&config.bucket)
        .key(&file_s3_path)
        .send()
        .await;
    if let Ok(operation) = operation_result {
        Ok(TextStream! {
            let mut operation = operation;
            while let Some(bytes_result) = operation.body.next().await {
                let bytes = bytes_result.unwrap().to_vec();
                let string_fragment = String::from_utf8(bytes).unwrap();
                yield string_fragment
            }
        })
    } else {
        Err(Status::NotFound)
    }
}
