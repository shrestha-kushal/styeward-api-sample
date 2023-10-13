use aws_config;
use aws_sdk_s3;
use aws_sdk_s3::types::ByteStream;
use rand;
use reqwest;
use serde::Deserialize;
use std::env;
use url::Url;

const ROOT_URL_ENV: &str = "ENV_STYEWARD_API_ROOT_URL";
const BUCKET_ENV: &str = "ENV_STYEWARD_API_BUCKET";
const KEY_PREFIX_ENV: &str = "ENV_STYEWARD_API_KEY_PREFIX";

struct Config {
    root_url: String,
    bucket: String,
    key_prefix: String,
}

fn config_from_env() -> Config {
    Config {
        root_url: env::var(ROOT_URL_ENV).expect("env var of root url not defined"),
        bucket: env::var(BUCKET_ENV).expect("env var of bucket name not defined"),
        key_prefix: env::var(KEY_PREFIX_ENV).expect("env var of key prefix not defined"),
    }
}

#[tokio::test]
async fn test_schema_tables_endpoint_for_single_table_happy_path() {
    // setup:
    let config = config_from_env();
    let schema = format!("schema{}", rand::random::<u32>());
    let aws_config = aws_config::load_from_env().await;
    let aws_client = aws_sdk_s3::Client::new(&aws_config);
    let table_name = format!("table{}", rand::random::<u32>());
    let obj_body = r#"[{"col1": "hello", "col2": "world"}]"#.as_bytes();
    let obj_key = format!("{}/{}/{}.json", config.key_prefix, schema, table_name);
    let api_url = Url::parse(&config.root_url).expect("error with url config parameter");
    let endpoint_url = api_url
        .join(&format!("api/v1/{}/{}", &schema, "tables"))
        .expect("error when creating endpoint url");
    aws_client
        .put_object()
        .bucket(&config.bucket)
        .key(String::from(&obj_key))
        .body(ByteStream::from_static(obj_body))
        .send()
        .await
        .expect("failed to create dummy s3 object.");

    let cleanup = || async {
        aws_client
            .delete_object()
            .bucket(&config.bucket)
            .key(String::from(&obj_key))
            .send()
            .await
            .expect(&format!("failed to clean up {}", &obj_key));
    };
    // call the api
    match reqwest::Client::builder()
        .https_only(true)
        .build()
        .unwrap()
        .get(endpoint_url.as_str())
        .send()
        .await
    {
        Ok(response) => {
            cleanup().await;
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            match response.json::<Vec<String>>().await {
                Ok(tables) => {
                    assert_eq!(vec![String::from(&table_name)], tables);
                }
                Err(error) => {
                    panic!(
                        "unable to deserialize api response to vector of strings: {}",
                        error
                    );
                }
            }
        }
        Err(error) => {
            cleanup().await;
            panic!("get request against api failed: {}", error);
        }
    }
}

#[tokio::test]
async fn test_schema_table_endpoint_with_bad_schema() {
    // setup:
    let config = config_from_env();
    let schema = format!("nonexistentschema{}", rand::random::<u32>());
    let api_url = Url::parse(&config.root_url).expect("error with url config parameter");
    let endpoint_url = api_url
        .join(&format!("api/v1/{}/{}", &schema, "tables"))
        .expect("error when creating endpoint url");
    // call the api
    match reqwest::get(endpoint_url.as_str()).await {
        Ok(response) => {
            assert_eq!(response.status(), 404);
        }
        Err(error) => {
            panic!("get request against api failed: {}", error);
        }
    }
}

#[derive(Deserialize)]
pub struct TableLocation {
    pub url: String,
}

#[tokio::test]
async fn test_table_endpoint_with_valid_schema_and_table() {
    // setup:
    let config = config_from_env();
    let schema = format!("schema{}", rand::random::<u32>());
    let aws_config = aws_config::load_from_env().await;
    let aws_client = aws_sdk_s3::Client::new(&aws_config);
    let table_name = format!("table{}", rand::random::<u32>());
    let obj_body = r#"[{"col1": "hello", "col2": "world"}]"#.as_bytes();
    let obj_key = format!("{}/{}/{}.json", config.key_prefix, schema, table_name);
    let api_url = Url::parse(&config.root_url).expect("error with url config parameter");
    let endpoint_url = api_url
        .join(&format!("api/v1/{}/{}/uri", &schema, &table_name))
        .expect("error when creating endpoint url");
    let table_data_url = format!("api/v1/data/json/{}/{}.json", &schema, &table_name);
    aws_client
        .put_object()
        .bucket(&config.bucket)
        .key(String::from(&obj_key))
        .body(ByteStream::from_static(obj_body))
        .send()
        .await
        .expect("failed to create dummy s3 object.");
    let cleanup = || async {
        aws_client
            .delete_object()
            .bucket(&config.bucket)
            .key(String::from(&obj_key))
            .send()
            .await
            .expect(&format!("failed to clean up {}", &obj_key));
    };
    // call the api
    match reqwest::get(endpoint_url.as_str()).await {
        Ok(response) => match response.json::<TableLocation>().await {
            Ok(table_location) => {
                cleanup().await;
                assert_eq!(table_data_url, table_location.url);
            }
            Err(error) => {
                cleanup().await;
                panic!(
                    "unable to deserialize api response to vector of strings: {}",
                    error
                );
            }
        },
        Err(error) => {
            cleanup().await;
            panic!("get request against api failed: {}", error);
        }
    }
}

#[tokio::test]
async fn test_table_location_endpoint_with_invalid_schema_and_table() {
    // setup:
    let config = config_from_env();
    let schema = format!("nonexistentschema{}", rand::random::<u32>());
    let table_name = format!("nonexistenttable{}", rand::random::<u32>());
    let api_url = Url::parse(&config.root_url).expect("error with url config parameter");
    let endpoint_url = api_url
        .join(&format!("api/v1/{}/{}", &schema, &table_name))
        .expect("error when creating endpoint url");
    // call the api
    match reqwest::get(endpoint_url.as_str()).await {
        Ok(response) => {
            assert_eq!(response.status(), 404);
        }
        Err(error) => {
            panic!("get request against api failed: {}", error);
        }
    }
}

#[tokio::test]
async fn test_table_location_endpoint_with_valid_schema_but_invalid_table() {
    // setup:
    let config = config_from_env();
    let schema = format!("schema{}", rand::random::<u32>());
    let aws_config = aws_config::load_from_env().await;
    let aws_client = aws_sdk_s3::Client::new(&aws_config);
    let table_name = format!("table{}", rand::random::<u32>());
    let bad_table_name = format!("nonexistent{}", &table_name);
    let obj_body = r#"[{"col1": "hello", "col2": "world"}]"#.as_bytes();
    let obj_key = format!("{}/{}/{}.json", config.key_prefix, schema, table_name);
    let api_url = Url::parse(&config.root_url).expect("error with url config parameter");
    let endpoint_url = api_url
        .join(&format!("api/v1/{}/{}", &schema, &bad_table_name))
        .expect("error when creating endpoint url");
    aws_client
        .put_object()
        .bucket(&config.bucket)
        .key(String::from(&obj_key))
        .body(ByteStream::from_static(obj_body))
        .send()
        .await
        .expect("failed to create dummy s3 object.");
    let cleanup = || async {
        aws_client
            .delete_object()
            .bucket(&config.bucket)
            .key(String::from(&obj_key))
            .send()
            .await
            .expect(&format!("failed to clean up {}", &obj_key));
    };
    // call the api
    match reqwest::get(endpoint_url.as_str()).await {
        Ok(response) => {
            cleanup().await;
            assert_eq!(response.status(), 404);
        }
        Err(error) => {
            cleanup().await;
            panic!("get request against api failed: {}", error);
        }
    }
}

#[tokio::test]
async fn test_table_location_endpoint_with_invalid_schema_but_valid_table() {
    // setup:
    let config = config_from_env();
    let schema = format!("schema{}", rand::random::<u32>());
    let bad_schema = format!("nonexistent{}", &schema);
    let aws_config = aws_config::load_from_env().await;
    let aws_client = aws_sdk_s3::Client::new(&aws_config);
    let table_name = format!("table{}", rand::random::<u32>());
    let obj_body = r#"[{"col1": "hello", "col2": "world"}]"#.as_bytes();
    let obj_key = format!("{}/{}/{}.json", config.key_prefix, schema, table_name);
    let api_url = Url::parse(&config.root_url).expect("error with url config parameter");
    let endpoint_url = api_url
        .join(&format!("api/v1/{}/{}", &bad_schema, &table_name))
        .expect("error when creating endpoint url");
    aws_client
        .put_object()
        .bucket(&config.bucket)
        .key(String::from(&obj_key))
        .body(ByteStream::from_static(obj_body))
        .send()
        .await
        .expect("failed to create dummy s3 object.");
    let cleanup = || async {
        aws_client
            .delete_object()
            .bucket(&config.bucket)
            .key(String::from(&obj_key))
            .send()
            .await
            .expect(&format!("failed to clean up {}", &obj_key));
    };
    // call the api
    match reqwest::get(endpoint_url.as_str()).await {
        Ok(response) => {
            cleanup().await;
            assert_eq!(response.status(), 404);
        }
        Err(error) => {
            cleanup().await;
            panic!("get request against api failed: {}", error);
        }
    }
}

#[tokio::test]
async fn test_schemas_endpoint_with_one_valid_schema() {
    // setup:
    let config = config_from_env();
    let schema = format!("schema{}", rand::random::<u32>());
    let aws_config = aws_config::load_from_env().await;
    let aws_client = aws_sdk_s3::Client::new(&aws_config);
    let table_name = format!("table{}", rand::random::<u32>());
    let obj_body = r#"[{"col1": "hello", "col2": "world"}]"#.as_bytes();
    let obj_key = format!("{}/{}/{}.json", config.key_prefix, schema, table_name);
    let api_url = Url::parse(&config.root_url).expect("error with url config parameter");
    let endpoint_url = api_url
        .join("api/v1/schemas")
        .expect("error when creating endpoint url");
    aws_client
        .put_object()
        .bucket(&config.bucket)
        .key(String::from(&obj_key))
        .body(ByteStream::from_static(obj_body))
        .send()
        .await
        .expect("failed to create dummy s3 object.");
    let cleanup = || async {
        aws_client
            .delete_object()
            .bucket(&config.bucket)
            .key(String::from(&obj_key))
            .send()
            .await
            .expect(&format!("failed to clean up {}", &obj_key));
    };
    // call the api
    match reqwest::get(endpoint_url.as_str()).await {
        Ok(response) => match response.json::<Vec<String>>().await {
            Ok(tables) => {
                cleanup().await;
                assert!(tables.contains(&schema));
            }
            Err(error) => {
                cleanup().await;
                panic!(
                    "unable to deserialize api response to vector of strings: {}",
                    error
                );
            }
        },
        Err(error) => {
            cleanup().await;
            panic!("get request against api failed: {}", error);
        }
    }
}

#[rocket::async_test]
async fn test_json_data_endpoint_with_single_file() {
    // setup:
    let config = config_from_env();
    let schema = format!("schema{}", rand::random::<u32>());
    let aws_config = aws_config::load_from_env().await;
    let aws_client = aws_sdk_s3::Client::new(&aws_config);
    let table_name = format!("table{}", rand::random::<u32>());
    let obj_body = r#"[{"col1": "hello", "col2": "world"}]"#.as_bytes();
    let obj_key = format!("{}/{}/{}.json", config.key_prefix, schema, table_name);
    let api_url = Url::parse(&config.root_url).expect("error with url config parameter");
    let endpoint_url = api_url
        .join(&format!(
            "api/v1/data/json/{}/{}.json",
            &schema, &table_name
        ))
        .expect("error when creating endpoint url");
    aws_client
        .put_object()
        .bucket(&config.bucket)
        .key(String::from(&obj_key))
        .body(ByteStream::from_static(obj_body))
        .send()
        .await
        .expect("failed to create dummy s3 object.");
    let cleanup = || async {
        aws_client
            .delete_object()
            .bucket(&config.bucket)
            .key(String::from(&obj_key))
            .send()
            .await
            .expect(&format!("failed to clean up {}", &obj_key));
    };
    // call the api
    match reqwest::get(endpoint_url.as_str()).await {
        Ok(response) => {
            cleanup().await;
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            match response.text().await {
                Ok(json_body) => {
                    assert_eq!(json_body.as_bytes(), obj_body);
                }
                Err(error) => {
                    panic!(
                        "unable to deserialize api response to vector of strings: {}",
                        error
                    );
                }
            }
        }
        Err(error) => {
            cleanup().await;
            panic!("get request against api failed: {}", error);
        }
    }
}

#[rocket::async_test]
async fn test_json_data_endpoint_with_bad_resource_location() {
    // setup:
    let config = config_from_env();
    let schema = format!("nonexistentschema{}", rand::random::<u32>());
    let table_name = format!("nonexistenttable{}", rand::random::<u32>());
    let api_url = Url::parse(&config.root_url).expect("error with url config parameter");
    let endpoint_url = api_url
        .join(&format!(
            "api/v1/data/json/{}/{}.json",
            &schema, &table_name
        ))
        .expect("error when creating endpoint url");
    // call the api
    match reqwest::get(endpoint_url.as_str()).await {
        Ok(response) => {
            assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
        }
        Err(error) => {
            panic!("get request against api failed: {}", error);
        }
    }
}
