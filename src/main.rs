#[macro_use]
extern crate rocket;

pub mod config;
pub mod handlers;

use handlers::get_json_file;

#[launch]
fn rocket() -> _ {
    let config = config::get_config_from_env();
    rocket::build().manage(config).mount(
        "/",
        routes![
            get_json_file,
            // ommiting below handlers from github.com sample
            // get_table_location,
            // get_schema_tables,
            // get_schemas
        ],
    )
}
