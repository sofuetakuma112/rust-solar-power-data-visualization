use std::env;

use chrono::{DateTime, Datelike, Local};

pub fn get_json_file_name_by_datetime(dt: &DateTime<Local>) -> String {
    format!("docs_{}{:0>2}{:0>2}.json", dt.year(), dt.month(), dt.day())
}

pub fn get_json_file_path_by_datetime(dt: &DateTime<Local>) -> Result<String, std::io::Error> {
    let file_name = get_json_file_name_by_datetime(dt);
    let path = env::current_dir()?;
    Ok(format!("{}/jsons/{}", path.display(), file_name))
}
