use chrono::{DateTime, Datelike, Duration, Local, NaiveDateTime, TimeZone};
use elasticsearch::{
    auth::Credentials,
    http::{
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    Elasticsearch, Error, ScrollParts, SearchParts,
};

use float_extras::f64;
use serde_json::{json, Value};
use std::{cmp::Ordering, env, io::Write};

use dotenv::dotenv;

use crate::filepath;
use serde::Deserialize;
// #[derive(Deserialize, Debug)]
// struct Document {
//     _id: String,
//     _index: String,
//     _score: f64,
//     _source: DocumentSource,
//     _type: String,
// }

// struct DocumentSource {
//     JPtime: String,
//     NO_0: String,
//         NO_1: String,
//         NO_16: String,
//         NO_18: String,
//         NO_2: String,
//         NO_20: String,
//         NO_21: String,
//         NO_25: String,
//         NO_26: String,
//         NO_3: String,
//         NO_30: String,
//         NO_31: String,
//         NO_32: String,
//         NO_4: String,
//         NO_5: String,
//         NO_6: String,
//         NO_7: String,
//         "ac-i(A)": f64,
//         "ac-pw(kw)": f64,
//         "ac-v(V)": f64,
//         "airTemperature(℃)": f64,
//         "co2_reduction(kg-CO2)": f64,
//         "dc-i(A)": f64,
//         "dc-pw(kw)": f64,
//         "dc-v(V)": f64,
//         "frequency(Hz)": f64,
//         "oil_conversion_amount(L)": f64,
//         "remaining storage battery capacity(%)": f64,
//         "single_unit_integrated_power_generation(kwh)": f64,
//         "solarIrradiance(kw/m^2)": f64,
//         "solar_cell_current(A)": f64,
//         "solar_cell_power(kw)": f64,
//         "solar_cell_voltage(V)": f64,
//         "total_ac_power(kw)": f64,
//         "total_unit_integrated_power_generation(kwh)": f64,
//         utctime: String,
// }

const ISO_DATE_FORMAT: &str = "%Y-%m-%dT%H:%M:%S.%f";

fn isoformat_to_dt(dt_str: &str) -> DateTime<chrono::Local> {
    let dt: NaiveDateTime = NaiveDateTime::parse_from_str(dt_str, ISO_DATE_FORMAT).unwrap();
    Local.from_local_datetime(&dt).unwrap()
}

fn create_doc_json(dt: DateTime<Local>, q: f64) -> Value {
    json!({
        "_source": {
            "JPtime": dt.format(ISO_DATE_FORMAT).to_string(),
            "solarIrradiance(kw/m^2)": q
        }
    })
}

pub fn load_q_and_dt_for_period(start_dt: &DateTime<Local>, span: f64) {
    // let mut q_all = Vec::new();
    // let mut dt_all = Vec::new();
    let mut dt_crr_fetching = start_dt;

    let separated_span = float_extras::f64::modf(span);
    let span_float = separated_span.0;
    let span_int = separated_span.1;

    for _ in 0..(span.ceil() as i64) {
        fetch_docs_by_datetime(&dt_crr_fetching).unwrap();

        let file_path = filepath::get_json_file_path_by_datetime(dt_crr_fetching).unwrap();
        if !std::path::Path::new(&file_path).exists() {
            panic!("JSONファイルが存在しない")
        }

        let json_str = std::fs::read_to_string(file_path).unwrap();
        let mut docs = serde_json::from_str::<Value>(&json_str).unwrap();

        docs.as_array_mut().unwrap().sort_by(|a, b| {
            let a_jp_time = a["_source"]["JPtime"].as_str().unwrap();
            let a_jp_time: DateTime<Local> = isoformat_to_dt(a_jp_time);

            let b_jp_time = b["_source"]["JPtime"].as_str().unwrap();
            let b_jp_time: DateTime<Local> = isoformat_to_dt(b_jp_time);

            let duration: Duration = b_jp_time - a_jp_time;
            if duration.num_milliseconds() > 0 {
                Ordering::Less
            } else if duration.num_milliseconds() == 0 {
                Ordering::Equal
            } else {
                Ordering::Greater
            }
        });

        let year = dt_crr_fetching.year();
        let month = dt_crr_fetching.month();
        let day = dt_crr_fetching.day();
        let date = Local.with_ymd_and_hms(year, month, day, 0, 0, 0).unwrap();

        if docs.as_array().unwrap().len() == 0 {
            docs = json!((0..86400)
                .collect::<Vec<i64>>()
                .iter()
                .map(|second_diff_from_day_begin| {
                    create_doc_json(date + Duration::seconds(*second_diff_from_day_begin), 0.0)
                })
                .collect::<Vec<Value>>());
        } else {
            let docs_vec = docs.as_array().unwrap();

            // start_dt <= first_dt <= last_dt <= end_dt
            let first_dt = isoformat_to_dt(docs_vec[0]["_source"]["JPtime"].as_str().unwrap());
            let last_dt = isoformat_to_dt(docs_vec[1]["_source"]["JPtime"].as_str().unwrap());
            let start_dt = Local
                .with_ymd_and_hms(first_dt.year(), first_dt.month(), first_dt.day(), 0, 0, 0)
                .unwrap();
            let end_dt = Local
                .with_ymd_and_hms(last_dt.year(), last_dt.month(), last_dt.day(), 0, 0, 0)
                .unwrap();

            // 1. start_dt ~ first_dt間を保管するdocsを生成
            let left_docs: Vec<Value>;
            let diff_seconds_from_start = (end_dt - start_dt).num_seconds();
            if diff_seconds_from_start != 0 {
                left_docs = (0..diff_seconds_from_start)
                    .collect::<Vec<i64>>()
                    .iter()
                    .map(|second_diff_from_day_begin| {
                        create_doc_json(date + Duration::seconds(*second_diff_from_day_begin), 0.0)
                    })
                    .collect::<Vec<Value>>();
            }

            // 2. first_dt ~ last_dt間を保管するdocsを生成
            let diff_seconds_from_last_to_end = (end_dt - last_dt).num_seconds();
            
        }
    }
}

#[tokio::main]
pub async fn fetch_docs_by_datetime(dt: &DateTime<Local>) -> Result<(), Error> {
    dotenv().ok();
    let user_name_key = "RECYCLE_ELASTIC_USER_NAME";
    let user_name = env::var(user_name_key).unwrap();
    let password_key = "RECYCLE_ELASTIC_PASSWORD";
    let password = env::var(password_key).unwrap();

    let credentials = Credentials::Basic(user_name.into(), password.into());
    let u = Url::parse("http://133.71.201.197:9200")?;
    let conn_pool = SingleNodeConnectionPool::new(u);
    let transport = TransportBuilder::new(conn_pool).auth(credentials).build()?;
    let client = Elasticsearch::new(transport);

    let path = env::current_dir()?;
    std::fs::create_dir_all(format!("{}/jsons", path.display())).unwrap_or_else(|reason| {
        println!("! {:?}", reason.kind());
    });

    let file_path = filepath::get_json_file_path_by_datetime(dt).unwrap();
    if std::path::Path::new(&file_path).exists() {
        // すでに存在する
        return Ok(());
    }

    let index_name = "pcs_recyclekan";

    let dt_next = *dt + Duration::days(1);

    let gte = format!("{}-{:0>2}-{:0>2}T00:00:00", dt.year(), dt.month(), dt.day());
    let lte = format!(
        "{}-{:0>2}-{:0>2}T00:00:00",
        dt_next.year(),
        dt_next.month(),
        dt_next.day()
    );

    let mut hits = Vec::new(); // 検索結果を格納するベクター

    let scroll = "2m";
    let mut response = client
        .search(SearchParts::Index(&[index_name]))
        .scroll(scroll)
        .from(0)
        .size(1000)
        .body(json!({
            "query": {
                "range": {
                    "JPtime": {
                        "gte": gte,
                        "lt": lte,
                    },  // JST時間をUTC時間として登録しているのでUTC時間として検索する必要がある
                }
            }
        }))
        .send()
        .await?;

    let mut body = response.json::<Value>().await?;
    hits.append(body["hits"]["hits"].as_array_mut().unwrap());

    let mut s_size = body["hits"]["total"]["value"].as_i64().unwrap();
    let mut scroll_id = body["_scroll_id"].as_str().unwrap().to_string();

    // ヒットしている間、次のバッチを要求し続ける。
    while s_size > 0 {
        println!("{}", hits.len());

        response = client
            .scroll(ScrollParts::None)
            .body(json!({
                "scroll": scroll,
                "scroll_id": scroll_id
            }))
            .send()
            .await?;

        body = response.json::<Value>().await?;

        s_size = body["hits"]["hits"].as_array().unwrap().len() as i64;
        scroll_id = body["_scroll_id"].as_str().unwrap().to_string(); // scroll_id を取得する

        let mut_hits = body["hits"]["hits"].as_array_mut().unwrap();
        hits.append(mut_hits);
    }

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(file_path)
        .unwrap();
    let serialized = serde_json::to_string(&hits)?;
    file.write_all(serialized.as_bytes())?;

    Ok(())
}
