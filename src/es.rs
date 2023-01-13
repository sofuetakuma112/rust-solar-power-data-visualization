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

pub fn load_q_and_dt_for_period(
    start_dt: &DateTime<Local>,
    span: f64,
) -> (Vec<DateTime<Local>>, Vec<f64>) {
    let mut q_all = Vec::new();
    let mut dt_all = Vec::new();
    let mut dt_crr_fetching = start_dt.clone();

    let separated_span = float_extras::f64::modf(span);
    let span_float = separated_span.0;
    let span_int = separated_span.1;

    let mut is_first_loop = true;

    for _ in 0..(span.ceil() as i64) {
        fetch_docs_by_datetime(&dt_crr_fetching).unwrap();

        let file_path = filepath::get_json_file_path_by_datetime(&dt_crr_fetching).unwrap();
        if !std::path::Path::new(&file_path).exists() {
            panic!("JSONファイルが存在しない")
        }

        let json_str = std::fs::read_to_string(file_path).unwrap();
        let mut docs = serde_json::from_str::<Value>(&json_str).unwrap();

        // JPtimeの昇順にソート
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

        // 欠損値を保管する処理
        if docs.as_array().unwrap().len() == 0 {
            docs = json!((0..86400)
                .map(|second_diff_from_day_begin| {
                    create_doc_json(date + Duration::seconds(second_diff_from_day_begin), 0.0)
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
            let mut docs_from_start_to_first = Vec::new();
            let diff_seconds_from_start = (end_dt - start_dt).num_seconds();
            if diff_seconds_from_start != 0 {
                docs_from_start_to_first = (0..diff_seconds_from_start)
                    .map(|second_diff_from_day_begin| {
                        create_doc_json(date + Duration::seconds(second_diff_from_day_begin), 0.0)
                    })
                    .collect::<Vec<Value>>();
            }

            // 2. first_dt ~ last_dt間を保管するdocsを生成
            let diff_seconds_from_last_to_end = (end_dt - last_dt).num_seconds();
            let offset = (last_dt
                - Local
                    .with_ymd_and_hms(last_dt.year(), last_dt.month(), last_dt.day(), 0, 0, 0)
                    .unwrap())
            .num_seconds();
            let mut docs_from_last_to_end = Vec::new();
            if offset != offset + diff_seconds_from_last_to_end {
                docs_from_last_to_end = ((offset + 1)
                    ..(offset + diff_seconds_from_last_to_end + 1)) // FIXME: +1しなくても良い方を探す
                    .map(|second_from_start| {
                        create_doc_json(date + Duration::seconds(second_from_start), 0.0)
                    })
                    .collect::<Vec<Value>>();
            }

            // 補完用に生成したdocsをマージする
            docs_from_start_to_first.append(docs.as_array_mut().unwrap());
            docs_from_start_to_first.append(&mut docs_from_last_to_end);

            // FIXME: メモリ効率悪そうなので直す
            docs = json!(docs_from_start_to_first);
        }

        let mut dts_per_day = docs
            .as_array()
            .unwrap()
            .iter()
            .map(|doc| isoformat_to_dt(doc["_source"]["JPtime"].as_str().unwrap()))
            .collect::<Vec<DateTime<chrono::Local>>>();

        let mut last_dt = Local.with_ymd_and_hms(2400, 1, 1, 0, 0, 0).unwrap();
        if is_first_loop {
            last_dt = dts_per_day[0]
                + Duration::days(span_int as i64)
                + Duration::hours((span_float * 24.0) as i64);
            is_first_loop = false;
        }
        let mut qs_per_day = docs
            .as_array()
            .unwrap()
            .iter()
            .map(|doc| doc["_source"]["solarIrradiance(kw/m^2)"].as_f64().unwrap())
            .collect::<Vec<f64>>();

        let mut has_reached_end = false;
        for dt in dts_per_day.iter() {
            if *dt > last_dt {
                has_reached_end = true;
            }
        }
        if has_reached_end {
            // last_dt以下だけ抽出してdt_all, q_allにマージする
            let mask = dts_per_day
                .iter()
                .map(|dt| *dt <= last_dt)
                .collect::<Vec<bool>>();

            let mut mask_iter = mask.iter();
            dts_per_day.retain(|_| *mask_iter.next().unwrap());
            let mut mask_iter = mask.iter();
            qs_per_day.retain(|_| *mask_iter.next().unwrap());

            dt_all.append(&mut dts_per_day);
            q_all.append(&mut qs_per_day);
            break;
        }

        dt_all.append(&mut dts_per_day);
        q_all.append(&mut qs_per_day);

        dt_crr_fetching = dt_crr_fetching + Duration::days(1);
    }

    return (dt_all, q_all);
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
