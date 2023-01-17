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
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{cmp::Ordering, env, io::Write};

use dotenv::dotenv;

use crate::filepath;

// use nalgebra::Vector3;

#[derive(Serialize, Deserialize, Debug, Default)]
struct Document {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_index")]
    index: String,
    #[serde(rename = "_score")]
    score: f64,
    #[serde(rename = "_source")]
    source: DocumentSource,
    #[serde(rename = "_type")]
    r#type: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct DocumentSource {
    #[serde(rename = "JPtime")]
    jptime: String,
    #[serde(rename = "NO_0")]
    no_0: String,
    #[serde(rename = "NO_1")]
    no_1: String,
    #[serde(rename = "NO_2")]
    no_2: String,
    #[serde(rename = "NO_3")]
    no_3: String,
    #[serde(rename = "NO_4")]
    no_4: String,
    #[serde(rename = "NO_5")]
    no_5: String,
    #[serde(rename = "NO_6")]
    no_6: String,
    #[serde(rename = "NO_7")]
    no_7: String,
    #[serde(rename = "NO_16")]
    no_16: String,
    #[serde(rename = "NO_18")]
    no_18: String,
    #[serde(rename = "NO_20")]
    no_20: String,
    #[serde(rename = "NO_21")]
    no_21: String,
    #[serde(rename = "NO_25")]
    no_25: String,
    #[serde(rename = "NO_26")]
    no_26: String,
    #[serde(rename = "NO_30")]
    no_30: String,
    #[serde(rename = "NO_31")]
    no_31: String,
    #[serde(rename = "NO_32")]
    no_32: String,
    #[serde(rename = "ac-i(A)")]
    ac_i: f64,
    #[serde(rename = "ac-pw(kw)")]
    ac_pw: f64,
    #[serde(rename = "ac-v(V)")]
    ac_v: f64,
    #[serde(rename = "airTemperature(℃)")]
    air_temperature: f64,
    #[serde(rename = "co2_reduction(kg-CO2)")]
    co2_reduction: f64,
    #[serde(rename = "dc-i(A)")]
    dc_i: f64,
    #[serde(rename = "dc-pw(kw)")]
    dc_pw: f64,
    #[serde(rename = "dc-v(V)")]
    dc_v: f64,
    #[serde(rename = "frequency(Hz)")]
    frequency: f64,
    #[serde(rename = "oil_conversion_amount(L)")]
    oil_conversion_amount: f64,
    #[serde(rename = "remaining storage battery capacity(%)")]
    remaining_storage_battery_capacity: f64,
    #[serde(rename = "single_unit_integrated_power_generation(kwh)")]
    single_unit_integrated_power_generation: f64,
    #[serde(rename = "solarIrradiance(kw/m^2)")]
    solar_irradiance: f64,
    #[serde(rename = "solar_cell_current(A)")]
    solar_cell_current: f64,
    #[serde(rename = "solar_cell_power(kw)")]
    solar_cell_power: f64,
    #[serde(rename = "solar_cell_voltage(V)")]
    solar_cell_voltage: f64,
    #[serde(rename = "total_ac_power(kw)")]
    total_ac_power: f64,
    #[serde(rename = "total_unit_integrated_power_generation(kwh)")]
    total_unit_integrated_power_generation: f64,
    utctime: String,
}

const ISO_DATE_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.f";

fn isoformat_to_dt(dt_str: &str) -> DateTime<chrono::Local> {
    let dt: NaiveDateTime = NaiveDateTime::parse_from_str(dt_str, ISO_DATE_FORMAT).unwrap();
    Local.from_local_datetime(&dt).unwrap()
}

fn create_doc(dt: DateTime<Local>, q: f64) -> Document {
    Document {
        source: DocumentSource {
            jptime: dt.format(ISO_DATE_FORMAT).to_string(),
            solar_irradiance: q,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn doc_to_dt(v: &Document) -> DateTime<Local> {
    isoformat_to_dt(&v.source.jptime)
}

pub fn load_q_and_dt_for_period(
    start_dt: &DateTime<Local>,
    span: f64,
) -> (Vec<DateTime<Local>>, Vec<f64>) {
    let start = std::time::Instant::now();

    let mut q_all = Vec::new();
    let mut dt_all = Vec::new();
    let mut dt_crr_fetching = start_dt.clone();

    let separated_span = float_extras::f64::modf(span);
    let span_float = separated_span.0;
    let span_int = separated_span.1;

    let mut is_first_loop = true;

    'loop_by_day: for _ in 0..(span.ceil() as i64) {
        // 対象の日時のJSONファイルがなければ取得する
        fetch_docs_by_datetime(&dt_crr_fetching).unwrap();

        let file_path = filepath::get_json_file_path_by_datetime(&dt_crr_fetching).unwrap();
        if !std::path::Path::new(&file_path).exists() {
            panic!("JSONファイルが存在しない")
        }

        let json_str = std::fs::read_to_string(file_path).unwrap();
        let mut docs = serde_json::from_str::<Vec<Document>>(&json_str).unwrap();
        println!("docs.len(): {}", docs.len());

        // JPtimeの昇順にソート
        let sort_start = std::time::Instant::now();
        docs.sort_by(|a, b| {
            let a_jp_time: DateTime<Local> = isoformat_to_dt(&a.source.jptime);
            let b_jp_time: DateTime<Local> = isoformat_to_dt(&b.source.jptime);

            let duration: Duration = b_jp_time - a_jp_time;
            if duration.num_milliseconds() > 0 {
                Ordering::Less
            } else if duration.num_milliseconds() == 0 {
                Ordering::Equal
            } else {
                Ordering::Greater
            }
        });
        let sort_end = sort_start.elapsed();
        println!(
            "ソート: {}.{:03}秒",
            sort_end.as_secs(),
            sort_end.subsec_nanos() / 1_000_000
        );

        let date = Local
            .with_ymd_and_hms(
                dt_crr_fetching.year(),
                dt_crr_fetching.month(),
                dt_crr_fetching.day(),
                0,
                0,
                0,
            )
            .unwrap();

        // 欠損値を保管する処理
        if docs.len() == 0 {
            docs = (0..86400)
                .map(|second_diff_from_day_begin| {
                    create_doc(date + Duration::seconds(second_diff_from_day_begin), 0.0)
                })
                .collect::<Vec<Document>>();
        } else {
            // start_dt <= first_dt <= last_dt <= end_dt
            let first_dt = isoformat_to_dt(&docs.first().unwrap().source.jptime);
            let last_dt = isoformat_to_dt(&docs.last().unwrap().source.jptime);
            let start_dt = Local
                .with_ymd_and_hms(first_dt.year(), first_dt.month(), first_dt.day(), 0, 0, 0)
                .unwrap();
            let end_dt = Local
                .with_ymd_and_hms(last_dt.year(), last_dt.month(), last_dt.day() + 1, 0, 0, 0)
                .unwrap();

            println!("first_dt: {}", first_dt);
            println!("last_dt: {}", last_dt);
            println!("start_dt: {}", start_dt);
            println!("end_dt: {}", end_dt);

            // 1. start_dt ~ first_dt間を保管するdocsを生成
            let mut docs_from_start_to_first = Vec::new();
            let diff_seconds_from_start = (first_dt - start_dt).num_seconds();

            println!("diff_seconds_from_start: {}", diff_seconds_from_start);

            if diff_seconds_from_start != 0 {
                docs_from_start_to_first = (0..diff_seconds_from_start)
                    .map(|second_diff_from_day_begin| {
                        create_doc(date + Duration::seconds(second_diff_from_day_begin), 0.0)
                    })
                    .collect::<Vec<Document>>();
            }

            if docs_from_start_to_first.len() > 0 {
                println!(
                    "left 0: {}",
                    doc_to_dt(docs_from_start_to_first.first().unwrap())
                );
                println!(
                    "left -1: {}",
                    doc_to_dt(docs_from_start_to_first.last().unwrap())
                );
            }

            println!("middle 0: {}", doc_to_dt(docs.first().unwrap()));
            println!("middle -1: {}", doc_to_dt(docs.last().unwrap()));

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
                        create_doc(date + Duration::seconds(second_from_start), 0.0)
                    })
                    .collect::<Vec<Document>>();
            }

            if docs_from_last_to_end.len() > 0 {
                println!(
                    "right 0: {}",
                    doc_to_dt(&docs_from_last_to_end.first().unwrap())
                );
                println!(
                    "right -1: {}",
                    doc_to_dt(&docs_from_last_to_end.last().unwrap())
                );
            }

            // 補完用に生成したdocsをマージする
            docs_from_start_to_first.append(&mut docs);
            docs_from_start_to_first.append(&mut docs_from_last_to_end);

            docs = docs_from_start_to_first; // FIXME: メモリ効率悪そうな気がするので直す

            println!("doc_to_dt(docs[0]): {}", doc_to_dt(docs.first().unwrap()));
            println!("doc_to_dt(docs[0]): {}", doc_to_dt(docs.last().unwrap()));
            println!(
                "diff_seconds_from_last_to_end: {}",
                diff_seconds_from_last_to_end
            );
            println!("offset: {}\n", offset);
        }

        let mut dts_per_day = docs
            .iter()
            .map(|doc| isoformat_to_dt(&doc.source.jptime))
            .collect::<Vec<DateTime<chrono::Local>>>();
        // let mut dts_per_day = Vector3::from_vec(dts_per_day);

        let mut last_dt = Local.with_ymd_and_hms(2400, 1, 1, 0, 0, 0).unwrap();
        if is_first_loop {
            last_dt = *dts_per_day.first().unwrap()
                + Duration::days(span_int as i64)
                + Duration::hours((span_float * 24.0) as i64);
            is_first_loop = false;
        }
        let mut qs_per_day = docs
            .iter()
            .map(|doc| doc.source.solar_irradiance)
            .collect::<Vec<f64>>();

        for dt in dts_per_day.iter() {
            if *dt > last_dt {
                // 取得しようとしている期間の末尾の日時に到達した

                // last_dt以下だけ抽出し、dt_all, q_allにマージしてループから抜ける
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
                break 'loop_by_day;
            }
        }

        dt_all.append(&mut dts_per_day);
        q_all.append(&mut qs_per_day);

        dt_crr_fetching = dt_crr_fetching + Duration::days(1);
    }

    let end = start.elapsed();
    println!(
        "{}.{:03}秒経過しました。",
        end.as_secs(),
        end.subsec_nanos() / 1_000_000
    );

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
        panic!("! {:?}", reason.kind());
    });

    let file_path = filepath::get_json_file_path_by_datetime(dt).unwrap();
    if std::path::Path::new(&file_path).exists() {
        // すでに存在する
        println!("すでにファイルが存在する");
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
