mod es;
mod filepath;

use chrono::prelude::*;

fn main() {
    let dt_ref = &Local.with_ymd_and_hms(2022, 9, 28, 0, 0, 0).unwrap();
    // es::fetch_docs_by_datetime(dt_ref);
    es::load_q_and_dt_for_period(dt_ref, 1.0);
}
