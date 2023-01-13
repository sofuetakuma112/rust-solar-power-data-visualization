use chrono::{DateTime, Datelike, FixedOffset, Local, TimeZone, Timelike};
use std::f64::consts::PI;

pub fn calc_q(dt: &DateTime<Local>, lat_deg: f64, lng_deg: f64) -> f64 {
    let dt_new_year = Local.with_ymd_and_hms(dt.year(), 1, 1, 0, 0, 0).unwrap();
    let dt_delta = *dt - dt_new_year;
    let dn = (dt_delta.num_days() + 1) as f64;
    let theta = 2.0 * PI * (dn - 1.0) / 365.0;

    // println!("dn: {}", dn);
    // println!("theta: {}", theta);

    // 太陽赤緯(単位はラジアン)
    let theta_2x = 2.0 * theta;
    let theta_3x = 3.0 * theta;

    // println!("theta.sin(): {}", theta.sin());
    // println!("theta_2x.sin(): {}", theta_2x.sin());
    // println!("theta_3x.sin(): {}", theta_3x.sin());

    // println!("theta.cos(): {}", theta.cos());
    // println!("theta_2x.cos(): {}", theta_2x.cos());
    // println!("theta_3x.cos(): {}", theta_3x.cos());

    let delta = 0.006918 - (0.399912 * theta.cos()) + (0.070257 * theta.sin())
        - (0.006758 * theta_2x.cos())
        + (0.000907 * theta_2x.sin())
        - (0.002697 * theta_3x.cos())
        + (0.001480 * theta_3x.sin());

    // println!("delta: {}", delta);

    // 地心太陽距離のルートの中身
    let geocentri_distance_like = 1.000110
        + 0.034221 * theta.cos()
        + 0.001280 * theta.sin()
        + 0.000719 * theta_2x.cos()
        + 0.000077 * theta_2x.sin();

    // println!("geocentri_distance_like: {}", geocentri_distance_like);

    // 均時差
    let eq = 0.000075 + 0.001868 * theta.cos()
        - 0.032077 * theta.sin()
        - 0.014615 * theta_2x.cos()
        - 0.040849 * theta_2x.sin();
    // println!("eq: {}", eq);

    let phi = lat_deg * PI / 180.0;

    // 経度差
    let lng_diff = (lng_deg - 135.0) / 180.0 * PI;

    let calc_h = |dt: &DateTime<Local>, lng_diff: f64, eq: f64| {
        let dt = FixedOffset::east_opt(0)
            .unwrap()
            .from_local_datetime(&dt.naive_local())
            .unwrap();
        (dt.hour() as f64 + ((dt.minute() as f64) / 60.0) + ((dt.second() as f64) / (60.0 * 60.0))
            - 12.0)
            / 12.0
            * PI
            + lng_diff
            + eq
    };

    let calc_sun_altitude_like =
        |h: f64, delta: f64, phi: f64| phi.sin() * delta.sin() + phi.cos() * delta.cos() * h.cos();

    let h = calc_h(dt, lng_diff, eq);
    let sin_alpha = calc_sun_altitude_like(h, delta, phi);

    1367.0 * geocentri_distance_like * sin_alpha
}

pub fn calc_q_kw(dt: &DateTime<Local>, lng: f64, lat: f64) -> f64 {
    let calc_q = calc_q(dt, lng, lat);
    let positive_calc_q = vec![0.0, calc_q]
        .iter()
        .fold(0.0 / 0.0, |m, v: &f64| v.max(m));
    positive_calc_q / 1000.0
}
