mod es;
mod filepath;

use es::load_q_and_dt_for_period;
use plotters::prelude::*;

use chrono::offset::{Local, TimeZone};

fn main() {
    let dt_ref = &Local.with_ymd_and_hms(2022, 9, 28, 0, 0, 0).unwrap();
    // es::fetch_docs_by_datetime(dt_ref);
    let (dt_all, q_all) = load_q_and_dt_for_period(dt_ref, 1.0);

    /* x軸とy軸で個別のVector型にする */
    // x軸 : 日付のVector
    // y軸: 値のVector

    /* (2) 描画先の情報を設定 */
    let image_width = 1080;
    let image_height = 720;
    // 描画先を指定。画像出力する場合はBitMapBackend
    let root =
        BitMapBackend::new("images/plot.png", (image_width, image_height)).into_drawing_area();

    // 背景を白にする
    root.fill(&WHITE).unwrap();

    /* (3) グラフ全般の設定 */
    /* y軸の最大最小値を算出
    f32型はNaNが定義されていてys.iter().max()等が使えないので工夫が必要
    参考サイト
    https://qiita.com/lo48576/items/343ca40a03c3b86b67cb */
    let (y_min, y_max) = q_all
        .iter()
        .fold((0.0 / 0.0, 0.0 / 0.0), |(m, n), v| (v.min(m), v.max(n)));

    let caption = "Sample Plot";
    let font = ("sans-serif", 20);

    let mut chart = ChartBuilder::on(&root)
        .caption(caption, font.into_font()) // キャプションのフォントやサイズ
        .margin(10) // 上下左右全ての余白
        .x_label_area_size(16) // x軸ラベル部分の余白
        .y_label_area_size(42) // y軸ラベル部分の余白
        .build_cartesian_2d(
            // x軸とy軸の数値の範囲を指定する
            *dt_all.first().unwrap()..*dt_all.last().unwrap(), // x軸の範囲
            y_min..y_max,                                      // y軸の範囲
        )
        .unwrap();

    /* (4) グラフの描画 */

    // x軸y軸、グリッド線などを描画
    chart.configure_mesh().draw().unwrap();

    // 折れ線グラフの定義＆描画
    let line_series = LineSeries::new(dt_all.iter().zip(q_all.iter()).map(|(x, y)| (*x, *y)), &RED);
    chart.draw_series(line_series).unwrap();
}
