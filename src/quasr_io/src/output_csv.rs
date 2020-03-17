use super::date_format;
use chrono::NaiveDate;
use quasr_core::OutputDataRow;
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryServerRow {
    #[serde(with = "date_format")]
    pub start_date: NaiveDate,
    #[serde(with = "date_format")]
    pub end_date: NaiveDate,
    pub metric_index: usize,
    pub value: f64,
    pub marketing_node: Option<String>,
    pub geography: String,
    pub ad_platform: Option<String>,
    pub metadata: String,
}
impl QueryServerRow {
    fn header() -> [&'static str; 8] {
        [
            "startDate",
            "endDate",
            "metricIndex",
            "value",
            "marketingNode",
            "geography",
            "adPlatform",
            "metadata",
        ]
    }
}
impl From<OutputDataRow> for QueryServerRow {
    fn from(row: OutputDataRow) -> Self {
        QueryServerRow {
            start_date: row.start_date,
            end_date: row.end_date,
            metric_index: row.metric_index,
            value: row.value,
            marketing_node: row.marketing_node,
            geography: "".to_owned(),
            ad_platform: row.ad_platform,
            metadata: "".to_owned(),
        }
    }
}
fn qs_rows_to_csv(rows: Vec<QueryServerRow>) -> String {
    let mut wtr = csv::Writer::from_writer(vec![]);
    if rows.is_empty() {
        wtr.write_record(&QueryServerRow::header()).unwrap();
    } else {
        rows.iter().for_each(|m| wtr.serialize(m).unwrap());
    }
    wtr.flush().unwrap();
    String::from_utf8(wtr.into_inner().unwrap()).unwrap()
}
pub fn qs_rows_to_string(rows: Vec<OutputDataRow>) -> String {
    qs_rows_to_csv(rows.into_iter().map(QueryServerRow::from).collect())
}
