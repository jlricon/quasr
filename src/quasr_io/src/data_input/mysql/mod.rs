use chrono::NaiveDate;
use diesel::{
    dsl::sql_query,
    prelude::*,
    sql_types::{Date, Double, Nullable, Varchar},
    QueryableByName,
};
use quasr_core::{input::InputDataRow, CoreSqlString};
#[allow(non_snake_case)]
#[derive(Debug, QueryableByName)]
struct DbRow {
    #[sql_type = "Nullable<Double>"]
    pub sourceValue: Option<f64>,
    #[sql_type = "Nullable<Date>"]
    pub qdate: Option<NaiveDate>,
    #[sql_type = "Varchar"]
    pub name: String,
    #[sql_type = "Nullable<Varchar>"]
    pub marketing_node: Option<String>,
    #[sql_type = "Varchar"]
    pub ad_platform: String,
}
impl Into<InputDataRow> for DbRow {
    fn into(self) -> InputDataRow {
        InputDataRow {
            value: self.sourceValue.unwrap_or(0.0),
            date: self.qdate,
            marketing_node: self.marketing_node,
            metric_name: self.name,
            ad_platform: self.ad_platform,
        }
    }
}

pub fn load_query_from_db(con: &MysqlConnection, query: CoreSqlString) -> Vec<InputDataRow> {
    let diesel_sql_query = sql_query(&query.to_string());
    let db_rows: Vec<DbRow> = diesel_sql_query.load(con).unwrap();
    db_rows.into_iter().map(|i| i.into()).collect()
}
