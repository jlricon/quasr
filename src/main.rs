#![feature(proc_macro_hygiene, decl_macro)]
use dotenv;
use quasr_core::{build_sql, input::QuasrQuery, metrics_to_indexed_metrics, OutputDataRow};
use quasr_io::{
    data_input::{json::AdsFlowQuery, mysql::load_query_from_db},
    output_csv::qs_rows_to_string,
};
use rocket::{
    http::ContentType,
    post,
    response::{
        Responder, Response, {self},
    },
    routes, Request,
};
use rocket_contrib::{database, json::Json};
use serde_json;
use std::{convert::TryInto, io::Cursor};
#[database("test_db")]
struct DbConn(diesel::mysql::MysqlConnection);
struct QSResponse {
    r: Vec<OutputDataRow>,
}
impl<'r> Responder<'r> for QSResponse {
    fn respond_to(self, _: &Request) -> response::Result<'r> {
        Response::build()
            .sized_body(Cursor::new(qs_rows_to_string(self.r)))
            .header(ContentType::parse_flexible("text/csv").unwrap())
            .raw_header("Vary", "Accept-Encoding")
            .ok()
    }
}
#[post("/", data = "<query>")]
fn index(query: Json<AdsFlowQuery>, conn: DbConn) -> QSResponse {
    let q = query.into_inner();
    println!("{}", serde_json::to_string_pretty(&q).unwrap());
    let q: QuasrQuery = q.try_into().unwrap();
    let sql_query = build_sql(&q);
    let db_rows = load_query_from_db(&conn, sql_query);
    // let db_rows = InputDataRow::mock();
    let qs_rows = metrics_to_indexed_metrics(q, db_rows);
    // let csv_rows: Vec<QueryServerRow> = qs_rows.into_iter().map(|r| r.into()).collect();
    QSResponse { r: qs_rows }
    // Content(ContentType::parse_flexible("text/csv").unwrap(), csv_rows)
}

fn main() {
    dotenv::dotenv().ok();
    rocket::ignite()
        .attach(DbConn::fairing())
        .mount("/", routes![index])
        .launch();
}
