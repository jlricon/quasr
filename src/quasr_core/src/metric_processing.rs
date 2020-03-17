use crate::{
    input::{CoreTimeBreakdown, InputDataVec, QuasrQuery},
    MarketingNode, MetricName, OutputDataRow, OutputDataVec,
};
use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
type DateNodeTuple = (Option<NaiveDate>, Option<MarketingNode>, String);
fn do_qs_divide(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}
pub fn get_division_metric_from_metrics(
    idx: usize,
    numerator: &HashSet<String>,
    denominator: &HashSet<String>,
    data: &InputDataVec,
    query: &QuasrQuery,
) -> OutputDataVec {
    let mut numerators: HashMap<DateNodeTuple, f64> = HashMap::new();
    let mut denominators: HashMap<DateNodeTuple, f64> = HashMap::new();
    data.iter().for_each(|d| {
        if numerator.contains(&d.metric_name) {
            *numerators
                .entry((d.date, d.marketing_node.clone(), d.ad_platform.clone()))
                .or_insert(0.0) += d.value;
        }
        if denominator.contains(&d.metric_name) {
            *denominators
                .entry((d.date, d.marketing_node.clone(), d.ad_platform.clone()))
                .or_insert(0.0) += d.value;
        }
    });
    // This is to account for the fact that we can be missing metrics for the numerator or denominator
    let all_keys: HashSet<&DateNodeTuple> = numerators.keys().chain(denominators.keys()).collect();
    all_keys
        .into_iter()
        .map(|(date, marketing_node, ad_platform)| OutputDataRow {
            metric_index: idx,
            marketing_node: marketing_node.clone(),
            ad_platform: Some(ad_platform.clone()),
            value: do_qs_divide(
                *numerators
                    .get(&(*date, marketing_node.clone(), ad_platform.clone()))
                    .unwrap_or(&0.0),
                *denominators
                    .get(&(*date, marketing_node.clone(), ad_platform.clone()))
                    .unwrap_or(&0.0),
            ),
            start_date: get_filter_date_or_self_date(*date, query.start_date, query.time_breakdown),
            end_date: get_filter_date_or_self_date(*date, query.end_date, query.time_breakdown),
        })
        .collect()
}
pub fn get_filter_date_or_self_date(
    self_date: Option<NaiveDate>,
    filter_date: NaiveDate,
    breakdown: Option<CoreTimeBreakdown>,
) -> NaiveDate {
    match (self_date, breakdown) {
        // If there is no breakdown, the date is the filter date
        (_, None) => filter_date,
        // If there is a daily breakdown, the date is the self date if available
        (Some(d), Some(CoreTimeBreakdown::Day)) => d,
        _ => panic!("Unexpected!"),
    }
}
pub fn get_summation_metric_from_metrics(
    idx: usize,
    metrics: &HashSet<MetricName>,
    data: &InputDataVec,
    query: &QuasrQuery,
) -> OutputDataVec {
    let mut ret: HashMap<DateNodeTuple, f64> = HashMap::new();
    data.iter().for_each(|d| {
        if metrics.contains(&d.metric_name) {
            *ret.entry((d.date, d.marketing_node.clone(), d.ad_platform.clone()))
                .or_insert(0.0) += d.value;
        }
    });
    ret.into_iter()
        .map(|(k, v)| OutputDataRow {
            start_date: get_filter_date_or_self_date(k.0, query.start_date, query.time_breakdown),
            end_date: get_filter_date_or_self_date(k.0, query.end_date, query.time_breakdown),
            metric_index: idx,
            marketing_node: k.1,
            value: v,
            ad_platform: Some(k.2),
        })
        .collect()
}
