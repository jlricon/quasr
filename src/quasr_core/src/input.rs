use crate::{MarketingNode, MetricName};
use chrono::NaiveDate;
use std::collections::HashSet;
pub type InputDataVec = Vec<InputDataRow>;
#[derive(Debug)]
pub struct CoreMarketingNodeFilter {
    pub value: Vec<MarketingNode>,
    pub level: CoreMarketingNodeLevel,
}
#[derive(Debug, Eq, PartialEq)]

pub enum CoreMetric {
    UpperFunnelMetric(MetricName),
    SummationMetric(HashSet<MetricName>),
    DivisionMetric {
        numerator: HashSet<MetricName>,
        denominator: HashSet<MetricName>,
    },
}
#[derive(Debug, Clone, Copy)]
pub enum CoreMarketingNodeLevel {
    Campaign,
    Ad,
    AdSet,
}
impl CoreMarketingNodeLevel {
    pub fn to_database_column_id_string(&self) -> &str {
        match self {
            Self::Campaign => "campaignId",
            Self::Ad => "adId",
            Self::AdSet => "adSetId",
        }
    }
}
#[derive(Debug)]
/// This is the type that the system receives
pub struct InputDataRow {
    pub value: f64,
    pub date: Option<NaiveDate>,
    pub metric_name: MetricName,
    pub marketing_node: Option<MarketingNode>,
    pub ad_platform: String,
}
impl InputDataRow {
    pub fn mock() -> InputDataVec {
        vec![InputDataRow {
            date: Some(NaiveDate::from_ymd(2020, 1, 1)),
            metric_name: "Cost".to_string(),
            marketing_node: Some("mnode1".to_string()),
            value: 140.0,
            ad_platform: "mock".to_string(),
        }]
    }
}
#[derive(Debug, Copy, Clone)]
pub enum CoreTimeBreakdown {
    Day,
}
#[derive(Debug)]
pub struct QuasrQuery {
    pub metrics: Vec<CoreMetric>,
    pub org_id: String,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub marketing_node_breakdown: Option<CoreMarketingNodeLevel>,
    pub marketing_node_filter: Option<CoreMarketingNodeFilter>,
    pub ad_platform_breakdown: bool,
    pub time_breakdown: Option<CoreTimeBreakdown>,
}
