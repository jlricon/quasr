use super::super::date_format;
use chrono::NaiveDate;
use core::convert::TryInto;
use quasr_core::{
    input::{CoreMarketingNodeFilter, CoreMarketingNodeLevel, CoreTimeBreakdown, QuasrQuery},
    set, CoreMetric,
};
use std::collections::HashSet;

#[derive(Deserialize, Eq, PartialEq, Hash, Copy, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
enum MarketingNodeLevel {
    Campaign,
    Ad,
    AdSet,
}
impl Into<CoreMarketingNodeLevel> for MarketingNodeLevel {
    fn into(self) -> CoreMarketingNodeLevel {
        match self {
            MarketingNodeLevel::Ad => CoreMarketingNodeLevel::Ad,
            MarketingNodeLevel::AdSet => CoreMarketingNodeLevel::AdSet,
            MarketingNodeLevel::Campaign => CoreMarketingNodeLevel::Campaign,
        }
    }
}
impl Default for MarketingNodeLevel {
    fn default() -> Self {
        MarketingNodeLevel::Ad
    }
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct TimeRange {
    #[serde(with = "date_format")]
    start_date: NaiveDate,
    #[serde(with = "date_format")]
    end_date: NaiveDate,
}
#[derive(Deserialize, Serialize)]
struct TimeValue {
    value: TimeRange,
}
#[derive(Deserialize, Serialize)]
struct MarketingNodeFilter {
    value: String,
    level: MarketingNodeLevel,
}

// #[derive(Deserialize)]
// struct AdPlatformValue {
//     value: String,
//     level: String,
// }
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConditionSet {
    time: Vec<TimeValue>,
    marketing_node: Option<Vec<MarketingNodeFilter>>,
    // ad_platform: Option<Vec<AdPlatformValue>>,
}
impl ConditionSet {
    fn get_marketing_node_filter(&self) -> Result<Option<CoreMarketingNodeFilter>, SimpleError> {
        if let Some(mnode) = &self.marketing_node {
            let node_level_set = mnode
                .iter()
                .map(|i| i.level)
                .collect::<HashSet<MarketingNodeLevel>>();
            if node_level_set.len() != 1 {
                bail!("We can only have filtering at one level!");
            } else {
                return Ok(Some(CoreMarketingNodeFilter {
                    level: node_level_set.iter().next().unwrap().clone().into(),
                    value: mnode.iter().map(|e| e.value.clone()).collect(),
                }));
            }
        }
        Ok(None)
    }
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum TimeBreakdown {
    Daily,
}
impl Into<CoreTimeBreakdown> for TimeBreakdown {
    fn into(self) -> CoreTimeBreakdown {
        match self {
            Self::Daily => CoreTimeBreakdown::Day,
        }
    }
}

#[derive(Deserialize, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct BreakdownSet {
    time: Option<TimeBreakdown>,
    marketing_node: Option<MarketingNodeLevel>,
    ad_platform: Option<String>,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConcreteMetric {
    metric_name: String,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "metricType")]
enum SummationOrUpperFunnel {
    #[serde(rename_all = "camelCase")]
    UpperFunnelMetric {
        metric_name: String,
    },
    SummationMetric {
        metrics: Vec<ConcreteMetric>,
    },
}
impl SummationOrUpperFunnel {
    fn into_hash_set(self) -> HashSet<String> {
        match self {
            Self::UpperFunnelMetric { metric_name } => set![metric_name],
            Self::SummationMetric { metrics } => {
                metrics.into_iter().map(|m| m.metric_name).collect()
            }
        }
    }
}
#[allow(clippy::enum_variant_names)]
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "metricType")]
enum Metric {
    #[serde(rename_all = "camelCase")]
    UpperFunnelMetric { metric_name: String },
    #[serde(rename_all = "camelCase")]
    SummationMetric {
        // metric_name: Option<String>,
        metrics: Vec<ConcreteMetric>,
    },
    // Division metric can be either summation or concrete
    #[serde(rename_all = "camelCase")]
    DivisionMetric {
        // metric_name: Option<String>,
        numerator: SummationOrUpperFunnel,
        denominator: SummationOrUpperFunnel,
    },
}
impl Into<CoreMetric> for Metric {
    fn into(self) -> CoreMetric {
        match self {
            Self::UpperFunnelMetric { metric_name } => CoreMetric::UpperFunnelMetric(metric_name),
            Self::SummationMetric { metrics, .. } => {
                CoreMetric::SummationMetric(metrics.into_iter().map(|i| i.metric_name).collect())
            }
            Self::DivisionMetric {
                numerator,
                denominator,
            } => CoreMetric::DivisionMetric {
                numerator: numerator.into_hash_set(),
                denominator: denominator.into_hash_set(),
            },
        }
    }
}

use serde::{Deserialize, Serialize};
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdsFlowQuery {
    data_query: DataQuery,
    org_id: String,
}
#[derive(Deserialize, Serialize)]
struct DataQuery {
    metrics: Vec<Metric>,
    filters: ConditionSet,
    #[serde(default)]
    breakdowns: BreakdownSet,
}
type BoxResult<T> = Result<T, SimpleError>;
use simple_error::{bail, SimpleError};

impl TryInto<QuasrQuery> for AdsFlowQuery {
    fn try_into(self) -> BoxResult<QuasrQuery> {
        if self.data_query.filters.time.len() != 1 {
            bail!("You can only have one time filter!")
        }
        Ok(QuasrQuery {
            org_id: self.org_id,
            marketing_node_filter: self.data_query.filters.get_marketing_node_filter().unwrap(),
            start_date: self.data_query.filters.time[0].value.start_date,
            end_date: self.data_query.filters.time[0].value.end_date,
            marketing_node_breakdown: self.data_query.breakdowns.marketing_node.map(|m| m.into()),
            ad_platform_breakdown: match self.data_query.breakdowns.ad_platform {
                Some(e) if e == "adPlatform" => true,
                None => false,
                Some(e) => panic!("{} is not a valid ad platform", e),
            },
            time_breakdown: self.data_query.breakdowns.time.map(|m| m.into()),
            metrics: self
                .data_query
                .metrics
                .into_iter()
                .map(|i| i.into())
                .collect(),
        })
    }
    type Error = SimpleError;
}

#[cfg(test)]
mod test {
    use super::{set, AdsFlowQuery, CoreMetric, QuasrQuery};
    use serde_json;
    use std::{collections::HashSet, convert::TryInto};
    #[test]
    fn test_deserialize_nested_division() {
        let json: AdsFlowQuery =
            serde_json::from_str(include_str!("data/query_div_summation.json")).unwrap();
        let core_query: QuasrQuery = json.try_into().unwrap();
        assert_eq!(
            core_query.metrics,
            vec![CoreMetric::DivisionMetric {
                numerator: set!["Cost"],
                denominator: set!["Install", "Other Install"]
            }]
        )
    }
}
