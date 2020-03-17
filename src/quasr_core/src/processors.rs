use crate::{input::QuasrQuery, CoreMetric};
use std::collections::HashSet;

pub trait Processor {
    fn select(&self, _: &QuasrQuery) -> Vec<String> {
        vec![]
    }
    fn filter(&self, _: &QuasrQuery) -> Vec<String> {
        vec![]
    }
    fn groupby(&self, _: &QuasrQuery) -> Vec<String> {
        vec![]
    }
}
pub struct BaseFilter;
pub struct MarketingNodeBreakdown;
pub struct TimeFilter;
pub struct TimeBreakdown;
pub struct MarketingNodeFilter;
pub struct MetricSelector;
impl Processor for BaseFilter {
    fn select(&self, _: &QuasrQuery) -> Vec<String> {
        vec![
            "SUM(sourceValue) AS sourceValue".to_owned(),
            "UpperFunnelMetricFields.name as name".to_owned(),
            "\"Twitter\" as ad_platform".to_owned(),
        ]
    }

    fn filter(&self, q: &QuasrQuery) -> Vec<String> {
        vec![
            format!("UpperFunnelMetricFields.organizationId=\"{}\"", q.org_id),
            "UpperFunnelMetricFields.id=UpperFunnelMetricValues.upperFunnelMetricFieldId"
                .to_owned(),
            "Properties.id=UpperFunnelMetricValues.propertyId".to_owned(),
        ]
    }

    fn groupby(&self, _: &QuasrQuery) -> Vec<String> {
        vec![
            "UpperFunnelMetricFields.name".to_owned(),
            "ad_platform".to_owned(),
            "qdate".to_owned(),
        ]
    }
}

impl Processor for MarketingNodeBreakdown {
    fn select(&self, q: &QuasrQuery) -> Vec<String> {
        match q.marketing_node_breakdown {
            Some(q) => vec![format!(
                "Properties.{} AS marketing_node",
                q.to_database_column_id_string()
            )],
            None => vec!["NULL as marketing_node".to_owned()],
        }
    }

    fn groupby(&self, q: &QuasrQuery) -> Vec<String> {
        match q.marketing_node_breakdown {
            Some(q) => vec![format!("Properties.{}", q.to_database_column_id_string())],
            None => vec![],
        }
    }
}
impl Processor for TimeFilter {
    fn filter(&self, q: &QuasrQuery) -> Vec<String> {
        vec![
            format!("date>=\"{}\" ", q.start_date),
            format!("date<=\"{}\" ", q.end_date),
        ]
    }
}
impl Processor for TimeBreakdown {
    fn select(&self, q: &QuasrQuery) -> Vec<String> {
        vec![{
            if let Some(_) = q.time_breakdown {
                "date AS qdate"
            } else {
                "NULL as qdate"
            }
        }
        .to_string()]
    }
}
impl Processor for MarketingNodeFilter {
    fn select(&self, q: &QuasrQuery) -> Vec<String> {
        if let Some(mnode_filter) = &q.marketing_node_filter {
            let mnode_string = mnode_filter
                .value
                .iter()
                .map(|i| format!("\"{}\"", i))
                .collect::<Vec<String>>()
                .join(",");
            vec![format!(
                "Properties.{} IN ({})",
                mnode_filter.level.to_database_column_id_string(),
                mnode_string
            )]
        } else {
            vec![]
        }
    }
}
impl Processor for MetricSelector {
    fn filter(&self, q: &QuasrQuery) -> Vec<String> {
        let unique_base_metric_names = q
            .metrics
            .iter()
            .map(|m| match m {
                CoreMetric::UpperFunnelMetric(metric_name) => vec![format!("\"{}\"", metric_name)],
                CoreMetric::SummationMetric(metrics) => {
                    metrics.iter().map(|m| format!("\"{}\"", m)).collect()
                }
                CoreMetric::DivisionMetric {
                    denominator,
                    numerator,
                } => denominator
                    .union(numerator)
                    .map(|i| format!("\"{}\"", i))
                    .collect(),
            })
            .flatten()
            .collect::<HashSet<String>>()
            .into_iter()
            .collect::<Vec<String>>()
            .join(",");
        vec![format!(
            "UpperFunnelMetricFields.name IN ({})",
            unique_base_metric_names
        )]
    }
}
