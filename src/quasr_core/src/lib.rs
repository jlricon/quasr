use crate::metric_processing::{
    get_division_metric_from_metrics, get_filter_date_or_self_date,
    get_summation_metric_from_metrics,
};
use chrono::NaiveDate;
use input::QuasrQuery;

pub mod input;
pub mod macros;
mod metric_processing;
mod processors;
pub type MetricName = String;
pub type MarketingNode = String;
use crate::processors::{
    BaseFilter, MarketingNodeBreakdown, MarketingNodeFilter, MetricSelector, Processor,
    TimeBreakdown, TimeFilter,
};
pub use input::CoreMetric;

/// A string representing valid SQL code
pub struct CoreSqlString(String);
impl CoreSqlString {
    pub fn to_string(self) -> String {
        self.0
    }
}
/// This is the type that the system outputs
#[derive(Debug)]
pub struct OutputDataRow {
    pub value: f64,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub metric_index: usize,
    pub marketing_node: Option<MarketingNode>,
    pub ad_platform: Option<String>,
}

type OutputDataVec = Vec<OutputDataRow>;

pub fn build_sql(query: &QuasrQuery) -> CoreSqlString {
    let proc: Vec<Box<dyn Processor>> = vec![
        Box::new(BaseFilter),
        Box::new(MarketingNodeBreakdown),
        Box::new(TimeFilter),
        Box::new(TimeBreakdown),
        Box::new(MarketingNodeFilter),
        Box::new(MetricSelector),
    ];
    let selects: Vec<String> = { proc.iter().map(|v| v.select(&query)).flatten().collect() };
    let filters: Vec<String> = { proc.iter().map(|v| v.filter(&query)).flatten().collect() };
    let groupbys: Vec<String> = { proc.iter().map(|v| v.groupby(&query)).flatten().collect() };
    let sql_string = format!(
        "SELECT {} FROM UpperFunnelMetricValues,UpperFunnelMetricFields,\
     Properties WHERE {} GROUP BY {}",
        selects.join(","),
        filters.join(" AND "),
        groupbys.join(",")
    );
    CoreSqlString(sql_string)
}
pub fn metrics_to_indexed_metrics(query: QuasrQuery, data: input::InputDataVec) -> OutputDataVec {
    // Takes an array of data and returns an array of indexed data
    // That is, instead of "Cost", it's metric 0.
    // For summation or division metrics, we just iterate over the array and compose them as we go
    // We map each metric to a vector of data vecs
    query
        .metrics
        .iter()
        .enumerate()
        .map(|(idx, metric)| match metric {
            CoreMetric::UpperFunnelMetric(metric_name) => data
                .iter()
                .filter_map(|d| {
                    if &d.metric_name == metric_name {
                        Some(OutputDataRow {
                            metric_index: idx,
                            start_date: get_filter_date_or_self_date(
                                d.date,
                                query.start_date,
                                query.time_breakdown,
                            ),
                            end_date: get_filter_date_or_self_date(
                                d.date,
                                query.end_date,
                                query.time_breakdown,
                            ),
                            marketing_node: d.marketing_node.clone(),
                            value: d.value,
                            ad_platform: if query.ad_platform_breakdown.clone() {
                                Some(d.ad_platform.clone())
                            } else {
                                None
                            },
                        })
                    } else {
                        None
                    }
                })
                .collect::<OutputDataVec>(),
            CoreMetric::SummationMetric(metrics) => {
                get_summation_metric_from_metrics(idx, metrics, &data, &query)
            }

            CoreMetric::DivisionMetric {
                numerator,
                denominator,
            } => get_division_metric_from_metrics(idx, numerator, denominator, &data, &query),
        })
        .flatten()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{build_sql, get_division_metric_from_metrics, set, CoreMetric, QuasrQuery};
    use crate::{
        input::{CoreMarketingNodeFilter, CoreMarketingNodeLevel, CoreTimeBreakdown, InputDataRow},
        metrics_to_indexed_metrics, OutputDataRow,
    };
    use chrono::NaiveDate;
    use pretty_assertions::{assert_eq, assert_ne};
    use std::collections::HashSet;
    fn get_query() -> QuasrQuery {
        QuasrQuery {
            metrics: vec![],
            org_id: "".to_string(),
            start_date: NaiveDate::from_ymd(2014, 7, 8),
            end_date: NaiveDate::from_ymd(2014, 7, 8),
            marketing_node_breakdown: Some(CoreMarketingNodeLevel::Ad),
            marketing_node_filter: None,
            ad_platform_breakdown: false,
            time_breakdown: Some(CoreTimeBreakdown::Day),
        }
    }
    #[test]
    fn division_metric_works_when_input_empty() {
        let data = vec![];
        let ret = get_division_metric_from_metrics(
            0,
            &set!["Numer"],
            &set!["Denom"],
            &data,
            &get_query(),
        );
        assert!(ret.is_empty());
    }
    #[test]
    fn division_metric_works_when_only_num() {
        let data = vec![
            // No denominator
            InputDataRow {
                value: 1.0,
                metric_name: "Numer".to_string(),
                marketing_node: Option::from("mnode1".to_string()),
                date: Option::from(NaiveDate::from_ymd(2014, 7, 8)),
                ad_platform: "mock".to_owned(),
            },
            // Both Numerator and denominator
            InputDataRow {
                value: 1.0,
                metric_name: "Numer".to_string(),
                marketing_node: Option::from("mnode2".to_string()),
                date: Option::from(NaiveDate::from_ymd(2014, 7, 8)),
                ad_platform: "mock".to_owned(),
            },
            InputDataRow {
                value: 2.0,
                metric_name: "Denom".to_string(),
                marketing_node: Option::from("mnode2".to_string()),
                date: Option::from(NaiveDate::from_ymd(2014, 7, 8)),
                ad_platform: "mock".to_owned(),
            },
            // No Numerator
            InputDataRow {
                value: 2.0,
                metric_name: "Denom".to_string(),
                marketing_node: Option::from("mnode3".to_string()),
                date: Option::from(NaiveDate::from_ymd(2015, 7, 8)),
                ad_platform: "mock".to_owned(),
            },
        ];
        let ret = get_division_metric_from_metrics(
            0,
            &set!["Numer"],
            &set!["Denom"],
            &data,
            &get_query(),
        );
        assert!(
            ret.iter()
                .filter(|f| f.marketing_node == Some("mnode1".to_string()))
                .nth(0)
                .unwrap()
                .value
                == 0.0
        );
        assert!(
            ret.iter()
                .filter(|f| f.marketing_node == Some("mnode2".to_string()))
                .nth(0)
                .unwrap()
                .value
                == 0.5
        );
        assert!(
            ret.iter()
                .filter(|f| f.marketing_node == Some("mnode3".to_string()))
                .nth(0)
                .unwrap()
                .value
                == 0.0
        );
    }
    #[test]
    fn test_marketing_node_filter() {
        let input = QuasrQuery {
            end_date: NaiveDate::from_ymd(2020, 1, 2),
            start_date: NaiveDate::from_ymd(2020, 1, 1),
            metrics: vec![],
            org_id: "test_org".to_owned(),
            marketing_node_breakdown: Option::from(CoreMarketingNodeLevel::Ad),
            marketing_node_filter: Some(CoreMarketingNodeFilter {
                level: CoreMarketingNodeLevel::Campaign,
                value: vec!["test_node".to_owned()],
            }),
            ad_platform_breakdown: false,
            time_breakdown: Option::from(CoreTimeBreakdown::Day),
        };
        let res = build_sql(&input);
        assert_eq!(
            res.0.replace("\\n"," ").replace("\n"," "),
        "SELECT SUM(sourceValue) AS sourceValue,Properties.adId AS marketing_node,UpperFunnelMetricFields.name as name,date AS qdate, \"Twitter\" as ad_platform\n\
        FROM UpperFunnelMetricValues,UpperFunnelMetricFields, Properties\n\
        WHERE UpperFunnelMetricFields.organizationId=\"test_org\"\n\
        AND UpperFunnelMetricFields.id=UpperFunnelMetricValues.upperFunnelMetricFieldId\n\
        AND Properties.id=UpperFunnelMetricValues.propertyId\n\
        AND UpperFunnelMetricFields.name IN ()\n\
        AND date>=\"2020-01-01\" \n\
        AND date<=\"2020-01-02\" \n\
        AND Properties.campaignId IN (\"test_node\")\
        GROUP BY UpperFunnelMetricFields.name,ad_platform,qdate,Properties.adId".replace("\n"," ")
        )
    }
    #[test]
    fn test_complex_metrics() {
        // Adsflow query fixture
        let core_query: QuasrQuery = QuasrQuery {
            org_id: "test".to_string(),
            end_date: NaiveDate::from_ymd(2020, 3, 3),
            start_date: NaiveDate::from_ymd(2020, 3, 1),
            metrics: vec![
                CoreMetric::UpperFunnelMetric("Cost".to_string()),
                CoreMetric::DivisionMetric {
                    numerator: set!["Install"],
                    denominator: set!["Install", "Cost"],
                },
            ],
            marketing_node_breakdown: Option::from(CoreMarketingNodeLevel::Ad),
            marketing_node_filter: None,
            ad_platform_breakdown: false,
            time_breakdown: Option::from(CoreTimeBreakdown::Day),
        };
        let db_mock = vec![
            InputDataRow {
                value: 1.0,
                date: Option::from(NaiveDate::from_ymd(2020, 1, 1)),
                marketing_node: Option::from("test_node".to_owned()),
                metric_name: "Cost".to_owned(),
                ad_platform: "mock".to_owned(),
            },
            InputDataRow {
                value: 2.0,
                date: Option::from(NaiveDate::from_ymd(2020, 1, 2)),
                marketing_node: Option::from("test_node".to_owned()),
                metric_name: "Cost".to_owned(),
                ad_platform: "mock".to_owned(),
            },
            InputDataRow {
                value: 4.0,
                date: Option::from(NaiveDate::from_ymd(2020, 1, 1)),
                marketing_node: Option::from("test_node".to_owned()),
                metric_name: "Install".to_owned(),
                ad_platform: "mock".to_owned(),
            },
        ];
        let mut ret = metrics_to_indexed_metrics(core_query, db_mock);
        let mut expected = vec![
            OutputDataRow {
                value: 1.0,
                start_date: NaiveDate::from_ymd(2020, 1, 1),
                end_date: NaiveDate::from_ymd(2020, 1, 1),
                metric_index: 0,
                marketing_node: Option::from("test_node".to_owned()),
                ad_platform: Option::from("mock".to_owned()),
            },
            OutputDataRow {
                value: 2.0,
                start_date: NaiveDate::from_ymd(2020, 1, 2),
                end_date: NaiveDate::from_ymd(2020, 1, 2),
                metric_index: 0,
                marketing_node: Option::from("test_node".to_owned()),
                ad_platform: Option::from("mock".to_owned()),
            },
            // Zero because that day there are no Installs
            OutputDataRow {
                value: 0.0,
                start_date: NaiveDate::from_ymd(2020, 1, 2),
                end_date: NaiveDate::from_ymd(2020, 1, 2),
                metric_index: 1,
                marketing_node: Option::from("test_node".to_owned()),
                ad_platform: Option::from("mock".to_owned()),
            },
            //4/(4+1)=0.8
            OutputDataRow {
                value: 0.8,
                start_date: NaiveDate::from_ymd(2020, 1, 1),
                end_date: NaiveDate::from_ymd(2020, 1, 1),
                metric_index: 1,
                marketing_node: Option::from("test_node".to_owned()),
                ad_platform: Option::from("mock".to_owned()),
            },
        ];
        assert_eq!(ret.len(), expected.len());
        // Need to sort because internally it uses HashMaps so ordering is not guaranteed
        ret.sort_by_key(|k| (k.start_date, k.metric_index, k.marketing_node.clone()));
        expected.sort_by_key(|k| (k.start_date, k.metric_index, k.marketing_node.clone()));

        for (ret, exp) in ret.iter().zip(expected) {
            assert_eq!(
                (ret.value, ret.start_date, &ret.marketing_node),
                (exp.value, exp.start_date, &exp.marketing_node),
                "Expected {:?}, got {:?}",
                exp,
                ret
            );
        }
    }
}
