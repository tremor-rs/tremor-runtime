// Copyright 2020-2022, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(dead_code)]

use super::{
    common, id,
    resource::{self, resource_to_pb},
};
use crate::{
    connectors::{
        impls::otel::common::maybe_instrumentation_scope_to_json,
        utils::pb::{self},
    },
    errors::Result,
};
use simd_json::StaticNode;
use tremor_otelapis::opentelemetry::proto::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    metrics::v1::{
        exemplar,
        metric::{self, Data},
        number_data_point,
        summary_data_point::ValueAtQuantile,
        Exemplar, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum, Summary, SummaryDataPoint,
    },
};
use tremor_value::{literal, prelude::*, Value};

pub(crate) fn exemplars_to_json(data: Vec<Exemplar>) -> Value<'static> {
    data.into_iter()
        .map(|exemplar| {
            let filtered_attributes = common::key_value_list_to_json(exemplar.filtered_attributes);
            let mut r = literal!({
                "span_id": id::hex_span_id_to_json(&exemplar.span_id),
                "trace_id": id::hex_trace_id_to_json(&exemplar.trace_id),
                "filtered_attributes": filtered_attributes,
                "time_unix_nano": exemplar.time_unix_nano,
            });
            match exemplar.value {
                Some(exemplar::Value::AsDouble(v)) => {
                    r.try_insert("value", v);
                }
                Some(exemplar::Value::AsInt(v)) => {
                    r.try_insert("value", v);
                }
                None => (),
            };
            r
        })
        .collect()
}

pub(crate) fn exemplars_to_pb(json: Option<&Value<'_>>) -> Result<Vec<Exemplar>> {
    json.as_array()
        .ok_or("Unable to map json value to Exemplars pb")?
        .iter()
        .map(|data| {
            let filtered_attributes =
                if let Some(filtered_attributes) = data.get_object("filtered_attributes") {
                    common::obj_key_value_list_to_pb(filtered_attributes)
                } else {
                    Vec::new()
                };
            Ok(Exemplar {
                filtered_attributes,
                span_id: id::hex_span_id_to_pb(data.get("span_id"))?,
                trace_id: id::hex_trace_id_to_pb(data.get("trace_id"))?,
                time_unix_nano: pb::maybe_int_to_pbu64(data.get("time_unix_nano"))?,
                value: match data.get("value") {
                    Some(Value::Static(StaticNode::F64(v))) => Some(exemplar::Value::AsDouble(*v)),
                    Some(Value::Static(StaticNode::I64(v))) => Some(exemplar::Value::AsInt(*v)),
                    _ => return Err("Unable to parse Exemplar value error".into()),
                },
            })
        })
        .collect()
}

pub(crate) fn quantile_values_to_json(data: Vec<ValueAtQuantile>) -> Value<'static> {
    data.into_iter()
        .map(|data| {
            literal!({
                "value": data.value,
                "quantile": data.quantile,
            })
        })
        .collect()
}

pub(crate) fn quantile_values_to_pb(json: Option<&Value<'_>>) -> Result<Vec<ValueAtQuantile>> {
    json.as_array()
        .ok_or("Unable to map json value to ValueAtQuantiles")?
        .iter()
        .map(|data| {
            let value = pb::maybe_double_to_pb(data.get("value"))?;
            let quantile = pb::maybe_double_to_pb(data.get("quantile"))?;
            Ok(ValueAtQuantile { quantile, value })
        })
        .collect()
}

pub(crate) fn data_points_to_json(pb: Vec<NumberDataPoint>) -> Value<'static> {
    pb.into_iter()
        .map(|data| {
            let attributes = common::key_value_list_to_json(data.attributes);
            let mut r = literal!({
                "start_time_unix_nano": data.start_time_unix_nano,
                "time_unix_nano": data.time_unix_nano,
                "attributes": attributes,
                "exemplars": exemplars_to_json(data.exemplars),
                "flags": data.flags,
            });
            match data.value {
                Some(number_data_point::Value::AsDouble(v)) => {
                    r.try_insert("value", v);
                }
                Some(number_data_point::Value::AsInt(v)) => {
                    r.try_insert("value", v);
                }
                None => (),
            };
            r
        })
        .collect()
}

pub(crate) fn data_points_to_pb(json: Option<&Value<'_>>) -> Result<Vec<NumberDataPoint>> {
    json.as_array()
        .ok_or("Unable to map json value to otel pb NumberDataPoint list")?
        .iter()
        .map(|data| {
            let attributes = common::get_attributes(data)?;
            Ok(NumberDataPoint {
                attributes,
                exemplars: exemplars_to_pb(data.get("exemplars"))?,
                time_unix_nano: pb::maybe_int_to_pbu64(data.get("time_unix_nano"))?,
                start_time_unix_nano: pb::maybe_int_to_pbu64(data.get("start_time_unix_nano"))?,
                value: match data.get("value") {
                    Some(Value::Static(StaticNode::F64(v))) => {
                        Some(number_data_point::Value::AsDouble(*v))
                    }
                    Some(Value::Static(StaticNode::I64(v))) => {
                        Some(number_data_point::Value::AsInt(*v))
                    }
                    _ => return Err("Unable to parse Exemplar value error".into()),
                },
                flags: pb::maybe_int_to_pbu32(data.get("flags"))?,
            })
        })
        .collect()
}

#[allow(deprecated)] // handling depricated fields is required by the PB files
pub(crate) fn histo_data_points_to_json(pb: Vec<HistogramDataPoint>) -> Value<'static> {
    pb.into_iter()
        .map(|point| {
            let attributes = common::key_value_list_to_json(point.attributes);
            literal!({
                "start_time_unix_nano": point.start_time_unix_nano,
                "time_unix_nano": point.time_unix_nano,
                "attributes": attributes,
                "exemplars": exemplars_to_json(point.exemplars),
                "sum": point.sum,
                "count": point.count,
                "explicit_bounds": point.explicit_bounds,
                "bucket_counts": point.bucket_counts,
                "min": point.min,
                "max": point.max,
                "flags": point.flags,
            })
        })
        .collect()
}

pub(crate) fn histo_data_points_to_pb(json: Option<&Value<'_>>) -> Result<Vec<HistogramDataPoint>> {
    json.as_array()
        .ok_or("Unable to map json value to otel pb HistogramDataPoint list")?
        .iter()
        .map(|data| {
            let attributes = common::maybe_key_value_list_to_pb(data.get("attributes"))?;
            Ok(HistogramDataPoint {
                attributes,
                time_unix_nano: pb::maybe_int_to_pbu64(data.get("time_unix_nano"))?,
                start_time_unix_nano: pb::maybe_int_to_pbu64(data.get("start_time_unix_nano"))?,
                sum: Some(pb::maybe_double_to_pb(data.get("sum"))?),
                count: pb::maybe_int_to_pbu64(data.get("count"))?,
                exemplars: exemplars_to_pb(data.get("exemplars"))?,
                explicit_bounds: pb::f64_repeated_to_pb(data.get("explicit_bounds"))?,
                bucket_counts: pb::u64_repeated_to_pb(data.get("explicit_bounds"))?,
                flags: pb::maybe_int_to_pbu32(data.get("flags"))?,
                max: pb::maybe_double_to_pb(data.get("max")).ok(),
                min: pb::maybe_double_to_pb(data.get("min")).ok(),
            })
        })
        .collect()
}

pub(crate) fn double_summary_data_points_to_json(pb: Vec<SummaryDataPoint>) -> Value<'static> {
    pb.into_iter()
        .map(|point| {
            let attributes = common::key_value_list_to_json(point.attributes);
            literal!({
                "start_time_unix_nano": point.start_time_unix_nano,
                "time_unix_nano": point.time_unix_nano,
                "attributes": attributes,
                "quantile_values": quantile_values_to_json(point.quantile_values),
                "sum": point.sum,
                "count": point.count,
                "flags": point.flags,
            })
        })
        .collect()
}

#[allow(deprecated)] // handling depricated fields is required by the PB files
pub(crate) fn double_summary_data_points_to_pb(
    json: Option<&Value<'_>>,
) -> Result<Vec<SummaryDataPoint>> {
    json.as_array()
        .ok_or("Unable to map json value to otel pb SummaryDataPoint list")?
        .iter()
        .map(|data| {
            let attributes = common::get_attributes(data)?;
            Ok(SummaryDataPoint {
                attributes,
                time_unix_nano: pb::maybe_int_to_pbu64(data.get("time_unix_nano"))?,
                start_time_unix_nano: pb::maybe_int_to_pbu64(data.get("start_time_unix_nano"))?,
                sum: pb::maybe_double_to_pb(data.get("sum"))?,
                count: pb::maybe_int_to_pbu64(data.get("count"))?,
                quantile_values: quantile_values_to_pb(data.get("quantile_values"))?,
                flags: pb::maybe_int_to_pbu32(data.get("flags"))?,
            })
        })
        .collect()
}

pub(crate) fn metrics_data_to_json(pb: Option<metric::Data>) -> Value<'static> {
    pb.map(|pb| match pb {
        Data::Sum(data) => literal!({
            "sum": {
            "is_monotonic": data.is_monotonic,
            "data_points":  data_points_to_json(data.data_points),
            "aggregation_temporality": data.aggregation_temporality,
        }}),
        Data::Gauge(data) => literal!({
            "gauge": {
            "data_points":  data_points_to_json(data.data_points),
        }}),
        Data::Histogram(data) => literal!({
            "histogram": {
            "data_points":  histo_data_points_to_json(data.data_points),
            "aggregation_temporality": data.aggregation_temporality,
        }}),
        Data::ExponentialHistogram(_data) => literal!({
            "exponential-histogram": { // TODO FIXME complete
            }
        }),
        Data::Summary(data) => literal!({
            "summary": {
            "data_points":  double_summary_data_points_to_json(data.data_points),
        }}),
    })
    .unwrap_or_default()
}

pub(crate) fn metrics_data_to_pb(data: &Value<'_>) -> Result<metric::Data> {
    if let Some(json) = data.get_object("gauge") {
        let data_points = data_points_to_pb(json.get("data_points"))?;
        Ok(metric::Data::Gauge(Gauge { data_points }))
    } else if let Some(json) = data.get_object("sum") {
        let data_points = data_points_to_pb(json.get("data_points"))?;
        let is_monotonic = pb::maybe_bool_to_pb(json.get("is_monotonic"))?;
        let aggregation_temporality = pb::maybe_int_to_pbi32(json.get("aggregation_temporality"))?;
        Ok(metric::Data::Sum(Sum {
            data_points,
            aggregation_temporality,
            is_monotonic,
        }))
    } else if let Some(json) = data.get_object("histogram") {
        let data_points = histo_data_points_to_pb(json.get("data_points"))?;
        let aggregation_temporality = pb::maybe_int_to_pbi32(json.get("aggregation_temporality"))?;
        Ok(metric::Data::Histogram(Histogram {
            data_points,
            aggregation_temporality,
        }))
    } else if let Some(json) = data.get_object("summary") {
        let data_points = double_summary_data_points_to_pb(json.get("data_points"))?;
        Ok(metric::Data::Summary(Summary { data_points }))
    } else {
        Err("Invalid metric data point type - cannot convert to pb".into())
    }
}

fn metric_to_json(metric: Metric) -> Value<'static> {
    literal!({
        "name": metric.name,
        "description": metric.description,
        "data": metrics_data_to_json(metric.data),
        "unit": metric.unit,
    })
}

pub(crate) fn metric_to_pb(metric: &Value) -> Result<Metric> {
    Ok(Metric {
        name: pb::maybe_string_to_pb(metric.get("name"))?,
        description: pb::maybe_string_to_pb(metric.get("description"))?,
        unit: pb::maybe_string_to_pb(metric.get("unit"))?,
        data: metric.get("data").map(metrics_data_to_pb).transpose()?,
    })
}

pub(crate) fn metrics_records_to_pb(json: Option<&Value<'_>>) -> Result<Vec<Metric>> {
    if let Some(json) = json {
        json.as_array()
            .ok_or("Invalid json mapping for [MetricRecord, ...]")?
            .iter()
            .map(metric_to_pb)
            .collect()
    } else {
        Ok(vec![])
    }
}

pub(crate) fn scope_metrics_to_json(pb: Vec<ScopeMetrics>) -> Value<'static> {
    Value::Array(
        pb.into_iter()
            .map(|data| {
                literal!({
                    "scope": maybe_instrumentation_scope_to_json(data.scope),
                    "metrics": Value::Array(data.metrics.into_iter().map(metric_to_json).collect()),
                    "schema_url": data.schema_url,
                })
            })
            .collect(),
    )
}

pub(crate) fn scope_metrics_to_pb(json: Option<&Value<'_>>) -> Result<Vec<ScopeMetrics>> {
    use crate::connectors::impls::otel::logs::scope_to_pb;
    if let Some(json) = json {
        json.as_array()
            .ok_or("Invalid json mapping for ScopeMetrics")?
            .iter()
            .filter_map(Value::as_object)
            .map(|item| {
                Ok(ScopeMetrics {
                    scope: item.get("scope").map(scope_to_pb).transpose()?,
                    metrics: metrics_records_to_pb(item.get("metrics"))?,
                    schema_url: item
                        .get("schema_url")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                })
            })
            .collect()
    } else {
        Ok(vec![])
    }
}

pub(crate) fn resource_metrics_to_json(
    request: ExportMetricsServiceRequest,
) -> Result<Value<'static>> {
    let metrics: Result<Vec<Value<'static>>> = request
        .resource_metrics
        .into_iter()
        .map(|metric| {
            let mut base = literal!({ "schema_url": metric.schema_url });
            if let Some(r) = metric.resource {
                base.try_insert("resource", resource::resource_to_json(r));
            };
            base.insert("scope_metrics", scope_metrics_to_json(metric.scope_metrics))?;
            Ok(base)
        })
        .collect();

    Ok(literal!({ "metrics": metrics? }))
}

pub(crate) fn resource_metrics_to_pb(json: Option<&Value<'_>>) -> Result<Vec<ResourceMetrics>> {
    json.get_array("metrics")
        .ok_or("Invalid json mapping for otel metrics message - cannot convert to pb")?
        .iter()
        .filter_map(Value::as_object)
        .map(|item| {
            Ok(ResourceMetrics {
                schema_url: item
                    .get("schema_url")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                resource: item.get("resource").map(resource_to_pb).transpose()?,
                scope_metrics: scope_metrics_to_pb(item.get("scope_metrics"))?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    // This is just for tests
    use tremor_otelapis::opentelemetry::proto::common::v1::InstrumentationScope;
    use tremor_otelapis::opentelemetry::proto::metrics::v1::exemplar::Value as OtelMetricsExemplarValue;
    use tremor_otelapis::opentelemetry::proto::metrics::v1::number_data_point::Value as OtelMetricsNumberValue;
    use tremor_otelapis::opentelemetry::proto::resource::v1::Resource;
    use tremor_script::utils::sorted_serialize;

    use crate::connectors::utils::pb::maybe_from_value;

    use super::*;

    #[test]
    fn int_exemplars() -> Result<()> {
        let nanos = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanos);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanos);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = vec![Exemplar {
            span_id: span_id_pb.clone(),
            trace_id: trace_id_pb,
            time_unix_nano: 0,
            filtered_attributes: vec![],
            value: Some(OtelMetricsExemplarValue::AsInt(42)),
        }];
        let json = exemplars_to_json(pb.clone());
        let back_again = exemplars_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "time_unix_nano": 0,
            "span_id": span_id_json,
            "trace_id": trace_id_json,
            "filtered_attributes": {},
            "value": 42
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = exemplars_to_json(vec![]);
        let back_again = exemplars_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    fn double_exemplars() -> Result<()> {
        let nanos = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanos);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanos);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = vec![Exemplar {
            filtered_attributes: vec![],
            span_id: span_id_pb.clone(),
            trace_id: trace_id_pb,
            time_unix_nano: 0,
            value: maybe_from_value(Some(&Value::from(42.42)))?,
        }];
        let json = exemplars_to_json(pb.clone());
        let back_again = exemplars_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "time_unix_nano": 0,
            "span_id": span_id_json,
            "trace_id": trace_id_json,
            "filtered_attributes": {},
            "value": 42.42
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = exemplars_to_json(vec![]);
        let back_again = exemplars_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    fn quantile_values() -> Result<()> {
        let pb = vec![ValueAtQuantile {
            value: 42.42,
            quantile: 0.3,
        }];
        let json = quantile_values_to_json(pb.clone());
        let back_again = quantile_values_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "value": 42.42,
            "quantile": 0.3,
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = quantile_values_to_json(vec![]);
        let back_again = quantile_values_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    fn int_data_points() -> Result<()> {
        let pb = vec![NumberDataPoint {
            value: Some(OtelMetricsNumberValue::AsInt(42)),
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            flags: 0,
            exemplars: vec![],
            attributes: vec![],
        }];
        let json = data_points_to_json(pb.clone());
        let back_again = data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "value": 42,
            "start_time_unix_nano": 0,
            "time_unix_nano": 0,
            "flags": 0,
            "exemplars": [],
            "attributes": {},
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = data_points_to_json(vec![]);
        let back_again = data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    fn double_data_points() -> Result<()> {
        let pb = vec![NumberDataPoint {
            attributes: vec![],
            value: Some(OtelMetricsNumberValue::AsDouble(42.42)),
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            flags: 0,
            exemplars: vec![],
        }];
        let json = data_points_to_json(pb.clone());
        let back_again = data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "value": 42.42,
            "start_time_unix_nano": 0,
            "time_unix_nano": 0,
            "attributes": {},
            "exemplars": [],
            "flags": 0,
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = data_points_to_json(vec![]);
        let back_again = data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    fn int_histo_data_points() -> Result<()> {
        let pb = vec![HistogramDataPoint {
            start_time_unix_nano: 0u64,
            time_unix_nano: 0u64,
            flags: 0,
            exemplars: vec![],
            sum: Some(0.0),
            count: 0,
            explicit_bounds: vec![],
            bucket_counts: vec![],
            attributes: vec![],
            min: Some(0.0),
            max: Some(0.0),
        }];
        let json = histo_data_points_to_json(pb.clone());
        let back_again = histo_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "start_time_unix_nano": 0u64,
            "time_unix_nano": 0u64,
            "flags": 0u64,
            "exemplars": [],
            "sum": 0.0,
            "count": 0u64,
            "explicit_bounds": [],
            "bucket_counts": [],
            "attributes": {},
            "min": 0.0,
            "max": 0.0
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = histo_data_points_to_json(vec![]);
        let back_again = histo_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    fn double_histo_data_points() -> Result<()> {
        let pb = vec![HistogramDataPoint {
            attributes: vec![],
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            flags: 0,
            exemplars: vec![],
            sum: Some(0.0),
            count: 0,
            explicit_bounds: vec![],
            bucket_counts: vec![],
            min: Some(0.0),
            max: Some(0.0),
        }];
        let json = histo_data_points_to_json(pb.clone());
        let back_again = histo_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "start_time_unix_nano": 0u64,
            "time_unix_nano": 0u64,
            "attributes": {},
            "exemplars": [],
            "sum": 0.0,
            "count": 0u64,
            "explicit_bounds": [],
            "bucket_counts": [],
            "min": 0.0,
            "max": 0.0,
            "flags": 0
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = histo_data_points_to_json(vec![]);
        let back_again = histo_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    fn double_summary_data_points() -> Result<()> {
        let pb = vec![SummaryDataPoint {
            attributes: vec![],
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            sum: 0.0,
            count: 0,
            quantile_values: vec![ValueAtQuantile {
                value: 0.1,
                quantile: 0.2,
            }],
            flags: 0,
        }];
        let json = double_summary_data_points_to_json(pb.clone());
        let back_again = double_summary_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "start_time_unix_nano": 0u64,
            "time_unix_nano": 0u64,
            "attributes": {},
            "sum": 0.0,
            "count": 0,
            "quantile_values": [ { "value": 0.1, "quantile": 0.2 }],
            "flags": 0,
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = double_summary_data_points_to_json(vec![]);
        let back_again = double_summary_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    fn metrics_data_int_gauge() -> Result<()> {
        let pb = Some(metric::Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                value: Some(OtelMetricsNumberValue::AsInt(42)),
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                flags: 0,
                exemplars: vec![],
                attributes: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(&json)?;
        let expected: Value = literal!({
            "gauge": {
                "data_points": [{
                    "start_time_unix_nano": 0u64,
                    "time_unix_nano": 0u64,
                    "flags": 0,
                    "exemplars": [],
                    "attributes": {},
                    "value": 42
                }]
        }});
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn metrics_data_double_sum() -> Result<()> {
        let pb = Some(metric::Data::Sum(Sum {
            is_monotonic: false,
            aggregation_temporality: 0,
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                value: maybe_from_value(Some(&Value::from(43.43)))?,
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                flags: 0,
                exemplars: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(&json)?;
        let expected: Value = literal!({
            "sum": {
                "is_monotonic": false,
                "aggregation_temporality": 0i64,
                "data_points": [{
                    "start_time_unix_nano": 0,
                    "time_unix_nano": 0,
                    "attributes": {},
                    "exemplars": [],
                    "flags": 0,
                    "value": 43.43
                }]
        }});
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn metrics_data_double_gauge() -> Result<()> {
        let pb = Some(metric::Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                value: maybe_from_value(Some(&Value::from(43.43)))?,
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                flags: 0,
                exemplars: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(&json)?;
        let expected: Value = literal!({
            "gauge": {
                "data_points": [{
                    "start_time_unix_nano": 0,
                    "time_unix_nano": 0,
                    "attributes": {},
                    "exemplars": [],
                    "value": 43.43,
                    "flags": 0
                }]
        }});
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn metrics_data_double_histo() -> Result<()> {
        let pb = Some(metric::Data::Histogram(Histogram {
            aggregation_temporality: 0,
            data_points: vec![HistogramDataPoint {
                attributes: vec![],
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                flags: 0,
                exemplars: vec![],
                count: 5,
                sum: Some(10.0),
                bucket_counts: vec![],
                explicit_bounds: vec![],
                min: Some(0.1),
                max: Some(100.0),
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(&json)?;
        let expected: Value = literal!({
            "histogram": {
                "data_points": [{
                    "start_time_unix_nano": 0u64,
                    "time_unix_nano": 0u64,
                    "attributes": {},
                    "exemplars": [],
                    "sum": 10.0,
                    "count": 5u64,
                    "explicit_bounds": [],
                    "bucket_counts": [],
                    "min": 0.1,
                    "max": 100.0,
                    "flags": 0
                }],
                "aggregation_temporality": 0i64,
            }
        });
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn metrics_data_double_summary() -> Result<()> {
        let pb = Some(metric::Data::Summary(Summary {
            data_points: vec![SummaryDataPoint {
                attributes: vec![],
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                count: 0,
                flags: 0,
                sum: 0.0,
                quantile_values: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(&json)?;
        let expected: Value = literal!({
            "summary": {
                "data_points": [{
                    "start_time_unix_nano": 0u64,
                    "time_unix_nano": 0u64,
                    "attributes": {},
                    "quantile_values": [],
                    "sum": 0.0,
                    "count": 0u64,
                    "flags": 0,
                }]
        }});
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn metrics_data_int_histo() -> Result<()> {
        let pb = Some(metric::Data::Histogram(Histogram {
            aggregation_temporality: 0,
            data_points: vec![HistogramDataPoint {
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                flags: 0,
                exemplars: vec![],
                count: 5,
                sum: Some(10.0),
                bucket_counts: vec![],
                explicit_bounds: vec![],
                min: Some(0.2),
                max: Some(99.1),
                attributes: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(&json)?;
        let expected: Value = literal!({
            "histogram": {
                "data_points": [{
                    "start_time_unix_nano": 0u64,
                    "time_unix_nano": 0u64,
                    "attributes": {},
                    "exemplars": [],
                    "sum": 10f64,
                    "count": 5u64,
                    "explicit_bounds": [],
                    "bucket_counts": [],
                    "min": 0.2,
                    "max": 99.1,
                    "flags": 0
                }],
                "aggregation_temporality": 0i64
        }});
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn metrics_data_int_sum() -> Result<()> {
        let pb = Some(metric::Data::Sum(Sum {
            is_monotonic: false,
            aggregation_temporality: 0,
            data_points: vec![NumberDataPoint {
                value: Some(OtelMetricsNumberValue::AsInt(4)),
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                flags: 0,
                exemplars: vec![],
                attributes: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(&json)?;
        let expected: Value = literal!({
            "sum": {
                "is_monotonic": false,
                "data_points": [{
                    "start_time_unix_nano": 0u64,
                    "time_unix_nano": 0u64,
                    "attributes": {},
                    "exemplars": [],
                    "flags": 0u64,
                    "value": 4
                }],
                "aggregation_temporality": 0i64
        }});
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn resource_metrics() -> Result<()> {
        let pb = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                schema_url: "schema_url".into(),
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 8,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "snot".to_string(),
                        version: "v1.2.3.4".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    schema_url: "snot".to_string(),
                    metrics: vec![Metric {
                        name: "test".into(),
                        description: "blah blah blah blah".into(),
                        unit: "badgerfeet".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                value: Some(OtelMetricsNumberValue::AsInt(42)),
                                start_time_unix_nano: 0,
                                time_unix_nano: 0,
                                exemplars: vec![],
                                attributes: vec![],
                                flags: 0,
                            }],
                        })),
                    }],
                }],
            }],
        };
        let json = resource_metrics_to_json(pb.clone())?;
        let back_again = resource_metrics_to_pb(Some(&json))?;
        let expected: Value = literal!({
            "metrics": [
                {
                    "resource": { "attributes": {}, "dropped_attributes_count": 8 },
                    "schema_url": "schema_url",
                    "scope_metrics": [{
                            "schema_url": "snot",
                            "metrics": [{
                                "name": "test",
                                "description": "blah blah blah blah",
                                "unit": "badgerfeet",
                                "data": {
                                    "gauge": {
                                        "data_points": [{
                                            "start_time_unix_nano": 0u64,
                                            "time_unix_nano": 0u64,
                                            "attributes": {},
                                            "flags": 0,
                                            "exemplars": [],
                                            "value": 42
                                        }]
                                    }
                                },
                            }],
                            "scope": {
                                "name": "snot",
                                "version": "v1.2.3.4",
                                "attributes": {},
                                "dropped_attributes_count": 0
                            }
                }]
                }
            ]
        });

        assert_eq!(sorted_serialize(&expected)?, sorted_serialize(&json)?);
        assert_eq!(pb.resource_metrics, back_again);

        Ok(())
    }

    #[test]
    fn minimal_resource_metrics() {
        let rm = literal!({
            "metrics": [
                {
                    "schema_url": "bla",
                    "instrumentation_library_metrics": []
                }
            ]
        });
        assert_eq!(
            Ok(vec![ResourceMetrics {
                resource: None,
                schema_url: "bla".to_string(),
                scope_metrics: vec![],
            }]),
            resource_metrics_to_pb(Some(&rm))
        );
    }

    #[test]
    fn minimal_metric() {
        let metric = literal!({
            "name": "badger",
            "description": "snot",
            "unit": "fahrenheit",
            // surprise: no data
        });
        assert_eq!(
            Ok(Metric {
                name: "badger".to_string(),
                description: "snot".to_string(),
                unit: "fahrenheit".to_string(),
                data: None
            }),
            metric_to_pb(&metric)
        );
    }
}
