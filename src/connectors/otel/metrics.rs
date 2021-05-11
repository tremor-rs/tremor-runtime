// Copyright 2020-2021, The Tremor Team
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

use super::super::pb;
use super::common;
use super::id;
use super::resource;
use crate::errors::Result;
use tremor_otelapis::opentelemetry::proto::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    metrics::v1::{
        double_summary_data_point::ValueAtQuantile,
        metric::{self, Data},
        DoubleDataPoint, DoubleExemplar, DoubleGauge, DoubleHistogram, DoubleHistogramDataPoint,
        DoubleSum, DoubleSummary, DoubleSummaryDataPoint, InstrumentationLibraryMetrics,
        IntDataPoint, IntExemplar, IntGauge, IntHistogram, IntHistogramDataPoint, IntSum, Metric,
        ResourceMetrics,
    },
};
use tremor_value::literal;
use tremor_value::StaticNode;

use tremor_value::Value;
use value_trait::ValueAccess;

pub(crate) fn int_exemplars_to_json<'event>(data: Vec<IntExemplar>) -> Value<'event> {
    let mut json: Vec<Value> = Vec::new();

    for exemplar in data {
        json.push(literal!({
            "span_id": id::hex_span_id_to_json(&exemplar.span_id),
            "trace_id": id::hex_trace_id_to_json(&exemplar.trace_id),
            "filtered_labels": common::string_key_value_to_json(exemplar.filtered_labels),
            "time_unix_nano": exemplar.time_unix_nano,
            "value": exemplar.value
        }))
    }
    Value::Array(json)
}

pub(crate) fn int_exemplars_to_pb(json: Option<&Value<'_>>) -> Result<Vec<IntExemplar>> {
    if let Some(Value::Array(json)) = json {
        let mut pb = Vec::new();
        for data in json {
            let filtered_labels = common::string_key_value_to_pb(data.get("filtered_labels"))?;
            let span_id = id::hex_span_id_to_pb(data.get("span_id"))?;
            let trace_id = id::hex_trace_id_to_pb(data.get("trace_id"))?;
            let time_unix_nano = pb::maybe_int_to_pbu64(data.get("time_unix_nano"))?;
            let value = pb::maybe_int_to_pbi64(data.get("value"))?;
            pb.push(IntExemplar {
                filtered_labels,
                time_unix_nano,
                value,
                span_id,
                trace_id,
            });
        }
        return Ok(pb);
    }

    Err("Unable to map json value to Exemplars pb".into())
}

pub(crate) fn double_exemplars_to_json<'event>(data: Vec<DoubleExemplar>) -> Value<'event> {
    let mut json: Vec<Value> = Vec::new();

    for exemplar in data {
        json.push(literal!({
            "span_id": id::hex_span_id_to_json(&exemplar.span_id),
            "trace_id": id::hex_trace_id_to_json(&exemplar.trace_id),
            "filtered_labels": common::string_key_value_to_json(exemplar.filtered_labels),
            "time_unix_nano": exemplar.time_unix_nano,
            "value": exemplar.value
        }))
    }
    Value::Array(json)
}

pub(crate) fn double_exemplars_to_pb(json: Option<&Value<'_>>) -> Result<Vec<DoubleExemplar>> {
    if let Some(Value::Array(json)) = json {
        let mut pb = Vec::with_capacity(json.len());
        for data in json {
            let filtered_labels = common::string_key_value_to_pb(data.get("filtered_labels"))?;
            let span_id = id::hex_span_id_to_pb(data.get("span_id"))?;
            let trace_id = id::hex_trace_id_to_pb(data.get("trace_id"))?;
            let time_unix_nano = pb::maybe_int_to_pbu64(data.get("time_unix_nano"))?;
            let value = pb::maybe_double_to_pb(data.get("value"))?;
            pb.push(DoubleExemplar {
                filtered_labels,
                time_unix_nano,
                value,
                span_id,
                trace_id,
            });
        }
        return Ok(pb);
    }

    Err("Unable to map json value to Exemplars pb".into())
}

pub(crate) fn quantile_values_to_json<'event>(data: Vec<ValueAtQuantile>) -> Value<'event> {
    let mut json: Vec<Value> = Vec::with_capacity(data.len());

    for data in data {
        json.push(literal!({
            "value": data.value,
            "quantile": data.quantile,
        }))
    }
    Value::Array(json)
}

pub(crate) fn quantile_values_to_pb(json: Option<&Value<'_>>) -> Result<Vec<ValueAtQuantile>> {
    if let Some(Value::Array(json)) = json {
        let mut arr = Vec::with_capacity(json.len());
        for data in json {
            let value = pb::maybe_double_to_pb(data.get("value"))?;
            let quantile = pb::maybe_double_to_pb(data.get("quantile"))?;
            arr.push(ValueAtQuantile { quantile, value });
        }
        return Ok(arr);
    }

    Err("Unable to map json value to ValueAtQuantiles".into())
}

pub(crate) fn int_data_points_to_json<'event>(pb: Vec<IntDataPoint>) -> Value<'event> {
    let mut json = Vec::with_capacity(pb.len());
    for data in pb {
        let labels = common::string_key_value_to_json(data.labels);
        let exemplars = int_exemplars_to_json(data.exemplars);
        let v: Value = literal!({
            "value": data.value,
            "start_time_unix_nano": data.start_time_unix_nano,
            "time_unix_nano": data.time_unix_nano,
            "labels": labels,
            "exemplars": exemplars,
        });
        json.push(v);
    }
    Value::Array(json)
}

pub(crate) fn int_data_points_to_pb(json: Option<&Value<'_>>) -> Result<Vec<IntDataPoint>> {
    if let Some(Value::Array(data)) = json {
        let mut pb = Vec::with_capacity(data.len());
        for item in data {
            let labels = common::string_key_value_to_pb(item.get("labels"))?;
            let exemplars = int_exemplars_to_pb(item.get("exemplars"))?;
            let time_unix_nano = pb::maybe_int_to_pbu64(item.get("time_unix_nano"))?;
            let start_time_unix_nano = pb::maybe_int_to_pbu64(item.get("start_time_unix_nano"))?;
            let value = pb::maybe_int_to_pbi64(item.get("value"))?;
            pb.push(IntDataPoint {
                labels,
                start_time_unix_nano,
                time_unix_nano,
                value,
                exemplars,
            })
        }

        return Ok(pb);
    };

    Err("Unable to map json value to otel pb IntDataPoint list".into())
}

pub(crate) fn double_data_points_to_json<'event>(pb: Vec<DoubleDataPoint>) -> Value<'event> {
    let mut json = Vec::with_capacity(pb.len());
    for data in pb {
        let labels = common::string_key_value_to_json(data.labels);
        let exemplars = double_exemplars_to_json(data.exemplars);

        let v: Value = literal!({
            "value": data.value,
            "start_time_unix_nano": data.start_time_unix_nano,
            "time_unix_nano": data.time_unix_nano,
            "labels": labels,
            "exemplars": exemplars,
        });
        json.push(v);
    }
    Value::Array(json)
}

pub(crate) fn double_data_points_to_pb(json: Option<&Value<'_>>) -> Result<Vec<DoubleDataPoint>> {
    if let Some(Value::Array(data)) = json {
        let mut pb = Vec::with_capacity(data.len());
        for item in data {
            let labels = common::string_key_value_to_pb(item.get("labels"))?;
            let exemplars = double_exemplars_to_pb(item.get("exemplars"))?;
            let time_unix_nano = pb::maybe_int_to_pbu64(item.get("time_unix_nano"))?;
            let start_time_unix_nano = pb::maybe_int_to_pbu64(item.get("start_time_unix_nano"))?;
            let value = pb::maybe_double_to_pb(item.get("value"))?;
            pb.push(DoubleDataPoint {
                labels,
                start_time_unix_nano,
                time_unix_nano,
                value,
                exemplars,
            })
        }

        return Ok(pb);
    };

    Err("Unable to map json value to otel pb DoubleDataPoint list".into())
}

pub(crate) fn double_histo_data_points_to_json<'event>(
    pb: Vec<DoubleHistogramDataPoint>,
) -> Value<'event> {
    let mut json = Vec::with_capacity(pb.len());
    for points in pb {
        let labels = common::string_key_value_to_json(points.labels);
        let exemplars = double_exemplars_to_json(points.exemplars);
        let v: Value = literal!({
            "start_time_unix_nano": points.start_time_unix_nano,
            "time_unix_nano": points.time_unix_nano,
            "labels": labels,
            "exemplars": exemplars,
            "sum": points.sum,
            "count": points.count,
            "explicit_bounds": points.explicit_bounds,
            "bucket_counts": points.bucket_counts,
        });
        json.push(v);
    }

    Value::Array(json)
}

pub(crate) fn double_histo_data_points_to_pb(
    json: Option<&Value<'_>>,
) -> Result<Vec<DoubleHistogramDataPoint>> {
    if let Some(Value::Array(data)) = json {
        let mut pb = Vec::with_capacity(data.len());
        for item in data {
            let labels = common::string_key_value_to_pb(item.get("labels"))?;
            let time_unix_nano = pb::maybe_int_to_pbu64(item.get("time_unix_nano"))?;
            let start_time_unix_nano = pb::maybe_int_to_pbu64(item.get("start_time_unix_nano"))?;
            let sum = pb::maybe_double_to_pb(item.get("sum"))?;
            let count = pb::maybe_int_to_pbu64(item.get("count"))?;
            let exemplars = double_exemplars_to_pb(item.get("exemplars"))?;
            let explicit_bounds = pb::f64_repeated_to_pb(item.get("explicit_bounds"))?;
            let bucket_counts = pb::u64_repeated_to_pb(item.get("explicit_bounds"))?;
            pb.push(DoubleHistogramDataPoint {
                labels,
                start_time_unix_nano,
                time_unix_nano,
                count,
                sum,
                bucket_counts,
                explicit_bounds,
                exemplars,
            })
        }

        return Ok(pb);
    };

    Err("Unable to map json value to otel pb DoubleHistogramDataPoint list".into())
}

pub(crate) fn double_summary_data_points_to_json<'event>(
    pb: Vec<DoubleSummaryDataPoint>,
) -> Value<'event> {
    let mut json = Vec::with_capacity(pb.len());
    for points in pb {
        let labels = common::string_key_value_to_json(points.labels);
        let quantile_values = quantile_values_to_json(points.quantile_values);
        let v: Value = literal!({
            "start_time_unix_nano": points.start_time_unix_nano,
            "time_unix_nano": points.time_unix_nano,
            "labels": labels,
            "quantile_values": quantile_values,
            "sum": points.sum,
            "count": points.count,
        });
        json.push(v);
    }
    Value::Array(json)
}

pub(crate) fn double_summary_data_points_to_pb(
    json: Option<&Value<'_>>,
) -> Result<Vec<DoubleSummaryDataPoint>> {
    if let Some(Value::Array(data)) = json {
        let mut pb = Vec::with_capacity(data.len());
        for item in data {
            let labels = common::string_key_value_to_pb(item.get("labels"))?;
            let time_unix_nano = pb::maybe_int_to_pbu64(item.get("time_unix_nano"))?;
            let start_time_unix_nano = pb::maybe_int_to_pbu64(item.get("start_time_unix_nano"))?;
            let sum = pb::maybe_double_to_pb(item.get("sum"))?;
            let count = pb::maybe_int_to_pbu64(item.get("count"))?;
            let quantile_values = quantile_values_to_pb(item.get("quantile_values"))?;
            pb.push(DoubleSummaryDataPoint {
                labels,
                start_time_unix_nano,
                time_unix_nano,
                count,
                sum,
                quantile_values,
            })
        }

        return Ok(pb);
    };

    Err("Unable to map json value to otel pb DoubleSummaryDataPoint list".into())
}

pub(crate) fn int_histo_data_points_to_json<'event>(
    pb: Vec<IntHistogramDataPoint>,
) -> Value<'event> {
    let mut json = Vec::with_capacity(pb.len());
    for points in pb {
        let labels = common::string_key_value_to_json(points.labels);
        let exemplars = int_exemplars_to_json(points.exemplars);
        let v: Value = literal!({
            "start_time_unix_nano": points.start_time_unix_nano,
            "time_unix_nano": points.time_unix_nano,
            "labels": labels,
            "exemplars": exemplars,
            "sum": points.sum,
            "count": points.count,
            "explicit_bounds": points.explicit_bounds,
            "bucket_counts": points.bucket_counts,
        });
        json.push(v);
    }

    Value::Array(json)
}

pub(crate) fn int_histo_data_points_to_pb(
    json: Option<&Value<'_>>,
) -> Result<Vec<IntHistogramDataPoint>> {
    if let Some(Value::Array(data)) = json {
        let mut pb = Vec::with_capacity(data.len());
        for item in data {
            let labels = common::string_key_value_to_pb(item.get("labels"))?;
            let time_unix_nano = pb::maybe_int_to_pbu64(item.get("time_unix_nano"))?;
            let start_time_unix_nano = pb::maybe_int_to_pbu64(item.get("start_time_unix_nano"))?;
            let sum = pb::maybe_int_to_pbi64(item.get("sum"))?;
            let count = pb::maybe_int_to_pbu64(item.get("count"))?;
            let exemplars = int_exemplars_to_pb(item.get("exemplars"))?;
            let explicit_bounds = pb::f64_repeated_to_pb(item.get("explicit_bounds"))?;
            let bucket_counts = pb::u64_repeated_to_pb(item.get("explicit_bounds"))?;
            pb.push(IntHistogramDataPoint {
                labels,
                start_time_unix_nano,
                time_unix_nano,
                count,
                sum,
                bucket_counts,
                explicit_bounds,
                exemplars,
            })
        }

        return Ok(pb);
    };

    Err("Unable to map json value to otel pb IntHistogramDataPoint list".into())
}

pub(crate) fn int_sum_data_points_to_json<'event>(pb: Vec<IntDataPoint>) -> Value<'event> {
    int_data_points_to_json(pb)
}

pub(crate) fn metrics_data_to_json<'event>(pb: Option<metric::Data>) -> Value<'event> {
    if let Some(pb) = pb {
        let json: Value = match pb {
            Data::IntGauge(data) => literal!({
                "int-gauge": {
                "data_points":  int_data_points_to_json(data.data_points)
            }}),
            Data::DoubleSum(data) => literal!({
                "double-sum": {
                "is_monotonic": data.is_monotonic,
                "data_points":  double_data_points_to_json(data.data_points),
                "aggregation_temporality": data.aggregation_temporality,
            }}),
            Data::DoubleGauge(data) => literal!({
                "double-gauge": {
                "data_points":  double_data_points_to_json(data.data_points),
            }}),
            Data::DoubleHistogram(data) => literal!({
                "double-histogram": {
                "data_points":  double_histo_data_points_to_json(data.data_points),
                "aggregation_temporality": data.aggregation_temporality,
            }}),
            Data::DoubleSummary(data) => literal!({
                "double-summary": {
                "data_points":  double_summary_data_points_to_json(data.data_points),
            }}),
            Data::IntHistogram(data) => literal!({
                "int-histogram": {
                "data_points":  int_histo_data_points_to_json(data.data_points),
                "aggregation_temporality": data.aggregation_temporality,
            }}),
            Data::IntSum(data) => literal!({
                "int-sum": {
                "is_monotonic": data.is_monotonic,
                "data_points":  int_sum_data_points_to_json(data.data_points),
                "aggregation_temporality": data.aggregation_temporality,
                }
            }),
        };

        json
    } else {
        Value::Static(StaticNode::Null)
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn metrics_data_to_pb(data: Option<&Value<'_>>) -> Result<metric::Data> {
    if let Some(Value::Object(json)) = data {
        if let Some(Value::Object(json)) = json.get("int-gauge") {
            let data_points = int_data_points_to_pb(json.get("data_points"))?;
            return Ok(metric::Data::IntGauge(IntGauge { data_points }));
        } else if let Some(Value::Object(json)) = json.get("double-gauge") {
            let data_points = double_data_points_to_pb(json.get("data_points"))?;
            return Ok(metric::Data::DoubleGauge(DoubleGauge { data_points }));
        } else if let Some(Value::Object(json)) = json.get("int-sum") {
            let data_points = int_data_points_to_pb(json.get("data_points"))?;
            let is_monotonic = pb::maybe_bool_to_pb(json.get("is_monotonic"))?;
            let aggregation_temporality =
                pb::maybe_int_to_pbi32(json.get("aggregation_temporality"))?;
            return Ok(metric::Data::IntSum(IntSum {
                data_points,
                aggregation_temporality,
                is_monotonic,
            }));
        } else if let Some(Value::Object(json)) = json.get("double-sum") {
            let data_points = double_data_points_to_pb(json.get("data_points"))?;
            let is_monotonic = pb::maybe_bool_to_pb(json.get("is_monotonic"))?;
            let aggregation_temporality =
                pb::maybe_int_to_pbi32(json.get("aggregation_temporality"))?;
            return Ok(metric::Data::DoubleSum(DoubleSum {
                data_points,
                aggregation_temporality,
                is_monotonic,
            }));
        } else if let Some(Value::Object(json)) = json.get("int-histogram") {
            let data_points = int_histo_data_points_to_pb(json.get("data_points"))?;
            let aggregation_temporality =
                pb::maybe_int_to_pbi32(json.get("aggregation_temporality"))?;
            return Ok(metric::Data::IntHistogram(IntHistogram {
                data_points,
                aggregation_temporality,
            }));
        } else if let Some(Value::Object(json)) = json.get("double-histogram") {
            let data_points = double_histo_data_points_to_pb(json.get("data_points"))?;
            let aggregation_temporality =
                pb::maybe_int_to_pbi32(json.get("aggregation_temporality"))?;
            return Ok(metric::Data::DoubleHistogram(DoubleHistogram {
                data_points,
                aggregation_temporality,
            }));
        } else if let Some(Value::Object(json)) = json.get("double-summary") {
            let data_points = double_summary_data_points_to_pb(json.get("data_points"))?;
            return Ok(metric::Data::DoubleSummary(DoubleSummary { data_points }));
        }
    }
    Err("Invalid metric data point type - cannot convert to pb".into())
}

pub(crate) fn instrumentation_library_metrics_to_json<'event>(
    pb: Vec<tremor_otelapis::opentelemetry::proto::metrics::v1::InstrumentationLibraryMetrics>,
) -> Value<'event> {
    let mut json = Vec::with_capacity(pb.len());
    for data in pb {
        let mut metrics = Vec::new();
        for metric in data.metrics {
            let data = metrics_data_to_json(metric.data);

            metrics.push(literal!({
                "name": metric.name,
                "description": metric.description,
                "data": data,
                "unit": metric.unit,
            }));
        }
        json.push(literal!({
            "instrumentation_library": common::maybe_instrumentation_library_to_json(data.instrumentation_library),
            "metrics": metrics
        }));
    }

    literal!(json)
}

pub(crate) fn instrumentation_library_metrics_to_pb(
    data: Option<&Value<'_>>,
) -> Result<Vec<InstrumentationLibraryMetrics>> {
    if let Some(Value::Array(data)) = data {
        let mut pb = Vec::with_capacity(data.len());
        for ilm in data {
            if let Value::Object(data) = ilm {
                let mut metrics = Vec::new();
                if let Some(Value::Array(data)) = data.get("metrics") {
                    for metric in data {
                        let name: String = pb::maybe_string_to_pb(metric.get("name"))?;
                        let description: String =
                            pb::maybe_string_to_pb(metric.get("description"))?;
                        let unit: String = pb::maybe_string_to_pb(metric.get("unit"))?;
                        let metric_data: Option<metric::Data> =
                            Some(metrics_data_to_pb(metric.get("data"))?);

                        metrics.push(Metric {
                            name,
                            description,
                            unit,
                            data: metric_data,
                        });
                    }
                }
                let il = data.get("instrumentation_library");
                let e = InstrumentationLibraryMetrics {
                    instrumentation_library: common::maybe_instrumentation_library_to_pb(il)?,
                    metrics,
                };
                pb.push(e);
            }
        }
        return Ok(pb);
    }

    Err("Invalid json mapping for InstrumentationLibraryMetrics".into())
}

pub(crate) fn resource_metrics_to_json<'event>(
    request: ExportMetricsServiceRequest,
) -> Result<Value<'event>> {
    let mut metrics: Vec<Value> = Vec::with_capacity(request.resource_metrics.len());
    for metric in request.resource_metrics {
        let ilm = instrumentation_library_metrics_to_json(metric.instrumentation_library_metrics);
        metrics.push(literal!({
            "instrumentation_library_metrics": ilm,
            "resource": resource::resource_to_json(metric.resource)?,
        }));
    }

    Ok(literal!({ "metrics": metrics }))
}

pub(crate) fn resource_metrics_to_pb(json: Option<&Value<'_>>) -> Result<Vec<ResourceMetrics>> {
    if let Some(Value::Object(json)) = json {
        if let Some(Value::Array(json)) = json.get("metrics") {
            let mut pb = Vec::with_capacity(json.len());
            for json in json {
                if let Value::Object(json) = json {
                    let instrumentation_library_metrics = instrumentation_library_metrics_to_pb(
                        json.get("instrumentation_library_metrics"),
                    )?;
                    let resource = Some(resource::maybe_resource_to_pb(json.get("resource"))?);
                    let item = ResourceMetrics {
                        resource,
                        instrumentation_library_metrics,
                    };
                    pb.push(item);
                }
            }
            return Ok(pb);
        }
    }

    Err("Invalid json mapping for otel metrics message - cannot convert to pb".into())
}

#[cfg(test)]
mod tests {
    use tremor_otelapis::opentelemetry::proto::{
        common::v1::InstrumentationLibrary, resource::v1::Resource,
    };

    use super::*;

    #[test]
    fn int_exemplars() -> Result<()> {
        let nanos = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanos);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanos);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = vec![IntExemplar {
            span_id: span_id_pb.clone(),
            trace_id: trace_id_pb,
            time_unix_nano: 0,
            filtered_labels: vec![],
            value: 42,
        }];
        let json = int_exemplars_to_json(pb.clone());
        let back_again = int_exemplars_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "time_unix_nano": 0,
            "span_id": span_id_json,
            "trace_id": trace_id_json,
            "filtered_labels": {},
            "value": 42
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = int_exemplars_to_json(vec![]);
        let back_again = int_exemplars_to_pb(Some(&json))?;
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

        let pb = vec![DoubleExemplar {
            span_id: span_id_pb.clone(),
            trace_id: trace_id_pb,
            time_unix_nano: 0,
            filtered_labels: vec![],
            value: 42.42,
        }];
        let json = double_exemplars_to_json(pb.clone());
        let back_again = double_exemplars_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "time_unix_nano": 0,
            "span_id": span_id_json,
            "trace_id": trace_id_json,
            "filtered_labels": {},
            "value": 42.42
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = double_exemplars_to_json(vec![]);
        let back_again = double_exemplars_to_pb(Some(&json))?;
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
        let pb = vec![IntDataPoint {
            value: 42,
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            labels: vec![],
            exemplars: vec![],
        }];
        let json = int_data_points_to_json(pb.clone());
        let back_again = int_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "value": 42,
            "start_time_unix_nano": 0,
            "time_unix_nano": 0,
            "labels": {},
            "exemplars": []
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = int_data_points_to_json(vec![]);
        let back_again = int_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    fn double_data_points() -> Result<()> {
        let pb = vec![DoubleDataPoint {
            value: 42.42,
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            labels: vec![],
            exemplars: vec![],
        }];
        let json = double_data_points_to_json(pb.clone());
        let back_again = double_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "value": 42.42,
            "start_time_unix_nano": 0,
            "time_unix_nano": 0,
            "labels": {},
            "exemplars": []
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = double_data_points_to_json(vec![]);
        let back_again = double_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    fn int_histo_data_points() -> Result<()> {
        let pb = vec![IntHistogramDataPoint {
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            labels: vec![],
            exemplars: vec![],
            sum: 0,
            count: 0,
            explicit_bounds: vec![],
            bucket_counts: vec![],
        }];
        let json = int_histo_data_points_to_json(pb.clone());
        let back_again = int_histo_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "start_time_unix_nano": 0,
            "time_unix_nano": 0,
            "labels": {},
            "exemplars": [],
            "sum": 0,
            "count": 0,
            "explicit_bounds": [],
            "bucket_counts": [],
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = int_histo_data_points_to_json(vec![]);
        let back_again = int_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    fn double_histo_data_points() -> Result<()> {
        let pb = vec![DoubleHistogramDataPoint {
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            labels: vec![],
            exemplars: vec![],
            sum: 0.0,
            count: 0,
            explicit_bounds: vec![],
            bucket_counts: vec![],
        }];
        let json = double_histo_data_points_to_json(pb.clone());
        let back_again = double_histo_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "start_time_unix_nano": 0,
            "time_unix_nano": 0,
            "labels": {},
            "exemplars": [],
            "sum": 0.0,
            "count": 0,
            "explicit_bounds": [],
            "bucket_counts": [],
        }]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty
        let json = double_histo_data_points_to_json(vec![]);
        let back_again = double_histo_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    fn double_summary_data_points() -> Result<()> {
        let pb = vec![DoubleSummaryDataPoint {
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            labels: vec![],
            sum: 0.0,
            count: 0,
            quantile_values: vec![ValueAtQuantile {
                value: 0.1,
                quantile: 0.2,
            }],
        }];
        let json = double_summary_data_points_to_json(pb.clone());
        let back_again = double_summary_data_points_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "start_time_unix_nano": 0,
            "time_unix_nano": 0,
            "labels": {},
            "sum": 0.0,
            "count": 0,
            "quantile_values": [ { "value": 0.1, "quantile": 0.2 }]
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
        let pb = Some(metric::Data::IntGauge(IntGauge {
            data_points: vec![IntDataPoint {
                value: 42,
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                labels: vec![],
                exemplars: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(Some(&json))?;
        let expected: Value = literal!({
            "int-gauge": {
                "data_points": [{
                    "start_time_unix_nano": 0,
                    "time_unix_nano": 0,
                    "labels": {},
                    "exemplars": [],
                    "value": 42
                }]
        }});
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn metrics_data_double_sum() -> Result<()> {
        let pb = Some(metric::Data::DoubleSum(DoubleSum {
            is_monotonic: false,
            aggregation_temporality: 0,
            data_points: vec![DoubleDataPoint {
                value: 43.43,
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                labels: vec![],
                exemplars: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(Some(&json))?;
        let expected: Value = literal!({
            "double-sum": {
                "is_monotonic": false,
                "aggregation_temporality": 0,
                "data_points": [{
                    "start_time_unix_nano": 0,
                    "time_unix_nano": 0,
                    "labels": {},
                    "exemplars": [],
                    "value": 43.43
                }]
        }});
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn metrics_data_double_gauge() -> Result<()> {
        let pb = Some(metric::Data::DoubleGauge(DoubleGauge {
            data_points: vec![DoubleDataPoint {
                value: 43.43,
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                labels: vec![],
                exemplars: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(Some(&json))?;
        let expected: Value = literal!({
            "double-gauge": {
                "data_points": [{
                    "start_time_unix_nano": 0,
                    "time_unix_nano": 0,
                    "labels": {},
                    "exemplars": [],
                    "value": 43.43
                }]
        }});
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn metrics_data_double_histo() -> Result<()> {
        let pb = Some(metric::Data::DoubleHistogram(DoubleHistogram {
            aggregation_temporality: 0,
            data_points: vec![DoubleHistogramDataPoint {
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                labels: vec![],
                exemplars: vec![],
                count: 5,
                sum: 10.0,
                bucket_counts: vec![],
                explicit_bounds: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(Some(&json))?;
        let expected: Value = literal!({
            "double-histogram": {
                "aggregation_temporality": 0,
                "data_points": [{
                    "start_time_unix_nano": 0,
                    "time_unix_nano": 0,
                    "labels": {},
                    "exemplars": [],
                    "sum": 10.0,
                    "count": 5,
                    "bucket_counts": [],
                    "explicit_bounds": []
                }]
            }
        });
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn metrics_data_double_summary() -> Result<()> {
        let pb = Some(metric::Data::DoubleSummary(DoubleSummary {
            data_points: vec![DoubleSummaryDataPoint {
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                labels: vec![],
                count: 0,
                sum: 0.0,
                quantile_values: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(Some(&json))?;
        let expected: Value = literal!({
            "double-summary": {
                "data_points": [{
                    "start_time_unix_nano": 0,
                    "time_unix_nano": 0,
                    "labels": {},
                    "count": 0,
                    "sum": 0.0,
                    "quantile_values": []
                }]
        }});
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn metrics_data_int_histo() -> Result<()> {
        let pb = Some(metric::Data::IntHistogram(IntHistogram {
            aggregation_temporality: 0,
            data_points: vec![IntHistogramDataPoint {
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                labels: vec![],
                exemplars: vec![],
                count: 5,
                sum: 10,
                bucket_counts: vec![],
                explicit_bounds: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(Some(&json))?;
        let expected: Value = literal!({
            "int-histogram": {
                "aggregation_temporality": 0,
                "data_points": [{
                    "start_time_unix_nano": 0,
                    "time_unix_nano": 0,
                    "labels": {},
                    "exemplars": [],
                    "count": 5,
                    "sum": 10,
                    "bucket_counts": [],
                    "explicit_bounds": []
                }]
        }});
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn metrics_data_int_sum() -> Result<()> {
        let pb = Some(metric::Data::IntSum(IntSum {
            is_monotonic: false,
            aggregation_temporality: 0,
            data_points: vec![IntDataPoint {
                value: 4,
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                labels: vec![],
                exemplars: vec![],
            }],
        }));

        let json = metrics_data_to_json(pb.clone());
        let back_again = metrics_data_to_pb(Some(&json))?;
        let expected: Value = literal!({
            "int-sum": {
                "is_monotonic": false,
                "aggregation_temporality": 0,
                "data_points": [{
                    "start_time_unix_nano": 0,
                    "time_unix_nano": 0,
                    "labels": {},
                    "exemplars": [],
                    "value": 4
                }]
        }});
        assert_eq!(expected, json);
        assert_eq!(pb, Some(back_again));
        Ok(())
    }

    #[test]
    fn instrumentation_library_metrics() -> Result<()> {
        let pb = vec![InstrumentationLibraryMetrics {
            instrumentation_library: Some(InstrumentationLibrary {
                name: "name".into(),
                version: "v0.1.2".into(),
            }), // TODO For now its an error for this to be None - may need to revisit
            metrics: vec![Metric {
                name: "test".into(),
                description: "blah blah blah blah".into(),
                unit: "badgerfeet".into(),
                data: Some(metric::Data::IntGauge(IntGauge {
                    data_points: vec![IntDataPoint {
                        value: 42,
                        start_time_unix_nano: 0,
                        time_unix_nano: 0,
                        labels: vec![],
                        exemplars: vec![],
                    }],
                })),
            }],
        }];
        let json = instrumentation_library_metrics_to_json(pb.clone());
        let back_again = instrumentation_library_metrics_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "instrumentation_library": { "name": "name", "version": "v0.1.2" },
            "metrics": [{
                "name": "test",
                "description": "blah blah blah blah",
                "unit": "badgerfeet",
                "data": {
                    "int-gauge": {
                        "data_points": [{
                            "start_time_unix_nano": 0,
                            "time_unix_nano": 0,
                            "labels": {},
                            "exemplars": [],
                            "value": 42
                        }]
                    }
                },
            }]
        }]);

        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn resource_metrics() -> Result<()> {
        let pb = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 8,
                }),
                instrumentation_library_metrics: vec![InstrumentationLibraryMetrics {
                    instrumentation_library: Some(InstrumentationLibrary {
                        name: "name".into(),
                        version: "v0.1.2".into(),
                    }), // TODO For now its an error for this to be None - may need to revisit
                    metrics: vec![Metric {
                        name: "test".into(),
                        description: "blah blah blah blah".into(),
                        unit: "badgerfeet".into(),
                        data: Some(metric::Data::IntGauge(IntGauge {
                            data_points: vec![IntDataPoint {
                                value: 42,
                                start_time_unix_nano: 0,
                                time_unix_nano: 0,
                                labels: vec![],
                                exemplars: vec![],
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
                    "instrumentation_library_metrics": [{
                            "instrumentation_library": { "name": "name", "version": "v0.1.2" },
                            "metrics": [{
                                "name": "test",
                                "description": "blah blah blah blah",
                                "unit": "badgerfeet",
                                "data": {
                                    "int-gauge": {
                                        "data_points": [{
                                            "start_time_unix_nano": 0,
                                            "time_unix_nano": 0,
                                            "labels": {},
                                            "exemplars": [],
                                            "value": 42
                                        }]
                                    }
                                },
                            }]
                    }]
                }
            ]
        });

        assert_eq!(expected, json);
        assert_eq!(pb.resource_metrics, back_again);

        Ok(())
    }
}
