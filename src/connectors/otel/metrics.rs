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
use super::resource;
use super::trace;
use crate::errors::Result;
use simd_json::json;
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

use simd_json::{Builder, Mutable, Value as SimdJsonValue};
use tremor_value::Value;

pub(crate) fn int_exemplars_to_pb<'event>(
    json: Option<&Value<'event>>,
) -> Result<Vec<IntExemplar>> {
    if let Some(Value::Array(json)) = json {
        let mut pb = Vec::new();
        for data in json {
            let filtered_labels = common::string_key_value_to_pb(data.get("filtered_labels"))?;
            let span_id = trace::span_id_to_pb(data.get("span_id"))?;
            let trace_id = trace::trace_id_to_pb(data.get("trace_id"))?;
            let time_unix_nano = pb::maybe_int_to_pbu64(data.get("time_unix_nano"))?;
            let value = pb::maybe_int_to_pbi64(data.get("value"))?;
            pb.push(IntExemplar {
                span_id,
                trace_id,
                time_unix_nano,
                filtered_labels,
                value,
            });
        }
        return Ok(pb);
    }

    Err("Unable to map json value to Exemplars pb".into())
}

pub(crate) fn double_exemplars_to_pb<'event>(
    json: Option<&Value<'event>>,
) -> Result<Vec<DoubleExemplar>> {
    if let Some(Value::Array(json)) = json {
        let mut pb = Vec::new();
        for data in json {
            let filtered_labels = common::string_key_value_to_pb(data.get("filtered_labels"))?;
            let span_id = trace::span_id_to_pb(data.get("span_id"))?;
            let trace_id = trace::trace_id_to_pb(data.get("trace_id"))?;
            let time_unix_nano = pb::maybe_int_to_pbu64(data.get("time_unix_nano"))?;
            let value = pb::maybe_double_to_pb(data.get("value"))?;
            pb.push(DoubleExemplar {
                span_id,
                trace_id,
                time_unix_nano,
                filtered_labels,
                value,
            });
        }
        return Ok(pb);
    }

    Err("Unable to map json value to Exemplars pb".into())
}

pub(crate) fn quantile_values_to_pb<'event>(
    json: Option<&Value<'event>>,
) -> Result<Vec<ValueAtQuantile>> {
    if let Some(Value::Array(json)) = json {
        let mut arr = Vec::new();
        for data in json {
            let value = pb::maybe_double_to_pb(data.get("value"))?;
            let quantile = pb::maybe_double_to_pb(data.get("quantile"))?;
            arr.push(ValueAtQuantile { value, quantile });
        }
        return Ok(arr);
    }

    Err("Unable to map json value to ValueAtQuantiles".into())
}

pub(crate) fn int_data_points_to_json<'event>(pb: Vec<IntDataPoint>) -> Result<Value<'event>> {
    let mut json = Vec::new();
    for data in pb {
        let mut labels = Vec::new();
        for label in data.labels {
            let v = json!({"key": label.key, "value": label.value });
            labels.push(v);
        }
        let mut exemplars = Vec::new();
        for exemplar in data.exemplars {
            let mut filtered_labels = Vec::new();
            for fl in exemplar.filtered_labels {
                filtered_labels.push(json!({"key": fl.value, "value": fl.value}));
            }
            let v: Value = json!({
                "span_id": exemplar.span_id,
                "trace_id": exemplar.trace_id,
                "time_unix_nano": exemplar.time_unix_nano,
                "value": exemplar.value,
                "filtered_labels": filtered_labels,
            })
            .into();
            exemplars.push(v);
        }
        let v: Value = json!({
            "value": data.value,
            "start_time_unix_nano": data.start_time_unix_nano,
            "time_unix_nano": data.time_unix_nano,
            "labels": labels,
            "exemplars": exemplars,
        })
        .into();
        json.push(v);
    }
    Ok(Value::Array(json))
}

pub(crate) fn double_data_points_to_json<'event>(
    pb: Vec<DoubleDataPoint>,
) -> Result<Value<'event>> {
    let mut json = Vec::new();
    for data in pb {
        let mut labels = Vec::new();
        for label in data.labels {
            let v = json!({"key": label.key, "value": label.value });
            labels.push(v);
        }
        let mut exemplars = Vec::new();
        for exemplar in data.exemplars {
            let mut filtered_labels: Vec<Value> = Vec::new();
            for fl in exemplar.filtered_labels {
                filtered_labels.push(json!({"key": fl.key, "value": fl.value}).into());
            }
            let v: Value = json!({
                "span_id": exemplar.span_id,
                "trace_id": exemplar.trace_id,
                "time_unix_nano": exemplar.time_unix_nano,
                "value": exemplar.value,
                "filtered_labels": filtered_labels,
            })
            .into();
            exemplars.push(v);
        }
        let v: Value = json!({
            "value": data.value,
            "start_time_unix_nano": data.start_time_unix_nano,
            "time_unix_nano": data.time_unix_nano,
            "labels": labels,
            "exemplars": exemplars,
        })
        .into();
        json.push(v);
    }
    Ok(Value::Array(json))
}

pub(crate) fn double_histo_data_points_to_json<'event>(
    pb: Vec<DoubleHistogramDataPoint>,
) -> Result<Value<'event>> {
    let mut json = Vec::new();
    for points in pb {
        let mut labels = Vec::new();
        for label in points.labels {
            let v: Value = json!({"key": label.key, "value": label.value }).into();
            labels.push(v);
        }
        let mut exemplars = Vec::new();
        for exemplar in points.exemplars {
            let mut filtered_labels: Vec<Value> = Vec::new();
            for fl in exemplar.filtered_labels {
                filtered_labels.push(json!({"key": fl.value, "value": fl.value}).into());
            }
            let v: Value = json!({
                "span_id": exemplar.span_id,
                "trace_id": exemplar.trace_id,
                "time_unix_nano": exemplar.time_unix_nano,
                "value": exemplar.value,
                "filtered_labels": filtered_labels,
            })
            .into();
            exemplars.push(v);
        }
        let v: Value = json!({
            "start_time_unix_nano": points.start_time_unix_nano,
            "time_unix_nano": points.time_unix_nano,
            "labels": labels,
            "exemplars": exemplars,
            "sum": points.sum,
            "count": points.count,
            "explicit_bounds": points.explicit_bounds,
            "bucket_counts": points.bucket_counts,
        })
        .into();
        json.push(v);
    }

    Ok(Value::Array(json))
}

pub(crate) fn double_summary_data_points_to_json<'event>(
    pb: Vec<DoubleSummaryDataPoint>,
) -> Result<Value<'event>> {
    let mut json = Vec::new();
    for points in pb {
        let mut labels = Vec::new();
        for label in points.labels {
            let v: Value = json!({"key": label.key, "value": label.value }).into();
            labels.push(v);
        }
        let mut quantile_values = Vec::new();
        for qv in points.quantile_values {
            let v: Value = json!({ "quantile": qv.quantile, "value": qv.value }).into();
            quantile_values.push(v);
        }
        let v: Value = json!({
            "start_time_unix_nano": points.start_time_unix_nano,
            "time_unix_nano": points.time_unix_nano,
            "labels": labels,
            "quantile_values": quantile_values,
            "sum": points.sum,
            "count": points.count,
        })
        .into();
        json.push(v);
    }
    Ok(Value::Array(json))
}

pub(crate) fn int_histo_data_points_to_json<'event>(
    pb: Vec<IntHistogramDataPoint>,
) -> Result<Value<'event>> {
    let mut json = Vec::new();
    for points in pb {
        let mut labels = Vec::new();
        for label in points.labels {
            let v: Value = json!({"key": label.key, "value": label.value }).into();
            labels.push(v);
        }
        let mut exemplars = Vec::new();
        for exemplar in points.exemplars {
            let mut filtered_labels: Vec<Value> = Vec::new();
            for fl in exemplar.filtered_labels {
                filtered_labels.push(json!({"key": fl.value, "value": fl.value}).into());
            }
            let v: Value = json!({
                "span_id": exemplar.span_id,
                "trace_id": exemplar.trace_id,
                "time_unix_nano": exemplar.time_unix_nano,
                "value": exemplar.value,
                "filtered_labels": filtered_labels,
            })
            .into();
            exemplars.push(v);
        }
        let v: Value = json!({
            "start_time_unix_nano": points.start_time_unix_nano,
            "time_unix_nano": points.time_unix_nano,
            "labels": labels,
            "exemplars": exemplars,
            "sum": points.sum,
            "count": points.count,
            "explicit_bounds": points.explicit_bounds,
            "bucket_counts": points.bucket_counts,
        })
        .into();
        json.push(v);
    }

    Ok(Value::Array(json))
}

pub(crate) fn int_sum_data_points_to_json<'event>(pb: Vec<IntDataPoint>) -> Result<Value<'event>> {
    let mut json = Vec::new();
    for points in pb {
        let mut labels = Vec::new();
        for label in points.labels {
            let v: Value = json!({"key": label.key, "value": label.value }).into();
            labels.push(v);
        }
        let mut exemplars = Vec::new();
        for exemplar in points.exemplars {
            let mut filtered_labels: Vec<Value> = Vec::new();
            for fl in exemplar.filtered_labels {
                filtered_labels.push(json!({"key": fl.value, "value": fl.value}).into());
            }
            let v: Value = json!({
                "span_id": exemplar.span_id,
                "trace_id": exemplar.trace_id,
                "time_unix_nano": exemplar.time_unix_nano,
                "value": exemplar.value,
                "filtered_labels": filtered_labels,
            })
            .into();
            exemplars.push(v);
        }
        let v: Value = json!({
            "value": points.value,
            "start_time_unix_nano": points.start_time_unix_nano,
            "time_unix_nano": points.time_unix_nano,
            "labels": labels,
            "exemplars": exemplars,
        })
        .into();
        json.push(v);
    }
    Ok(Value::Array(json))
}

pub(crate) fn metric_data_to_pb<'event>(data: Option<&Value<'event>>) -> Result<metric::Data> {
    if let Some(Value::Object(json)) = data {
        if let Some(Value::Object(json)) = json.get("int-gauge") {
            if let Some(Value::Array(arr)) = json.get("data_points") {
                let mut data_points = Vec::new();
                for item in arr {
                    let labels = common::string_key_value_to_pb(item.get("labels"))?;
                    let exemplars = int_exemplars_to_pb(item.get("exemplars"))?;
                    let time_unix_nano = pb::maybe_int_to_pbu64(item.get("time_unix_nano"))?;
                    let start_time_unix_nano =
                        pb::maybe_int_to_pbu64(item.get("start_time_unix_nano"))?;
                    let value = pb::maybe_int_to_pbi64(item.get("value"))?;
                    data_points.push(IntDataPoint {
                        labels,
                        exemplars,
                        time_unix_nano,
                        start_time_unix_nano,
                        value,
                    })
                }
                return Ok(metric::Data::IntGauge(IntGauge { data_points }));
            }
        } else if let Some(Value::Object(json)) = json.get("double-gauge") {
            if let Some(Value::Array(arr)) = json.get("data_points") {
                let mut data_points = Vec::new();
                for item in arr {
                    let labels = common::string_key_value_to_pb(item.get("labels"))?;
                    let exemplars = double_exemplars_to_pb(item.get("exemplars"))?;
                    let time_unix_nano = pb::maybe_int_to_pbu64(item.get("time_unix_nano"))?;
                    let start_time_unix_nano =
                        pb::maybe_int_to_pbu64(item.get("start_time_unix_nano"))?;
                    let value = pb::maybe_double_to_pb(item.get("value"))?;
                    data_points.push(DoubleDataPoint {
                        labels,
                        exemplars,
                        time_unix_nano,
                        start_time_unix_nano,
                        value,
                    })
                }
                return Ok(metric::Data::DoubleGauge(DoubleGauge { data_points }));
            }
        } else if let Some(Value::Object(json)) = json.get("int-sum") {
            let mut data_points = Vec::new();
            if let Some(Value::Array(arr)) = json.get("data_points") {
                for item in arr {
                    let labels = common::string_key_value_to_pb(item.get("labels"))?;
                    let exemplars = int_exemplars_to_pb(item.get("exemplars"))?;
                    let time_unix_nano = pb::maybe_int_to_pbu64(item.get("time_unix_nano"))?;
                    let start_time_unix_nano =
                        pb::maybe_int_to_pbu64(item.get("start_time_unix_nano"))?;
                    let value = pb::maybe_int_to_pbi64(item.get("value"))?;
                    data_points.push(IntDataPoint {
                        labels,
                        exemplars,
                        time_unix_nano,
                        start_time_unix_nano,
                        value,
                    })
                }
            }
            let is_monotonic = pb::maybe_bool_to_pb(json.get("is_monotonic"))?;
            let aggregation_temporality =
                pb::maybe_int_to_pbi32(json.get("aggregation_temporality"))?;
            return Ok(metric::Data::IntSum(IntSum {
                is_monotonic,
                data_points,
                aggregation_temporality,
            }));
        } else if let Some(Value::Object(json)) = json.get("double-sum") {
            let mut data_points = Vec::new();
            if let Some(Value::Array(json)) = json.get("data_points") {
                for item in json {
                    let labels = common::string_key_value_to_pb(item.get("labels"))?;
                    let exemplars = double_exemplars_to_pb(item.get("exemplars"))?;
                    let time_unix_nano = pb::maybe_int_to_pbu64(item.get("time_unix_nano"))?;
                    let start_time_unix_nano =
                        pb::maybe_int_to_pbu64(item.get("start_time_unix_nano"))?;
                    let value = pb::maybe_double_to_pb(item.get("value"))?;
                    data_points.push(DoubleDataPoint {
                        labels,
                        exemplars,
                        time_unix_nano,
                        start_time_unix_nano,
                        value,
                    })
                }
            }
            let is_monotonic = pb::maybe_bool_to_pb(json.get("is_monotonic"))?;
            let aggregation_temporality =
                pb::maybe_int_to_pbi32(json.get("aggregation_temporality"))?;
            return Ok(metric::Data::DoubleSum(DoubleSum {
                is_monotonic,
                data_points,
                aggregation_temporality,
            }));
        } else if let Some(Value::Object(json)) = json.get("int-histogram") {
            let mut data_points = Vec::new();
            if let Some(Value::Array(json)) = json.get("data_points") {
                for item in json {
                    let labels = common::string_key_value_to_pb(item.get("labels"))?;
                    let time_unix_nano = pb::maybe_int_to_pbu64(item.get("time_unix_nano"))?;
                    let start_time_unix_nano =
                        pb::maybe_int_to_pbu64(item.get("start_time_unix_nano"))?;
                    let sum = pb::maybe_int_to_pbi64(item.get("sum"))?;
                    let count = pb::maybe_int_to_pbu64(item.get("count"))?;
                    let exemplars = int_exemplars_to_pb(item.get("exemplars"))?;
                    let explicit_bounds = pb::f64_repeated_to_pb(item.get("explicit_bounds"))?;
                    let bucket_counts = pb::u64_repeated_to_pb(item.get("explicit_bounds"))?;
                    data_points.push(IntHistogramDataPoint {
                        labels,
                        time_unix_nano,
                        start_time_unix_nano,
                        sum,
                        count,
                        bucket_counts,
                        explicit_bounds,
                        exemplars,
                    });
                }
            }
            let aggregation_temporality =
                pb::maybe_int_to_pbi32(json.get("aggregation_temporality"))?;
            return Ok(metric::Data::IntHistogram(IntHistogram {
                data_points,
                aggregation_temporality,
            }));
        } else if let Some(Value::Object(json)) = json.get("double-histogram") {
            let mut data_points = Vec::new();
            if let Some(Value::Array(json)) = json.get("data_points") {
                for item in json {
                    let labels = common::string_key_value_to_pb(item.get("labels"))?;
                    let time_unix_nano = pb::maybe_int_to_pbu64(item.get("time_unix_nano"))?;
                    let start_time_unix_nano =
                        pb::maybe_int_to_pbu64(item.get("start_time_unix_nano"))?;
                    let sum = pb::maybe_double_to_pb(item.get("sum"))?;
                    let count = pb::maybe_int_to_pbu64(item.get("count"))?;
                    let exemplars = double_exemplars_to_pb(item.get("exemplars"))?;
                    let explicit_bounds = pb::f64_repeated_to_pb(item.get("explicit_bounds"))?;
                    let bucket_counts = pb::u64_repeated_to_pb(item.get("explicit_bounds"))?;
                    data_points.push(DoubleHistogramDataPoint {
                        labels,
                        time_unix_nano,
                        start_time_unix_nano,
                        sum,
                        count,
                        bucket_counts,
                        explicit_bounds,
                        exemplars,
                    });
                }
            }
            let aggregation_temporality =
                pb::maybe_int_to_pbi32(json.get("aggregation_temporality"))?;
            return Ok(metric::Data::DoubleHistogram(DoubleHistogram {
                data_points,
                aggregation_temporality,
            }));
        } else if let Some(Value::Object(json)) = json.get("double-summary") {
            if let Some(Value::Array(json)) = json.get("data_points") {
                let mut data_points = Vec::new();
                for item in json {
                    let labels = common::string_key_value_to_pb(item.get("labels"))?;
                    let quantile_values = quantile_values_to_pb(item.get("quantile_values"))?;
                    let time_unix_nano = pb::maybe_int_to_pbu64(item.get("time_unix_nano"))?;
                    let start_time_unix_nano =
                        pb::maybe_int_to_pbu64(item.get("start_time_unix_nano"))?;
                    let sum = pb::maybe_double_to_pb(item.get("sum"))?;
                    let count = pb::maybe_int_to_pbu64(item.get("count"))?;
                    data_points.push(DoubleSummaryDataPoint {
                        labels,
                        quantile_values,
                        time_unix_nano,
                        start_time_unix_nano,
                        sum,
                        count,
                    })
                }
                return Ok(metric::Data::DoubleSummary(DoubleSummary { data_points }));
            }
        }
    }
    Err("Invalid metric data point type - cannot convert to pb".into())
}

pub(crate) fn instrumentation_library_metrics_to_pb<'event>(
    data: Option<&Value<'event>>,
) -> Result<InstrumentationLibraryMetrics> {
    let mut metrics = Vec::new();
    if let Some(Value::Object(data)) = data {
        if let Some(Value::Array(data)) = data.get("metrics") {
            for metric in data {
                let name: String = pb::maybe_string_to_pb(metric.get("name"))?;
                let description: String = pb::maybe_string_to_pb(metric.get("description"))?;
                let unit: String = pb::maybe_string_to_pb(metric.get("unit"))?;
                let metric_data: Option<metric::Data> =
                    Some(metric_data_to_pb(metric.get("data"))?);

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
        return Ok(e);
    }
    Err("Invalid json mapping for InstrumentationLibraryMetrics".into())
}

pub(crate) fn metrics_to_json<'event>(
    request: ExportMetricsServiceRequest,
) -> Result<Value<'event>> {
    let mut data = Value::object_with_capacity(0);
    let mut metrics = Vec::new();
    for metric in request.resource_metrics {
        let mut ilm = Vec::new();
        for el in metric.instrumentation_library_metrics {
            let instrumentation_library =
                common::maybe_instrumentation_library_to_json(el.instrumentation_library)?;

            let mut im = Vec::new();
            for m in el.metrics {
                let data: Option<Value> = match m.data {
                    Some(Data::IntGauge(data)) => Some(
                        json!({
                            "int-gauge": {
                            "data_points":  int_data_points_to_json(data.data_points)?
                        }})
                        .into(),
                    ),
                    Some(Data::DoubleSum(data)) => Some(
                        json!({
                            "double-sum": {
                            "is_monotonic": data.is_monotonic,
                            "data_points":  double_data_points_to_json(data.data_points)?,
                            "aggregation_temporality": data.aggregation_temporality,
                        }})
                        .into(),
                    ),
                    Some(Data::DoubleGauge(data)) => Some(
                        json!({
                            "double-gauge": {
                            "data_points":  double_data_points_to_json(data.data_points)?,
                        }})
                        .into(),
                    ),
                    Some(Data::DoubleHistogram(data)) => Some(
                        json!({
                            "double-histogram": {
                            "data_points":  double_histo_data_points_to_json(data.data_points)?,
                            "aggregation_temporality": data.aggregation_temporality,
                        }})
                        .into(),
                    ),
                    Some(Data::DoubleSummary(data)) => Some(
                        json!({
                            "double-summary": {
                            "data_points":  double_summary_data_points_to_json(data.data_points)?,
                        }})
                        .into(),
                    ),
                    Some(Data::IntHistogram(data)) => Some(
                        json!({
                            "int-histogram": {
                            "data_points":  int_histo_data_points_to_json(data.data_points)?,
                            "aggregation_temporality": data.aggregation_temporality,
                        }})
                        .into(),
                    ),
                    Some(Data::IntSum(data)) => Some(
                        json!({
                            "int-sum": {
                            "is_monotonic": data.is_monotonic,
                            "data_points":  int_sum_data_points_to_json(data.data_points)?,
                            "aggregation_temporality": data.aggregation_temporality,
                            }
                        })
                        .into(),
                    ),
                    None => None,
                };

                im.push(json!({
                    "name": m.name,
                    "description": m.description,
                    "data": data,
                    "unit": m.unit,
                }));
            }
            ilm.push(json!({
                "instrumentation_library": instrumentation_library,
                "metrics": im,
            }));
        }
        metrics.push(json!({
            "instrumentation_library_metrics": ilm,
            "resource": resource::resource_to_json(metric.resource)?,
        }));
    }
    data.insert("metrics", json!({ "resource_metrics": metrics }))?;

    Ok(data)
}

pub(crate) fn resource_metrics_to_pb<'event>(
    json: Option<&Value<'event>>,
) -> Result<Vec<ResourceMetrics>> {
    if let Some(Value::Object(json)) = json {
        if let Some(Value::Array(json)) = json.get("resource_metrics") {
            let mut pb = Vec::new();
            for json in json {
                if let Value::Object(json) = json {
                    let mut instrumentation_library_metrics = Vec::new();
                    if let Some(Value::Array(json)) = json.get("instrumentation_library_metrics") {
                        for data in json {
                            let item = instrumentation_library_metrics_to_pb(Some(data))?;
                            instrumentation_library_metrics.push(item);
                        }
                    }
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
