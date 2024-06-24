use std::sync::Arc;

use arrow::datatypes::DataType;
use polars::datatypes::ArrowDataType as PlDataType;

pub fn _convert_array(
    array: &Box<dyn polars_arrow::array::Array>,
) -> anyhow::Result<Arc<dyn arrow::array::Array>> {
    match array.data_type() {
        PlDataType::Int32 => {
            if let Some(_arr) = array
                .as_any()
                .downcast_ref::<polars_arrow::array::Int32Array>()
            {
                todo!()
            } else {
                anyhow::bail!(
                    "Polars array of type {} failed to downcast to array of type {}",
                    "Int32",
                    "Int32Array"
                );
            }
        }
        _ => todo!("Array conversion from polars to arrow not yet supported."),
    }
}

pub fn convert_schema(
    schema: polars_arrow::datatypes::ArrowSchema,
) -> anyhow::Result<arrow::datatypes::Schema> {
    let fields: anyhow::Result<Vec<arrow::datatypes::Field>> = schema
        .fields
        .into_iter()
        .map(|field| -> anyhow::Result<arrow::datatypes::Field> { Ok(convert_field(&field)?) })
        .collect();

    Ok(arrow::datatypes::Schema::new_with_metadata(
        arrow::datatypes::Fields::from(fields?),
        schema.metadata.into_iter().collect(),
    ))
}

pub fn convert_datatype(datatype: &PlDataType) -> anyhow::Result<DataType> {
    Ok(match datatype {
        PlDataType::Null => DataType::Null,
        PlDataType::Boolean => DataType::Boolean,
        PlDataType::Int8 => DataType::Int8,
        PlDataType::Int16 => DataType::Int16,
        PlDataType::Int32 => DataType::Int32,
        PlDataType::Int64 => DataType::Int64,
        PlDataType::UInt8 => DataType::UInt8,
        PlDataType::UInt16 => DataType::UInt16,
        PlDataType::UInt32 => DataType::UInt32,
        PlDataType::UInt64 => DataType::UInt64,
        PlDataType::Float16 => DataType::Float16,
        PlDataType::Float32 => DataType::Float32,
        PlDataType::Float64 => DataType::Float64,
        PlDataType::Timestamp(time_unit, time_zone) => DataType::Timestamp(
            convert_time_unit(time_unit),
            time_zone.as_ref().map(|s| s.to_string().into()),
        ),
        PlDataType::Date32 => DataType::Date32,
        PlDataType::Date64 => DataType::Date64,
        PlDataType::Time32(time_unit) => DataType::Time32(convert_time_unit(time_unit)),
        PlDataType::Time64(time_unit) => DataType::Time64(convert_time_unit(time_unit)),
        PlDataType::Duration(time_unit) => DataType::Duration(convert_time_unit(time_unit)),
        PlDataType::Interval(interval_unit) => {
            DataType::Interval(convert_interval_unit(interval_unit))
        }
        PlDataType::Binary => DataType::Binary,
        PlDataType::FixedSizeBinary(size) => DataType::FixedSizeBinary(i32::try_from(*size)?),
        PlDataType::LargeBinary => DataType::LargeBinary,
        PlDataType::Utf8 => DataType::Utf8,
        PlDataType::LargeUtf8 => DataType::LargeUtf8,
        PlDataType::List(field) => DataType::List(Arc::new(convert_field(field)?)),
        PlDataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(Arc::new(convert_field(field)?), i32::try_from(*size)?)
        }
        PlDataType::LargeList(field) => DataType::LargeList(Arc::new(convert_field(field)?)),
        PlDataType::Struct(field_list) => {
            let fields: anyhow::Result<Vec<arrow::datatypes::Field>> =
                field_list.iter().map(convert_field).collect();
            DataType::Struct(arrow::datatypes::Fields::from(fields?))
        }
        PlDataType::Union(_field_list, _, _mode) => {
            todo!("Union type conversion from polars is not yet implemented")
            // let fields: anyhow::Result<Vec<arrow::datatypes::Field>> =
            //     field_list.iter().zip(type_ids.iter()).map(|(field, id)| {
            //         Ok((i8::try_from(id)?, convert_field(field)?))
            //     }).collect();
            // DataType::Union(
            //     arrow::datatypes::UnionFields::from(fields?),
            //     convert_union_mode(*mode),
            // )
        }
        PlDataType::Map(field, is_sorted) => {
            DataType::Map(Arc::new(convert_field(field)?), *is_sorted)
        }
        PlDataType::Dictionary(int_type, data_type, _) => DataType::Dictionary(
            Box::new(convert_int_type(int_type)),
            Box::new(convert_datatype(data_type)?),
        ),
        PlDataType::Decimal(precision, scale) => {
            DataType::Decimal128(u8::try_from(*precision)?, i8::try_from(*scale)?)
        }
        PlDataType::Decimal256(precision, scale) => {
            DataType::Decimal256(u8::try_from(*precision)?, i8::try_from(*scale)?)
        }
        PlDataType::Extension(_, _, _) => {
            anyhow::bail!("Polars Extension datatype not supported in arrow.")
        }
        PlDataType::BinaryView => DataType::BinaryView,
        PlDataType::Utf8View => DataType::Utf8View,
        PlDataType::Unknown => {
            anyhow::bail!("Polars Unknown datatype not supported in arrow.")
        }
    })
}

pub fn convert_int_type(
    int_type: &polars_arrow::datatypes::IntegerType,
) -> arrow::datatypes::DataType {
    match int_type {
        polars_arrow::datatypes::IntegerType::Int8 => arrow::datatypes::DataType::Int8,
        polars_arrow::datatypes::IntegerType::Int16 => arrow::datatypes::DataType::Int16,
        polars_arrow::datatypes::IntegerType::Int32 => arrow::datatypes::DataType::Int32,
        polars_arrow::datatypes::IntegerType::Int64 => arrow::datatypes::DataType::Int64,
        polars_arrow::datatypes::IntegerType::UInt8 => arrow::datatypes::DataType::UInt8,
        polars_arrow::datatypes::IntegerType::UInt16 => arrow::datatypes::DataType::UInt16,
        polars_arrow::datatypes::IntegerType::UInt32 => arrow::datatypes::DataType::UInt32,
        polars_arrow::datatypes::IntegerType::UInt64 => arrow::datatypes::DataType::UInt64,
    }
}

pub fn convert_time_unit(
    time_unit: &polars_arrow::datatypes::TimeUnit,
) -> arrow::datatypes::TimeUnit {
    match time_unit {
        polars_arrow::datatypes::TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
        polars_arrow::datatypes::TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
        polars_arrow::datatypes::TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
        polars_arrow::datatypes::TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
    }
}

pub fn convert_interval_unit(
    interval_unit: &polars_arrow::datatypes::IntervalUnit,
) -> arrow::datatypes::IntervalUnit {
    match interval_unit {
        polars_arrow::datatypes::IntervalUnit::YearMonth => {
            arrow::datatypes::IntervalUnit::YearMonth
        }
        polars_arrow::datatypes::IntervalUnit::DayTime => arrow::datatypes::IntervalUnit::DayTime,
        polars_arrow::datatypes::IntervalUnit::MonthDayNano => {
            arrow::datatypes::IntervalUnit::MonthDayNano
        }
    }
}

pub fn convert_field(
    field: &polars_arrow::datatypes::Field,
) -> anyhow::Result<arrow::datatypes::Field> {
    Ok(arrow::datatypes::Field::new(
        field.name.clone(),
        convert_datatype(&field.data_type)?,
        field.is_nullable,
    ))
}

// pub fn convert_union_mode(mode: polars_arrow::datatypes::UnionMode) -> arrow::datatypes::UnionMode {
//     todo!()
// }
