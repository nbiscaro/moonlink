use crate::pg_replicate::{
    conversions::{numeric::PgNumeric, table_row::TableRow, ArrayCell, Cell},
    table::{LookupKey, TableSchema},
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_schema::extension::{ExtensionType, Json as ArrowJson, Uuid as ArrowUuid};
use arrow_schema::{DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE};
use chrono::Timelike;
use moonlink::row::RowValue;
use moonlink::row::{IdentityProp, MoonlinkRow};
use num_traits::cast::ToPrimitive;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::types::{Kind, Type};
use tracing::warn;

fn numeric_precision_scale(modifier: i32) -> Option<(u8, i8)> {
    const VARHDRSZ: i32 = 4;
    if modifier < VARHDRSZ {
        return None;
    }
    let typmod = modifier - VARHDRSZ;
    // Derived from: [https://github.com/postgres/postgres/blob/4fbb46f61271f4b7f46ecad3de608fc2f4d7d80f/src/backend/utils/adt/numeric.c#L929v]
    let precision = ((typmod >> 16) & 0xffff) as u8;
    // Derived from: [https://github.com/postgres/postgres/blob/4fbb46f61271f4b7f46ecad3de608fc2f4d7d80f/src/backend/utils/adt/numeric.c#L944]
    let raw_scale = (typmod & 0x7ff);
    let scale = ((raw_scale ^ 1024) - 1024) as i8;
    Some((precision, scale))
}

enum ArrowExtensionType {
    Uuid,
    Json,
}

fn postgres_primitive_to_arrow_type(
    typ: &Type,
    modifier: i32,
    name: &str,
    mut nullable: bool,
    field_id: &mut i32,
) -> Field {
    let (data_type, extension_name) = match *typ {
        Type::BOOL => (DataType::Boolean, None),
        Type::INT2 => (DataType::Int16, None),
        Type::INT4 => (DataType::Int32, None),
        Type::INT8 => (DataType::Int64, None),
        Type::FLOAT4 => (DataType::Float32, None),
        Type::FLOAT8 => (DataType::Float64, None),
        Type::NUMERIC => {
            // Numeric type can contain invalid values, we will cast them to NULL
            // so make it nullable.
            nullable = true;
            let (precision, scale) = numeric_precision_scale(modifier)
                .unwrap_or((DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE));
            (DataType::Decimal128(precision, scale), None)
        }
        Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::CHAR | Type::NAME => {
            (DataType::Utf8, None)
        }
        Type::DATE => (DataType::Date32, None),
        Type::TIMESTAMP => (
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            None,
        ),
        Type::TIMESTAMPTZ => (
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            None,
        ),
        Type::TIME => (
            DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            None,
        ),
        Type::TIMETZ => (
            DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            None,
        ),
        Type::UUID => (
            DataType::FixedSizeBinary(16),
            Some(ArrowExtensionType::Uuid),
        ),
        Type::JSON | Type::JSONB => (DataType::Utf8, Some(ArrowExtensionType::Json)),
        Type::BYTEA => (DataType::Binary, None),
        // The type alias for postgres OID is uint32, but iceberg-rust doesn't support unsigned type, so use int64 instead.
        Type::OID => (DataType::Int64, None),
        _ => (DataType::Utf8, None), // Default to string for unknown types
    };

    let mut field = Field::new(name, data_type, nullable);
    let mut metadata = HashMap::new();
    metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
    *field_id += 1;
    field = field.with_metadata(metadata);

    // Apply extension type if specified
    if let Some(ext_name) = extension_name {
        match ext_name {
            ArrowExtensionType::Uuid => {
                field = field.with_extension_type(ArrowUuid::default());
            }
            ArrowExtensionType::Json => {
                field = field.with_extension_type(ArrowJson::default());
            }
        }
    }

    field
}

fn postgres_type_to_arrow_type(
    typ: &Type,
    modifier: i32,
    name: &str,
    nullable: bool,
    field_id: &mut i32,
) -> Field {
    match typ.kind() {
        Kind::Simple => postgres_primitive_to_arrow_type(typ, modifier, name, nullable, field_id),
        Kind::Array(inner) => {
            let item_type = postgres_type_to_arrow_type(
                inner, /*modifier=*/ -1, /*name=*/ "item", /*nullable=*/ true,
                field_id,
            );
            let field = Field::new_list(name, Arc::new(item_type), nullable);
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            field.with_metadata(metadata)
        }
        Kind::Composite(fields) => {
            let fields: Vec<Field> = fields
                .iter()
                .map(|f| {
                    postgres_type_to_arrow_type(
                        f.type_(),
                        /*modifier=*/ -1,
                        f.name(),
                        /*nullable=*/ true,
                        field_id,
                    )
                })
                .collect();
            let mut field = Field::new_struct(name, fields, nullable);
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            field.with_metadata(metadata)
        }
        Kind::Enum(_) => Field::new(name, DataType::Utf8, nullable),
        _ => {
            todo!("Unsupported type: {:?}", typ);
        }
    }
}

/// Convert a PostgreSQL TableSchema to an Arrow Schema
pub fn postgres_schema_to_moonlink_schema(table_schema: &TableSchema) -> (Schema, IdentityProp) {
    let mut field_id = 0; // Used to indicate different columns, including internal fields within complex type.
    let fields: Vec<Field> = table_schema
        .column_schemas
        .iter()
        .map(|col| {
            postgres_type_to_arrow_type(
                &col.typ,
                col.modifier,
                &col.name,
                col.nullable,
                &mut field_id,
            )
        })
        .collect();

    let identity = match &table_schema.lookup_key {
        LookupKey::Key { name: _, columns } => {
            let columns = columns
                .iter()
                .map(|c| {
                    table_schema
                        .column_schemas
                        .iter()
                        .position(|cs| cs.name == *c)
                        .unwrap()
                })
                .collect();
            IdentityProp::new_key(columns, &fields)
        }
        LookupKey::FullRow => IdentityProp::FullRow,
    };
    (Schema::new(fields), identity)
}

pub fn _table_schema_to_iceberg_schema(_table_schema: &TableSchema) -> Schema {
    todo!("Iceberg: convert postgres table schema to iceberg schema!")
}

pub(crate) struct PostgresTableRow(pub TableRow);

const ARROW_EPOCH: chrono::NaiveDate = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

fn convert_array_cell(cell: ArrayCell) -> Vec<RowValue> {
    match cell {
        ArrayCell::Null => vec![],
        ArrayCell::Bool(values) => values
            .into_iter()
            .map(|v| v.map(RowValue::Bool).unwrap_or(RowValue::Null))
            .collect(),
        ArrayCell::String(values) => values
            .into_iter()
            .map(|v| {
                v.map(|s| RowValue::ByteArray(s.into_bytes()))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::I16(values) => values
            .into_iter()
            .map(|v| {
                v.map(|i| RowValue::Int32(i as i32))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::I32(values) => values
            .into_iter()
            .map(|v| v.map(RowValue::Int32).unwrap_or(RowValue::Null))
            .collect(),
        ArrayCell::U32(values) => values
            .into_iter()
            .map(|v| {
                v.map(|i| RowValue::Int64(i as i64))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::I64(values) => values
            .into_iter()
            .map(|v| v.map(RowValue::Int64).unwrap_or(RowValue::Null))
            .collect(),
        ArrayCell::F32(values) => values
            .into_iter()
            .map(|v| v.map(RowValue::Float32).unwrap_or(RowValue::Null))
            .collect(),
        ArrayCell::F64(values) => values
            .into_iter()
            .map(|v| v.map(RowValue::Float64).unwrap_or(RowValue::Null))
            .collect(),
        ArrayCell::Numeric(values) => values
            .into_iter()
            .map(|v| {
                v.map(|n| match n {
                    PgNumeric::Value(bigdecimal) => {
                        let (int_val, _) = bigdecimal.into_bigint_and_exponent();
                        RowValue::Decimal(int_val.to_i128().unwrap())
                    }
                    _ => RowValue::Null,
                })
                .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::Date(values) => values
            .into_iter()
            .map(|v| {
                v.map(|d| RowValue::Int32(d.signed_duration_since(ARROW_EPOCH).num_days() as i32))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::Time(values) => values
            .into_iter()
            .map(|v| {
                v.map(|t| {
                    RowValue::Int64(
                        t.num_seconds_from_midnight() as i64 * 1_000_000
                            + t.nanosecond() as i64 / 1_000,
                    )
                })
                .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::TimeStamp(values) => values
            .into_iter()
            .map(|v| {
                v.map(|t| RowValue::Int64(t.and_utc().timestamp_micros()))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::TimeStampTz(values) => values
            .into_iter()
            .map(|v| {
                v.map(|t| RowValue::Int64(t.timestamp_micros()))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::Uuid(values) => values
            .into_iter()
            .map(|v| {
                v.map(|u| RowValue::FixedLenByteArray(*u.as_bytes()))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::Json(values) => values
            .into_iter()
            .map(|v| {
                v.map(|j| RowValue::ByteArray(serde_json::to_vec(&j).unwrap()))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::Bytes(values) => values
            .into_iter()
            .map(|v| v.map(RowValue::ByteArray).unwrap_or(RowValue::Null))
            .collect(),
        ArrayCell::Composite(values) => values
            .into_iter()
            .map(|v| {
                v.map(|cells| {
                    let struct_values: Vec<RowValue> =
                        cells.into_iter().map(|cell| cell.into()).collect();
                    RowValue::Struct(struct_values)
                })
                .unwrap_or(RowValue::Null)
            })
            .collect(),
    }
}

impl From<Cell> for RowValue {
    fn from(cell: Cell) -> Self {
        match cell {
            Cell::I16(value) => RowValue::Int32(value as i32),
            Cell::I32(value) => RowValue::Int32(value),
            Cell::U32(value) => RowValue::Int64(value as i64),
            Cell::I64(value) => RowValue::Int64(value),
            Cell::F32(value) => RowValue::Float32(value),
            Cell::F64(value) => RowValue::Float64(value),
            Cell::Bool(value) => RowValue::Bool(value),
            Cell::String(value) => RowValue::ByteArray(value.into_bytes()),
            Cell::Date(value) => {
                RowValue::Int32(value.signed_duration_since(ARROW_EPOCH).num_days() as i32)
            }
            Cell::Time(value) => {
                let seconds = value.num_seconds_from_midnight() as i64;
                let nanos = value.nanosecond() as i64;
                RowValue::Int64(seconds * 1_000_000 + nanos / 1_000)
            }
            Cell::TimeStamp(value) => RowValue::Int64(value.and_utc().timestamp_micros()),
            Cell::TimeStampTz(value) => RowValue::Int64(value.timestamp_micros()),
            Cell::Uuid(value) => RowValue::FixedLenByteArray(*value.as_bytes()),
            Cell::Json(value) => RowValue::ByteArray(serde_json::to_vec(&value).unwrap()),
            Cell::Bytes(value) => RowValue::ByteArray(value),
            Cell::Array(value) => RowValue::Array(convert_array_cell(value)),
            Cell::Composite(value) => {
                let struct_values: Vec<RowValue> =
                    value.into_iter().map(|cell| cell.into()).collect();
                RowValue::Struct(struct_values)
            }
            Cell::Numeric(value) => {
                match value {
                    PgNumeric::Value(bigdecimal) => {
                        let (int_val, _) = bigdecimal.into_bigint_and_exponent();
                        RowValue::Decimal(int_val.to_i128().unwrap())
                    }
                    _ => {
                        // DevNote:
                        // nan, inf, -inf will be converted to null
                        RowValue::Null
                    }
                }
            }
            Cell::Null => RowValue::Null,
        }
    }
}

impl From<PostgresTableRow> for MoonlinkRow {
    fn from(row: PostgresTableRow) -> Self {
        let values: Vec<RowValue> = row.0.values.into_iter().map(|cell| cell.into()).collect();
        MoonlinkRow::new(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_replicate::table::{ColumnSchema, LookupKey, TableName, TableSchema};
    use arrow::array::{Date32Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::DataType;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use iceberg::arrow as IcebergArrow;
    use moonlink::row::RowValue;
    use std::str::FromStr;

    #[test]
    fn test_table_schema_to_arrow_schema() {
        let table_schema = TableSchema {
            table_name: TableName {
                schema: "public".to_string(),
                name: "test_table".to_string(),
            },
            src_table_id: 1,
            column_schemas: vec![
                ColumnSchema {
                    name: "bool_field".to_string(),
                    typ: Type::BOOL,
                    modifier: 0,
                    nullable: false,
                },
                ColumnSchema {
                    name: "int2_field".to_string(),
                    typ: Type::INT2,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "int4_field".to_string(),
                    typ: Type::INT4,
                    modifier: 0,
                    nullable: false,
                },
                ColumnSchema {
                    name: "int8_field".to_string(),
                    typ: Type::INT8,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "float4_field".to_string(),
                    typ: Type::FLOAT4,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "float8_field".to_string(),
                    typ: Type::FLOAT8,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "numeric_field".to_string(),
                    typ: Type::NUMERIC,
                    modifier: ((12 << 16) | 5) + 4, // NUMERIC(12,5)
                    nullable: true,
                },
                ColumnSchema {
                    name: "varchar_field".to_string(),
                    typ: Type::VARCHAR,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "text_field".to_string(),
                    typ: Type::TEXT,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "bpchar_field".to_string(),
                    typ: Type::BPCHAR,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "char_field".to_string(),
                    typ: Type::CHAR,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "name_field".to_string(),
                    typ: Type::NAME,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "date_field".to_string(),
                    typ: Type::DATE,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "timestamp_field".to_string(),
                    typ: Type::TIMESTAMP,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "timestamptz_field".to_string(),
                    typ: Type::TIMESTAMPTZ,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "time_field".to_string(),
                    typ: Type::TIME,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "timetz_field".to_string(),
                    typ: Type::TIMETZ,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "uuid_field".to_string(),
                    typ: Type::UUID,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "json_field".to_string(),
                    typ: Type::JSON,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "jsonb_field".to_string(),
                    typ: Type::JSONB,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "bytea_field".to_string(),
                    typ: Type::BYTEA,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "oid_field".to_string(),
                    typ: Type::OID,
                    modifier: 0,
                    nullable: true,
                },
                // Array type.
                ColumnSchema {
                    name: "bool_array_field".to_string(),
                    typ: Type::BOOL_ARRAY,
                    modifier: 0,
                    nullable: true,
                },
                // PostgreSQL type: CREATE TYPE point AS (x int4, y int4);
                // Column type: point
                // Arrow type: Struct {x: Int32, y: Int32}
                ColumnSchema {
                    name: "point_field".to_string(),
                    typ: Type::new(
                        "point".to_string(),
                        0, // OID doesn't matter for this test
                        Kind::Composite(vec![
                            tokio_postgres::types::Field::new("x".to_string(), Type::INT4), // x coordinate
                            tokio_postgres::types::Field::new("y".to_string(), Type::INT4), // y coordinate
                        ]),
                        "public".to_string(),
                    ),
                    modifier: 0,
                    nullable: true,
                },
                // PostgreSQL type: CREATE TYPE point AS (x int4, y int4);
                // Column type: point[]
                // Arrow type: List<Struct {x: Int32, y: Int32}>
                ColumnSchema {
                    name: "point_array_field".to_string(),
                    typ: Type::new(
                        "point_array".to_string(),
                        0, // OID doesn't matter for this test
                        Kind::Array(Type::new(
                            "point".to_string(),
                            0, // OID doesn't matter for this test
                            Kind::Composite(vec![
                                tokio_postgres::types::Field::new("x".to_string(), Type::INT4), // x coordinate
                                tokio_postgres::types::Field::new("y".to_string(), Type::INT4), // y coordinate
                            ]),
                            "public".to_string(),
                        )),
                        "public".to_string(),
                    ),
                    modifier: 0,
                    nullable: true,
                },
                // CREATE TYPE point AS (x int4, y int4);
                // CREATE TYPE rectangle AS (top_left point);
                //
                // Column type: rectangle
                // Arrow type: Struct {
                //   top_left:  Struct { x: Int32, y: Int32 },
                // }
                ColumnSchema {
                    name: "rectangle_field".to_string(),
                    typ: Type::new(
                        "rectangle".to_string(),
                        0, // OID doesn't matter for this test
                        Kind::Composite(vec![tokio_postgres::types::Field::new(
                            "top_left".to_string(),
                            Type::new(
                                "point".to_string(),
                                0, // OID doesn't matter for this test
                                Kind::Composite(vec![
                                    tokio_postgres::types::Field::new("x".to_string(), Type::INT4), // x coordinate
                                    tokio_postgres::types::Field::new("y".to_string(), Type::INT4), // y coordinate
                                ]),
                                "public".to_string(),
                            ),
                        )]),
                        "public".to_string(),
                    ),
                    modifier: 0,
                    nullable: true,
                },
            ],
            lookup_key: LookupKey::Key {
                name: "uuid_field".to_string(),
                columns: vec!["uuid_field".to_string()],
            },
        };

        let (arrow_schema, identity) = postgres_schema_to_moonlink_schema(&table_schema);
        assert_eq!(arrow_schema.fields().len(), 26);

        assert_eq!(arrow_schema.field(0).name(), "bool_field");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Boolean);
        assert!(!arrow_schema.field(0).is_nullable());

        assert_eq!(arrow_schema.field(1).name(), "int2_field");
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Int16);
        assert!(arrow_schema.field(1).is_nullable());

        assert_eq!(arrow_schema.field(2).name(), "int4_field");
        assert_eq!(arrow_schema.field(2).data_type(), &DataType::Int32);
        assert!(!arrow_schema.field(2).is_nullable());

        assert_eq!(arrow_schema.field(3).name(), "int8_field");
        assert_eq!(arrow_schema.field(3).data_type(), &DataType::Int64);
        assert!(arrow_schema.field(3).is_nullable());

        assert_eq!(arrow_schema.field(4).name(), "float4_field");
        assert_eq!(arrow_schema.field(4).data_type(), &DataType::Float32);

        assert_eq!(arrow_schema.field(5).name(), "float8_field");
        assert_eq!(arrow_schema.field(5).data_type(), &DataType::Float64);

        assert_eq!(arrow_schema.field(6).name(), "numeric_field");
        assert_eq!(
            arrow_schema.field(6).data_type(),
            &DataType::Decimal128(12, 5)
        );

        assert_eq!(arrow_schema.field(7).name(), "varchar_field");
        assert_eq!(arrow_schema.field(7).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(8).name(), "text_field");
        assert_eq!(arrow_schema.field(8).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(9).name(), "bpchar_field");
        assert_eq!(arrow_schema.field(9).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(10).name(), "char_field");
        assert_eq!(arrow_schema.field(10).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(11).name(), "name_field");
        assert_eq!(arrow_schema.field(11).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(12).name(), "date_field");
        assert_eq!(arrow_schema.field(12).data_type(), &DataType::Date32);

        assert_eq!(arrow_schema.field(13).name(), "timestamp_field");
        assert!(matches!(
            arrow_schema.field(13).data_type(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        ));

        assert_eq!(arrow_schema.field(14).name(), "timestamptz_field");
        assert!(matches!(
            arrow_schema.field(14).data_type(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(_))
        ));

        assert_eq!(arrow_schema.field(15).name(), "time_field");
        assert_eq!(
            arrow_schema.field(15).data_type(),
            &DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
        );

        assert_eq!(arrow_schema.field(16).name(), "timetz_field");
        assert_eq!(
            arrow_schema.field(16).data_type(),
            &DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
        );

        assert_eq!(arrow_schema.field(17).name(), "uuid_field");
        assert_eq!(
            arrow_schema.field(17).data_type(),
            &DataType::FixedSizeBinary(16)
        );

        assert_eq!(arrow_schema.field(18).name(), "json_field");
        assert_eq!(arrow_schema.field(18).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(19).name(), "jsonb_field");
        assert_eq!(arrow_schema.field(19).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(20).name(), "bytea_field");
        assert_eq!(arrow_schema.field(20).data_type(), &DataType::Binary);

        assert_eq!(arrow_schema.field(21).name(), "oid_field");
        assert_eq!(arrow_schema.field(21).data_type(), &DataType::Int64);

        assert_eq!(arrow_schema.field(22).name(), "bool_array_field");
        let mut expected_field = Field::new("item", DataType::Boolean, /*nullable=*/ true);
        let mut field_metadata = HashMap::new();
        field_metadata.insert("PARQUET:field_id".to_string(), "22".to_string());
        expected_field.set_metadata(field_metadata);
        assert_eq!(
            arrow_schema.field(22).data_type(),
            &DataType::List(expected_field.into()),
        );

        assert_eq!(arrow_schema.field(23).name(), "point_field");
        assert!(matches!(
            arrow_schema.field(23).data_type(),
            DataType::Struct(_)
        ));

        assert_eq!(arrow_schema.field(24).name(), "point_array_field");
        assert!(matches!(
            arrow_schema.field(24).data_type(),
            DataType::List(_)
        ));

        assert_eq!(arrow_schema.field(25).name(), "rectangle_field");
        assert!(matches!(
            arrow_schema.field(25).data_type(),
            DataType::Struct(_)
        ));

        // Check identity property.
        assert_eq!(identity, IdentityProp::Keys(vec![17]));

        // Convert Arrow schema to Iceberg schema and check field id/name mapping.
        let iceberg_arrow = IcebergArrow::arrow_schema_to_schema(&arrow_schema).unwrap();
        for (field_id, expected_name) in [
            (0, "bool_field"),
            (1, "int2_field"),
            (2, "int4_field"),
            (3, "int8_field"),
            (4, "float4_field"),
            (5, "float8_field"),
            (6, "numeric_field"),
            (7, "varchar_field"),
            (8, "text_field"),
            (9, "bpchar_field"),
            (10, "char_field"),
            (11, "name_field"),
            (12, "date_field"),
            (13, "timestamp_field"),
            (14, "timestamptz_field"),
            (15, "time_field"),
            (16, "timetz_field"),
            (17, "uuid_field"),
            (18, "json_field"),
            (19, "jsonb_field"),
            (20, "bytea_field"),
            (21, "oid_field"),
            (22, "bool_array_field.element"),
            (23, "bool_array_field"),
            (24, "point_field.x"),
            (25, "point_field.y"),
            (26, "point_field"),
            (27, "point_array_field.element.x"),
            (28, "point_array_field.element.y"),
            (29, "point_array_field.element"),
            (30, "point_array_field"),
            (31, "rectangle_field.top_left.x"),
            (32, "rectangle_field.top_left.y"),
            (33, "rectangle_field.top_left"),
            (34, "rectangle_field"),
        ] {
            assert_eq!(
                iceberg_arrow.name_by_field_id(field_id).unwrap(),
                expected_name,
                "field id {} mismatch",
                field_id,
            );
        }
        assert!(iceberg_arrow.name_by_field_id(35).is_none());
    }

    #[test]
    fn test_postgres_table_row_to_moonlink_row() {
        let postgres_table_row = PostgresTableRow(TableRow {
            values: vec![
                Cell::I16(0),
                Cell::I32(1),
                Cell::U32(2),
                Cell::I64(3),
                Cell::F32(std::f32::consts::PI),
                Cell::F64(std::f64::consts::E),
                Cell::Bool(true),
                Cell::String("test".to_string()),
                Cell::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
                Cell::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
                Cell::TimeStamp(
                    NaiveDateTime::parse_from_str("2024-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")
                        .unwrap(),
                ),
                Cell::TimeStampTz(
                    DateTime::parse_from_rfc3339("2024-01-01T14:00:00+02:00")
                        .unwrap()
                        .with_timezone(&Utc),
                ),
                Cell::Uuid(uuid::Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap()),
                Cell::Json(serde_json::from_str(r#"{"name":"Alice"}"#).unwrap()),
                Cell::Null,
            ],
        });

        let moonlink_row: MoonlinkRow = postgres_table_row.into();
        assert_eq!(moonlink_row.values.len(), 15);
        assert_eq!(moonlink_row.values[0], RowValue::Int32(0));
        assert_eq!(moonlink_row.values[1], RowValue::Int32(1));
        assert_eq!(moonlink_row.values[2], RowValue::Int64(2));
        assert_eq!(moonlink_row.values[3], RowValue::Int64(3));
        assert_eq!(
            moonlink_row.values[4],
            RowValue::Float32(std::f32::consts::PI)
        );
        assert_eq!(
            moonlink_row.values[5],
            RowValue::Float64(std::f64::consts::E)
        );
        assert_eq!(moonlink_row.values[6], RowValue::Bool(true));
        let vec = "test".as_bytes().to_vec();
        assert_eq!(moonlink_row.values[7], RowValue::ByteArray(vec.clone()));
        let string = unsafe { std::str::from_utf8_unchecked(&vec) };
        let array = StringArray::from(vec![string]);
        assert_eq!(array.value(0), "test");
        assert_eq!(moonlink_row.values[8], RowValue::Int32(19723)); // 2024-01-01 days since epoch
        let array = Date32Array::from(vec![19723]);
        assert_eq!(
            array.value_as_date(0),
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(moonlink_row.values[9], RowValue::Int64(43200000000)); // 12:00:00 in microseconds
        assert_eq!(moonlink_row.values[10], RowValue::Int64(1704110400000000)); // 2024-01-01 12:00:00 in microseconds
        let array = TimestampMicrosecondArray::from(vec![1704110400000000]);
        assert_eq!(
            array.value_as_datetime(0),
            Some(
                NaiveDateTime::parse_from_str("2024-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        );
        assert_eq!(moonlink_row.values[11], RowValue::Int64(1704110400000000)); // 2024-01-01 12:00:00 UTC in microseconds
        let array = TimestampMicrosecondArray::from(vec![1704110400000000]);
        assert_eq!(
            array.value_as_datetime(0),
            Some(
                NaiveDateTime::parse_from_str("2024-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        );
        if let RowValue::FixedLenByteArray(bytes) = moonlink_row.values[12] {
            assert_eq!(
                uuid::Uuid::from_bytes(bytes).to_string(),
                "123e4567-e89b-12d3-a456-426614174000"
            );
        } else {
            panic!("Expected fixed length byte array");
        };
        let vec = r#"{"name":"Alice"}"#.to_string().as_bytes().to_vec();
        assert_eq!(moonlink_row.values[13], RowValue::ByteArray(vec));
        assert_eq!(moonlink_row.values[14], RowValue::Null);
    }

    #[test]
    fn test_postgres_numeric_row_to_moonlink_row() {
        let postgres_table_row = PostgresTableRow(TableRow {
            values: vec![
                Cell::Numeric(PgNumeric::NaN),
                Cell::Numeric(PgNumeric::NegativeInf),
                Cell::Numeric(PgNumeric::PositiveInf),
                Cell::Numeric(PgNumeric::Value(
                    bigdecimal::BigDecimal::from_str("12345.6789").unwrap(),
                )),
            ],
        });
        let moonlink_row: MoonlinkRow = postgres_table_row.into();
        assert_eq!(moonlink_row.values.len(), 4);
        assert_eq!(moonlink_row.values[0], RowValue::Null);
        assert_eq!(moonlink_row.values[1], RowValue::Null);
        assert_eq!(moonlink_row.values[2], RowValue::Null);
        assert_eq!(moonlink_row.values[3], RowValue::Decimal(123456789 as i128));
    }

    #[test]
    fn test_postgres_composite_to_moonlink_row() {
        let postgres_table_row = PostgresTableRow(TableRow {
            values: vec![
                Cell::I32(1),
                // Structure:
                // - Outer composite: {id: i32, tags: Array<String>, nested: NestedComposite}
                //   - Nested composite: {value: f32, scores: Array<i32>}
                Cell::Composite(vec![
                    Cell::I32(100),
                    Cell::Array(ArrayCell::String(vec![
                        Some("tag1".to_string()),
                        Some("tag2".to_string()),
                    ])),
                    Cell::Composite(vec![
                        Cell::F32(3.5),
                        Cell::Array(ArrayCell::I32(vec![Some(10), Some(20), None])),
                    ]),
                ]),
            ],
        });

        let moonlink_row: MoonlinkRow = postgres_table_row.into();
        assert_eq!(moonlink_row.values.len(), 2);
        assert_eq!(moonlink_row.values[0], RowValue::Int32(1));

        // Check the outer composite/struct field
        match &moonlink_row.values[1] {
            RowValue::Struct(outer_fields) => {
                assert_eq!(outer_fields.len(), 3);
                assert_eq!(outer_fields[0], RowValue::Int32(100));

                // Check array within struct
                match &outer_fields[1] {
                    RowValue::Array(tags) => {
                        assert_eq!(tags.len(), 2);
                        assert_eq!(tags[0], RowValue::ByteArray("tag1".as_bytes().to_vec()));
                        assert_eq!(tags[1], RowValue::ByteArray("tag2".as_bytes().to_vec()));
                    }
                    _ => panic!("Expected array in struct"),
                }

                // Check nested struct within struct
                match &outer_fields[2] {
                    RowValue::Struct(inner_fields) => {
                        assert_eq!(inner_fields.len(), 2);
                        assert_eq!(inner_fields[0], RowValue::Float32(3.5));

                        // Check array within nested struct
                        match &inner_fields[1] {
                            RowValue::Array(scores) => {
                                assert_eq!(scores.len(), 3);
                                assert_eq!(scores[0], RowValue::Int32(10));
                                assert_eq!(scores[1], RowValue::Int32(20));
                                assert_eq!(scores[2], RowValue::Null);
                            }
                            _ => panic!("Expected array in nested struct"),
                        }
                    }
                    _ => panic!("Expected nested struct"),
                }
            }
            _ => panic!("Expected struct"),
        }
    }

    #[test]
    fn test_postgres_array_of_composites_to_moonlink_row() {
        let postgres_table_row = PostgresTableRow(TableRow {
            values: vec![
                Cell::I32(1),
                // Structure:
                // - Array of UserComposite: [UserComposite, null, UserComposite]
                //   - UserComposite: {id: i32, name: String, active: bool}
                Cell::Array(ArrayCell::Composite(vec![
                    Some(vec![
                        Cell::I32(100),
                        Cell::String("alice".to_string()),
                        Cell::Bool(true),
                    ]),
                    None, // null composite
                    Some(vec![
                        Cell::I32(200),
                        Cell::String("bob".to_string()),
                        Cell::Bool(false),
                    ]),
                ])),
            ],
        });

        let moonlink_row: MoonlinkRow = postgres_table_row.into();
        assert_eq!(moonlink_row.values.len(), 2);
        assert_eq!(moonlink_row.values[0], RowValue::Int32(1));

        // Check the array of composites
        match &moonlink_row.values[1] {
            RowValue::Array(structs) => {
                assert_eq!(structs.len(), 3);

                // First struct
                match &structs[0] {
                    RowValue::Struct(fields) => {
                        assert_eq!(fields.len(), 3);
                        assert_eq!(fields[0], RowValue::Int32(100));
                        assert_eq!(fields[1], RowValue::ByteArray("alice".as_bytes().to_vec()));
                        assert_eq!(fields[2], RowValue::Bool(true));
                    }
                    _ => panic!("Expected struct in array"),
                }

                // Second is null
                assert_eq!(structs[1], RowValue::Null);

                // Third struct
                match &structs[2] {
                    RowValue::Struct(fields) => {
                        assert_eq!(fields.len(), 3);
                        assert_eq!(fields[0], RowValue::Int32(200));
                        assert_eq!(fields[1], RowValue::ByteArray("bob".as_bytes().to_vec()));
                        assert_eq!(fields[2], RowValue::Bool(false));
                    }
                    _ => panic!("Expected struct in array"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_postgres_array_to_moonlink_row() {
        let postgres_table_row = PostgresTableRow(TableRow {
            values: vec![
                // Array of integers
                Cell::Array(ArrayCell::I32(vec![Some(1), Some(2), Some(3)])),
                // Array of strings
                Cell::Array(ArrayCell::String(vec![
                    Some("hello".to_string()),
                    Some("world".to_string()),
                ])),
                // Array with null values
                Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)])),
                // Empty array
                Cell::Array(ArrayCell::I32(vec![])),
                // Array of booleans
                Cell::Array(ArrayCell::Bool(vec![Some(true), Some(false)])),
                // Array of timestamps
                Cell::Array(ArrayCell::TimeStamp(vec![
                    Some(
                        NaiveDateTime::parse_from_str("2024-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")
                            .unwrap(),
                    ),
                    Some(
                        NaiveDateTime::parse_from_str("2024-01-02 12:00:00", "%Y-%m-%d %H:%M:%S")
                            .unwrap(),
                    ),
                ])),
            ],
        });

        let moonlink_row: MoonlinkRow = postgres_table_row.into();
        assert_eq!(moonlink_row.values.len(), 6);

        // Test array of integers
        let int_array = match &moonlink_row.values[0] {
            RowValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(int_array.len(), 3);
        assert_eq!(int_array[0], RowValue::Int32(1));
        assert_eq!(int_array[1], RowValue::Int32(2));
        assert_eq!(int_array[2], RowValue::Int32(3));

        // Test array of strings
        let str_array = match &moonlink_row.values[1] {
            RowValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(str_array.len(), 2);
        assert_eq!(
            str_array[0],
            RowValue::ByteArray("hello".as_bytes().to_vec())
        );
        assert_eq!(
            str_array[1],
            RowValue::ByteArray("world".as_bytes().to_vec())
        );

        // Test array with null values
        let null_array = match &moonlink_row.values[2] {
            RowValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(null_array.len(), 3);
        assert_eq!(null_array[0], RowValue::Int32(1));
        assert_eq!(null_array[1], RowValue::Null);
        assert_eq!(null_array[2], RowValue::Int32(3));

        // Test empty array
        let empty_array = match &moonlink_row.values[3] {
            RowValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(empty_array.len(), 0);

        // Test array of booleans
        let bool_array = match &moonlink_row.values[4] {
            RowValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(bool_array.len(), 2);
        assert_eq!(bool_array[0], RowValue::Bool(true));
        assert_eq!(bool_array[1], RowValue::Bool(false));

        // Test array of timestamps
        let timestamp_array = match &moonlink_row.values[5] {
            RowValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(timestamp_array.len(), 2);
        assert_eq!(
            timestamp_array[0],
            RowValue::Int64(1704110400000000) // 2024-01-01 12:00:00 in microseconds
        );
        assert_eq!(
            timestamp_array[1],
            RowValue::Int64(1704196800000000) // 2024-01-02 12:00:00 in microseconds
        );
    }
}
