use std::fmt::Display;

use pg_escape::quote_identifier;
use tokio_postgres::types::Type;

#[derive(Debug, Clone)]
pub struct TableName {
    pub schema: String,
    pub name: String,
}

impl TableName {
    pub fn parse_schema_name(table_name: &str) -> (String, String) {
        let tokens: Vec<&str> = table_name.split('.').collect();
        assert_eq!(tokens.len(), 2);
        let schema = Self::unquote_identifier(tokens[0]);
        let name = Self::unquote_identifier(tokens[1]);
        (schema, name)
    }

    fn unquote_identifier(raw: &str) -> String {
        if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
            // strip outer quotes
            let inner = &raw[1..raw.len() - 1];
            // collapse double quotes
            inner.replace("\"\"", "\"")
        } else {
            // unquoted identifiers are case-insensitive
            raw.to_ascii_lowercase()
        }
    }
    pub fn get_schema_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
    pub fn as_quoted_identifier(&self) -> String {
        let quoted_schema = quote_identifier(&self.schema);
        let quoted_name = quote_identifier(&self.name);
        format!("{quoted_schema}.{quoted_name}")
    }
}

impl Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{0}.{1}", self.schema, self.name))
    }
}

type TypeModifier = i32;

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub typ: Type,
    pub modifier: TypeModifier,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub enum LookupKey {
    Key { name: String, columns: Vec<String> },
    FullRow,
}

pub type TableId = u32;

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub table_name: TableName,
    pub table_id: TableId,
    pub column_schemas: Vec<ColumnSchema>,
    pub lookup_key: LookupKey,
}

impl TableSchema {}
