use crate::SpanRecord;

/// How many origin columns the span table has.
pub(crate) const ORIGIN_COLUMNS: usize = 7;

/// Positions of the origin columns in [`Origin`] order. The first three come
/// from a `ResourceSpans`, the last four from a `ScopeSpans`.
pub(crate) mod slot {
    pub const RESOURCE: usize = 0;
    pub const SERVICE_NAME: usize = 1;
    pub const RESOURCE_SCHEMA_URL: usize = 2;
    pub const SCOPE: usize = 3;
    pub const SCOPE_NAME: usize = 4;
    pub const SCOPE_VERSION: usize = 5;
    pub const SCOPE_SCHEMA_URL: usize = 6;
    /// Slots from here on belong to the scope level.
    pub const FIRST_SCOPE_LEVEL: usize = SCOPE;
}

/// The seven values a span inherits from the `ResourceSpans` and `ScopeSpans`
/// that hold it: the columns `resource`, `service_name`,
/// `resource_schema_url`, `scope`, `scope_name`, `scope_version` and
/// `scope_schema_url`. Every field may be absent.
///
/// The two levels may be interned separately, by filling only one level, and
/// the keys joined with [`OriginKeys::combine`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Origin<'a> {
    /// `resource` column: serialized OTLP `Resource`, as received.
    pub resource: Option<&'a [u8]>,
    /// `service_name` column.
    pub service_name: Option<&'a str>,
    /// `resource_schema_url` column.
    pub resource_schema_url: Option<&'a str>,
    /// `scope` column: serialized OTLP `InstrumentationScope`, as received.
    pub scope: Option<&'a [u8]>,
    /// `scope_name` column.
    pub scope_name: Option<&'a str>,
    /// `scope_version` column.
    pub scope_version: Option<&'a str>,
    /// `scope_schema_url` column.
    pub scope_schema_url: Option<&'a str>,
}

impl<'a> Origin<'a> {
    /// The origin fields of a record.
    pub fn of(r: &SpanRecord<'a>) -> Self {
        Self {
            resource: r.resource,
            service_name: r.service_name,
            resource_schema_url: r.resource_schema_url,
            scope: r.scope,
            scope_name: r.scope_name,
            scope_version: r.scope_version,
            scope_schema_url: r.scope_schema_url,
        }
    }
}

/// The dictionary keys of one [`Origin`] in the batch being built, from
/// [`SpanBatchBuilder::intern_origin`](crate::SpanBatchBuilder::intern_origin).
///
/// Keys are valid until the builder's next `finish`. Using them afterwards,
/// or with another builder, panics instead of writing wrong data: each key set
/// carries the number of the batch it was made for, and batch numbers are
/// never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OriginKeys {
    pub(crate) batch: u64,
    pub(crate) keys: [Option<u32>; ORIGIN_COLUMNS],
}

impl OriginKeys {
    /// Joins the resource-level keys of `resource_level` (resource, service
    /// name, resource schema URL) with the scope-level keys of `scope_level`
    /// (scope, scope name, scope version, scope schema URL). Both must be for
    /// the same batch of the same builder.
    ///
    /// # Panics
    ///
    /// Panics if the two key sets are for different batches.
    pub fn combine(resource_level: &OriginKeys, scope_level: &OriginKeys) -> OriginKeys {
        assert_eq!(
            resource_level.batch, scope_level.batch,
            "origin keys are for different batches"
        );
        let mut keys = resource_level.keys;
        keys[slot::FIRST_SCOPE_LEVEL..]
            .copy_from_slice(&scope_level.keys[slot::FIRST_SCOPE_LEVEL..]);
        OriginKeys {
            batch: resource_level.batch,
            keys,
        }
    }
}
