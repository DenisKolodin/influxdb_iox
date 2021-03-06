use std::collections::BTreeSet;

use arrow_deps::arrow::{self, array::Array};
use either::Either;

use super::cmp;
use super::encoding::dictionary::{Encoding, Plain, RLE};
use crate::column::{RowIDs, Value, Values};

// Edd's totally made up magic constant. This determines whether we would use
// a run-length encoded dictionary encoding or just a plain dictionary encoding.
// I have ideas about how to build heuristics to do this in a much better way
// than an arbitrary constant but for now it's this...
//
// FWIW it's not the cardinality of the column that should drive the decision
// it's how many run-lengths would be produced in an RLE column and whether that
// compression is worth the memory and compute costs to work on it.
pub const TEMP_CARDINALITY_DICTIONARY_ENCODING_LIMIT: usize = 100_000;

pub enum StringEncoding {
    RLEDictionary(RLE),
    Dictionary(Plain),
    // TODO - simple array encoding, e.g., via Arrow String array.
}

/// This implementation is concerned with how to produce string columns with
/// different encodings.
impl StringEncoding {
    /// The total size in bytes of the store columnar data.
    pub fn size(&self) -> u64 {
        match self {
            Self::RLEDictionary(enc) => enc.size(),
            Self::Dictionary(enc) => enc.size(),
        }
    }

    /// The total number of rows in the column.
    pub fn num_rows(&self) -> u32 {
        match self {
            Self::RLEDictionary(enc) => enc.num_rows(),
            Self::Dictionary(enc) => enc.num_rows(),
        }
    }

    /// The lexicographical min and max values in the column.
    pub fn column_range(&self) -> Option<(String, String)> {
        match self {
            Self::RLEDictionary(enc) => match (enc.column_min(), enc.column_max()) {
                (None, None) => None,
                (Some(min), Some(max)) => Some((min.to_owned(), max.to_owned())),
                (min, max) => panic!("invalid column range: ({:?}, {:?})", min, max),
            },
            Self::Dictionary(enc) => match (enc.column_min(), enc.column_max()) {
                (None, None) => None,
                (Some(min), Some(max)) => Some((min.to_owned(), max.to_owned())),
                (min, max) => panic!("invalid column range: ({:?}, {:?})", min, max),
            },
        }
    }

    /// Determines if the column contains a NULL value.
    pub fn contains_null(&self) -> bool {
        match self {
            Self::RLEDictionary(enc) => enc.contains_null(),
            Self::Dictionary(enc) => enc.contains_null(),
        }
    }

    /// Returns true if encoding can return row ID sets for logical values.
    pub fn has_pre_computed_row_id_sets(&self) -> bool {
        match &self {
            Self::RLEDictionary(_) => true,
            Self::Dictionary(_) => false,
        }
    }

    /// Determines if the column contains a non-null value
    pub fn has_any_non_null_value(&self) -> bool {
        match &self {
            Self::RLEDictionary(c) => c.has_any_non_null_value(),
            Self::Dictionary(c) => c.has_any_non_null_value(),
        }
    }

    /// Determines if the column contains a non-null value at one of the
    /// provided rows.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match &self {
            Self::RLEDictionary(c) => c.has_non_null_value(row_ids),
            Self::Dictionary(c) => c.has_non_null_value(row_ids),
        }
    }

    /// Determines if the column contains any values other than those provided.
    /// Short-circuits execution as soon as it finds a value not in `values`.
    pub fn has_other_non_null_values(&self, values: &BTreeSet<String>) -> bool {
        match &self {
            Self::RLEDictionary(c) => c.has_other_non_null_values(values),
            Self::Dictionary(c) => c.has_other_non_null_values(values),
        }
    }

    /// Returns the logical value found at the provided row id.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match &self {
            Self::RLEDictionary(c) => match c.value(row_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
            Self::Dictionary(c) => match c.value(row_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
        }
    }

    /// All values present at the provided logical row ids.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn values(&self, row_ids: &[u32]) -> Values<'_> {
        match &self {
            Self::RLEDictionary(c) => Values::String(c.values(row_ids, vec![])),
            Self::Dictionary(c) => Values::String(c.values(row_ids, vec![])),
        }
    }

    /// All values in the column.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn all_values(&self) -> Values<'_> {
        match &self {
            Self::RLEDictionary(c) => Values::String(c.all_values(vec![])),
            Self::Dictionary(c) => Values::String(c.all_values(vec![])),
        }
    }

    /// Returns the logical value for the specified encoded representation.
    pub fn decode_id(&self, encoded_id: u32) -> Value<'_> {
        match &self {
            Self::RLEDictionary(c) => match c.decode_id(encoded_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
            Self::Dictionary(c) => match c.decode_id(encoded_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
        }
    }

    /// Returns the distinct set of values found at the provided row ids.
    ///
    /// TODO(edd): perf - pooling of destination sets.
    pub fn distinct_values(&self, row_ids: impl Iterator<Item = u32>) -> BTreeSet<Option<&'_ str>> {
        match &self {
            Self::RLEDictionary(c) => c.distinct_values(row_ids, BTreeSet::new()),
            Self::Dictionary(c) => c.distinct_values(row_ids, BTreeSet::new()),
        }
    }

    /// Returns the row ids that satisfy the provided predicate.
    pub fn row_ids_filter(&self, op: &cmp::Operator, value: &str, dst: RowIDs) -> RowIDs {
        match &self {
            Self::RLEDictionary(c) => c.row_ids_filter(value, op, dst),
            Self::Dictionary(c) => c.row_ids_filter(value, op, dst),
        }
    }

    /// The lexicographic minimum non-null value at the rows specified, or the
    /// NULL value if the column only contains NULL values at the provided row
    /// ids.
    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::RLEDictionary(c) => match c.min(row_ids) {
                Some(min) => Value::String(min),
                None => Value::Null,
            },
            Self::Dictionary(c) => match c.min(row_ids) {
                Some(min) => Value::String(min),
                None => Value::Null,
            },
        }
    }

    /// The lexicographic maximum non-null value at the rows specified, or the
    /// NULL value if the column only contains NULL values at the provided row
    /// ids.
    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::RLEDictionary(c) => match c.max(row_ids) {
                Some(max) => Value::String(max),
                None => Value::Null,
            },
            Self::Dictionary(c) => match c.max(row_ids) {
                Some(max) => Value::String(max),
                None => Value::Null,
            },
        }
    }

    /// The number of non-null values at the provided row ids.
    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            Self::RLEDictionary(c) => c.count(row_ids),
            Self::Dictionary(c) => c.count(row_ids),
        }
    }

    /// Calculate all row ids for each distinct value in the column.
    pub fn group_row_ids(&self) -> Either<Vec<&RowIDs>, Vec<RowIDs>> {
        match self {
            Self::RLEDictionary(enc) => Either::Left(enc.group_row_ids()),
            Self::Dictionary(enc) => Either::Right(enc.group_row_ids()),
        }
    }

    /// All encoded values for the provided logical row ids.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn encoded_values(&self, row_ids: &[u32], dst: Vec<u32>) -> Vec<u32> {
        match &self {
            Self::RLEDictionary(c) => c.encoded_values(row_ids, dst),
            Self::Dictionary(c) => c.encoded_values(row_ids, dst),
        }
    }

    /// All encoded values for the column.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn all_encoded_values(&self, dst: Vec<u32>) -> Vec<u32> {
        match &self {
            Self::RLEDictionary(c) => c.all_encoded_values(dst),
            Self::Dictionary(c) => c.all_encoded_values(dst),
        }
    }
}

impl std::fmt::Display for StringEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RLEDictionary(data) => write!(f, "{}", data),
            Self::Dictionary(data) => write!(f, "{}", data),
        }
    }
}

// Converts an Arrow `StringArray` into a `StringEncoding`.
//
// Note: this currently runs through the array and builds the dictionary before
// creating the encoding. There is room for performance improvement here but
// ideally it's a "write once read many" scenario.
impl From<arrow::array::StringArray> for StringEncoding {
    fn from(arr: arrow::array::StringArray) -> Self {
        // build a sorted dictionary.
        let mut dictionary = BTreeSet::new();

        for i in 0..arr.len() {
            if !arr.is_null(i) {
                dictionary.insert(arr.value(i).to_owned());
            }
        }

        let mut data: Encoding = if dictionary.len() > TEMP_CARDINALITY_DICTIONARY_ENCODING_LIMIT {
            Encoding::Plain(Plain::with_dictionary(dictionary))
        } else {
            Encoding::RLE(RLE::with_dictionary(dictionary))
        };

        let mut prev = if !arr.is_null(0) {
            Some(arr.value(0))
        } else {
            None
        };

        let mut count = 1;
        for i in 1..arr.len() {
            let next = if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            };

            if prev == next {
                count += 1;
                continue;
            }

            match prev {
                Some(x) => data.push_additional(Some(x.to_string()), count),
                None => data.push_additional(None, count),
            }
            prev = next;
            count = 1;
        }

        // Add final batch to column if any
        match prev {
            Some(x) => data.push_additional(Some(x.to_string()), count),
            None => data.push_additional(None, count),
        };

        match data {
            Encoding::RLE(enc) => Self::RLEDictionary(enc),
            Encoding::Plain(enc) => Self::Dictionary(enc),
        }
    }
}

impl From<&[Option<&str>]> for StringEncoding {
    fn from(arr: &[Option<&str>]) -> Self {
        // build a sorted dictionary.
        let mut dictionary = BTreeSet::new();

        for v in arr {
            if let Some(x) = v {
                dictionary.insert(x.to_string());
            }
        }

        let mut data: Encoding = if dictionary.len() > TEMP_CARDINALITY_DICTIONARY_ENCODING_LIMIT {
            Encoding::Plain(Plain::with_dictionary(dictionary))
        } else {
            Encoding::RLE(RLE::with_dictionary(dictionary))
        };

        let mut prev = &arr[0];

        let mut count = 1;
        for next in arr[1..].iter() {
            if prev == next {
                count += 1;
                continue;
            }

            match prev {
                Some(x) => data.push_additional(Some(x.to_string()), count),
                None => data.push_additional(None, count),
            }
            prev = next;
            count = 1;
        }

        // Add final batch to column if any
        match prev {
            Some(x) => data.push_additional(Some(x.to_string()), count),
            None => data.push_additional(None, count),
        };

        match data {
            Encoding::RLE(enc) => Self::RLEDictionary(enc),
            Encoding::Plain(enc) => Self::Dictionary(enc),
        }
    }
}

impl From<&[&str]> for StringEncoding {
    fn from(arr: &[&str]) -> Self {
        // build a sorted dictionary.
        let dictionary = arr.iter().map(|x| x.to_string()).collect::<BTreeSet<_>>();

        let mut data: Encoding = if dictionary.len() > TEMP_CARDINALITY_DICTIONARY_ENCODING_LIMIT {
            Encoding::Plain(Plain::with_dictionary(dictionary))
        } else {
            Encoding::RLE(RLE::with_dictionary(dictionary))
        };

        let mut prev = &arr[0];
        let mut count = 1;
        for next in arr[1..].iter() {
            if prev == next {
                count += 1;
                continue;
            }

            data.push_additional(Some(prev.to_string()), count);
            prev = next;
            count = 1;
        }

        // Add final batch to column if any
        data.push_additional(Some(prev.to_string()), count);

        match data {
            Encoding::RLE(enc) => Self::RLEDictionary(enc),
            Encoding::Plain(enc) => Self::Dictionary(enc),
        }
    }
}
