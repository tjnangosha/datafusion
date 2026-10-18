use arrow::datatypes::UInt64Type;
use datafusion::arrow::{
    array::ArrayRef, 
    array::AsArray,
    array::Array,
    array::UInt64Array, 
    array::StringArray,
    datatypes::DataType, 
    record_batch::RecordBatch, 
};
use datafusion::{error::Result, physical_plan::Accumulator};
use datafusion::{logical_expr::Volatility, prelude::*, scalar::ScalarValue};
use std::sync::Arc;
use std::collections::HashMap;
use std::hash::Hash;
use datafusion::arrow::datatypes::Field;

fn create_context() -> Result<SessionContext> {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::MemTable;

    // define a schema.
    let field_age = Field::new("age", DataType::UInt64, false);
    let field_gender = Field::new("gender", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field_age, field_gender]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![20, 20, 80, 60, 40, 80])),
            Arc::new(StringArray::from(vec!["Male", "Male", "Female", "Female", "Male", "Female"])),
        ],
    )?;

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![30, 30, 80, 60, 30, 80])),
            Arc::new(StringArray::from(vec!["Male", "Male", "Female", "Female", "Male", "Female"])),
        ],
    )?;

    let ctx = SessionContext::new();

    let provider = MemTable::try_new(schema, vec![vec![batch1, batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;
    Ok(ctx)
}

/// A UDAF has state across multiple rows, and thus we require a `struct` with that state.
#[derive(Debug)]
struct Mode <T> {
    value_counts: HashMap<T, u64>,
    modal_value: Option<T>,
    modal_value_freq: u64,
}

impl<T> Default for Mode<T> {
    fn default() -> Self {
        Mode {
            value_counts: HashMap::new(),
            modal_value: None,
            modal_value_freq: 0,
        }
    }
}

impl<T: Eq + Hash + Clone + Ord> Mode<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, value: T, n: u64) {
        if n == 0 {
            return;
        }
        let count = self.value_counts.entry(value.clone()).or_insert(0);
        *count += n;
        let count = *count;

        let better = match &self.modal_value {
            None => true,
            Some(cur) => {
                count > self.modal_value_freq
                    || (count == self.modal_value_freq && value < *cur)
            }
        };
        if better {
            self.modal_value_freq = count;
            self.modal_value = Some(value);
        }
    }
}

impl Accumulator for Mode<u64> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<ScalarValue> = self.value_counts.keys()
            .map(|v| ScalarValue::from(*v))
            .collect();
        let counts: Vec<ScalarValue> = self.value_counts.values()
            .map(|c| ScalarValue::from(*c))
            .collect();

        Ok(vec![
            ScalarValue::List(ScalarValue::new_list_nullable(&values, &DataType::UInt64)),
            ScalarValue::List(ScalarValue::new_list_nullable(&counts, &DataType::UInt64)),
        ])
    }

    // DataFusion expects this function to return the final value of this aggregator.
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::from(self.modal_value))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = &values[0];
        (0..arr.len()).try_for_each(|index| {
            let v = ScalarValue::try_from_array(arr, index)?;
            if let ScalarValue::UInt64(Some(value)) = v {
                self.update(value, 1);
            }

            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let values_col = states[0].as_list::<i32>();
        let counts_col = states[1].as_list::<i32>();

        for row in 0..values_col.len() {
            if values_col.is_null(row) {
                continue;
            }
            let values = values_col.value(row);
            let counts = counts_col.value(row);
            let values = values.as_primitive::<UInt64Type>();
            let counts = counts.as_primitive::<UInt64Type>();

            for i in 0..values.len() {
                self.update(values.value(i), counts.value(i))
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

pub async fn simple_udaf_mode() -> Result<()> {
    let list_u64 = DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true)));

    let ctx = create_context()?;

    let mode = create_udaf(
        "mode",
        vec![DataType::UInt64],
        Arc::new(DataType::UInt64),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(Mode::new()))),
        Arc::new(vec![list_u64.clone(), list_u64]),
    );
    ctx.register_udaf(mode.clone());

    let sql_df = ctx.sql("SELECT gender, mode(age) FROM t group by gender").await?;
    sql_df.show().await?;

    Ok(())
}
