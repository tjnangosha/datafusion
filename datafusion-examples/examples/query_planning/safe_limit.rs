use arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::SessionContext;
use std::sync::{Arc};

pub async fn safe_limit() -> Result<()> {
    let rule = Arc::new(SafeLimit { max_rows: 10 });
    let ctx = SessionContext::new();
    ctx.add_analyzer_rule(Arc::clone(&rule) as _);

    ctx.register_batch("employee", employee_batch())?;

    let plan = ctx.sql("EXPLAIN SELECT * FROM employee").await?.into_optimized_plan()?;

    // Printing the query plan shows a filter has been added
    println!("[SafeLimit] Logical Plan:\n\n{}\n", plan.display_indent());

    Ok(())
}

/// Example AnalyzerRule that will always return atmost 50 rows
/// whether the query specifies it or not
#[derive(Debug)]
pub struct SafeLimit {
    pub max_rows: usize,
}

impl AnalyzerRule for SafeLimit {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        if matches!(plan, LogicalPlan::Dml(_) | LogicalPlan::Copy(_) | LogicalPlan::Analyze(_)) {
            return Ok(plan);
        }

        LogicalPlanBuilder::from(plan)
            .limit(0, Some(self.max_rows))?
            .build()
    }

    fn name(&self) -> &str {
        "safe_limit"
    }
}

/// Return a RecordBatch with made up data about fictional employees
fn employee_batch() -> RecordBatch {
    const LAST_NAMES: [&str; 20] = [
        "Akello", "Okello", "Nakato", "Mugisha", "Ssempala", "Namutebi", "Okot",
        "Kyomuhendo", "Tumusiime", "Nsubuga", "Atim", "Byaruhanga", "Wasswa", "Babirye",
        "Mukasa", "Musoke", "Lubega", "Opio", "Atuhaire", "Kiggundu",
    ];
    const N: usize = 100;

    let names = (0..N).map(|i| LAST_NAMES[(i * 7) % LAST_NAMES.len()]);
    let name: ArrayRef = Arc::new(StringArray::from_iter_values(names));

    RecordBatch::try_from_iter(vec![("name", name)]).unwrap()
}
