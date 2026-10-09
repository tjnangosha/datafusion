use arrow::array::{ArrayRef, RecordBatch, StringArray, Int32Array};
use datafusion::optimizer::{ ApplyOrder, OptimizerRule, OptimizerConfig };
use datafusion::common::tree_node::{ Transformed };
use datafusion::logical_expr::{ LogicalPlan };
use datafusion::prelude::SessionContext;
use datafusion::common::{ Result, not_impl_err };
use std::sync::Arc;

pub async fn optimizer_rule_reject_joins() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.add_optimizer_rule(Arc::new(OptimizerRuleRejectJoins {}));

    ctx.register_batch("customers", customers_batch())?;
    ctx.register_batch("orders", orders_batch())?;

    let sql1 = "SELECT id, c.name, amount_paid from customers c JOIN orders o on c.name = o.name";
    let _sql2 = "SELECT DISTINCT c.name FROM customers c LEFT JOIN orders o ON c.name = o.name";
    let plan = ctx.sql(sql1).await?.into_optimized_plan()?;

    println!("[OptimizerRuleRejectJoins] Logical Plan:\n\n{}\n", plan.display_indent());

    Ok(())
}

#[derive(Default, Debug)]

struct OptimizerRuleRejectJoins {}   

impl OptimizerRule for OptimizerRuleRejectJoins {
    fn name(&self) -> &str {
        "optimizer_rule_reject_joins"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

     fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match &plan {
            LogicalPlan::Join(_) | LogicalPlan::AsOfJoin(_) => {
                not_impl_err!("JOIN operations are disabled")
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

}

fn customers_batch() -> RecordBatch {
    let id: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
    let name: ArrayRef =  Arc::new(StringArray::from_iter_values(["Nangosha", "Malaika", "Apio", "Kakyo", "Letaru"]));
    RecordBatch::try_from_iter(vec![("id", id), ("name", name)]).unwrap()
}

fn orders_batch() -> RecordBatch {
    let amount_paid: ArrayRef = Arc::new(Int32Array::from(vec![1000, 2000, 3000, 4000, 5000]));
    let name: ArrayRef =  Arc::new(StringArray::from_iter_values(["Nangosha", "Malaika", "Apio", "Kakyo", "Letaru"]));
    RecordBatch::try_from_iter(vec![("amount_paid", amount_paid), ("name", name)]).unwrap()
}
