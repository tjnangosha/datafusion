use arrow::array::{ArrayRef, RecordBatch, StringArray, Int32Array};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::{ ApplyOrder, OptimizerRule, OptimizerConfig };
use datafusion::common::tree_node::{ Transformed };
use datafusion::logical_expr::{ LogicalPlan };
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{
    AsOfJoinExec, CrossJoinExec, HashJoinExec, NestedLoopJoinExec,
    PiecewiseMergeJoinExec, SortMergeJoinExec, SymmetricHashJoinExec,
};
use datafusion::prelude::SessionContext;
use datafusion::common::{ Result, not_impl_err };
use std::sync::Arc;
use datafusion::physical_plan::ExecutionPlan;

pub async fn optimizer_rule_reject_joins() -> Result<()> {
    let state  = SessionStateBuilder::new_with_default_features()
        .with_physical_optimizer_rule(Arc::new(PhysicalOptimizerRuleRejectJoins {}))
        .build();

    let ctx = SessionContext::new_with_state(state);
    ctx.add_optimizer_rule(Arc::new(OptimizerRuleRejectJoins {}));

    ctx.register_batch("customers", customers_batch())?;
    ctx.register_batch("orders", orders_batch())?;
    
    // TODO; @tjnangosha - Test out against the dataframe API as well

    // Choosing to "reject" joins has this effect of rejecting query statements that
    // don't contain the JOIN keyword themselves. This is because internally, the optimiser
    // will rewrite some statements into JOINs. And for some of these statements 
    // there is no other way to execute them without using joins!
    // Examples of such cases include the following - e.g `SELECT * from a,b`, 
    // `IN (SELECT ...)`, `EXISTS (...)` and correlated scalar subqueries which are rewritten into semi / left  
    // joins by the optimizer's subquery decorrelation rules
    //.
    // On the other hand, some statements like `SELECT DISTINCT c.name FROM customers c LEFT JOIN orders o ON c.name = o.name`
    //  below will contain the JOIN keyword but will not be rejected by our rule
    // Since inbuilt rules are applied before custom rules, the `EliminateJoin` rule.
    // in datafusion/optimizer/src/eliminate_join.rs will optimise away the JOIN and by the time
    // our custom rule is applied, there is no JOIN to reject.
    let sql1 = "SELECT id, c.name, amount_paid from customers c JOIN orders o on c.name = o.name";
    let sql2 = "SELECT DISTINCT c.name FROM customers c LEFT JOIN orders o ON c.name = o.name";
    let sql3 = "SELECT * FROM customers, orders";
    let sql4 = "SELECT * FROM customers, orders o WHERE o.name IN (SELECT name FROM orders)";
    let sql5 = "SELECT * FROM customers c, orders WHERE EXISTS (SELECT 1 FROM orders o WHERE o.name = c.name)";
    let sql6 = "SELECT name FROM customers INTERSECT SELECT name FROM orders";
    ctx.sql(sql1).await?.create_physical_plan().await?;

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
        println!("[OptimizerRuleRejectJoins] Logical Plan:\n\n{}\n", plan.display_indent());
        match &plan {
            LogicalPlan::Join(_) | LogicalPlan::AsOfJoin(_) => {
                not_impl_err!("JOIN operations are disabled")
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

}

#[derive(Default, Debug)]
pub struct  PhysicalOptimizerRuleRejectJoins {}

impl PhysicalOptimizerRule for PhysicalOptimizerRuleRejectJoins {
    fn name(&self) -> &str {
        "physical_optimizer_rule_reject_joins"
    }
    
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &datafusion::config::ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
       // read-only walk: `apply` visits every node, root included
        plan.apply(|node| {
            if is_join(node.as_ref()) {
                return not_impl_err!("JOIN operations are disabled (found {})", node.name());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(plan) // unchanged
    }
    
    fn schema_check(&self) -> bool {
        true
    }
}

fn is_join(node: &dyn ExecutionPlan) -> bool {
    node.is::<HashJoinExec>()
        || node.is::<SortMergeJoinExec>()
        || node.is::<NestedLoopJoinExec>()
        || node.is::<CrossJoinExec>()
        || node.is::<SymmetricHashJoinExec>()
        || node.is::<PiecewiseMergeJoinExec>()
        || node.is::<AsOfJoinExec>()
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
