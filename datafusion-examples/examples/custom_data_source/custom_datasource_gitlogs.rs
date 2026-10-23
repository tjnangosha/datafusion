use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::exec_datafusion_err;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream, project_schema,
};
use datafusion::prelude::*;
use arrow::array::{ArrayRef, Int32Builder, StringBuilder, TimestampSecondBuilder};
use datafusion::common::{ DataFusionError};

use datafusion::catalog::Session;
use gix::bstr::ByteSlice;

fn git<E: fmt::Display>(e: E) -> DataFusionError {
    exec_datafusion_err!("git: {e}")
}

pub async fn custom_datasource_gitlogs() -> Result<()> {
    let db = GitLogsDataSource {
        repo_path: String::from(""), // TODO; add a path here
    };

    let ctx = SessionContext::new();
    ctx.register_table("commits", Arc::new(db))?;
    let sql = ctx
        .sql("SELECT author_name, hash, author_time FROM commits LIMIT 10")
        .await?;
    sql.show().await?;

    Ok(())
}

/// A custom datasource, used to represent a datastore with a single index
#[derive(Clone)]
pub struct GitLogsDataSource {
    repo_path: String
}

impl GitLogsDataSource {
    pub(crate) fn create_physical_plan(
        &self,
        projections: Option<&[usize]>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExec::new(projections, schema, self.clone())))
    }
}

impl Debug for GitLogsDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("GitLogsDataSource").finish()
    }
}

#[async_trait]
impl TableProvider for GitLogsDataSource {
    fn schema(&self) -> SchemaRef {
        SchemaRef::new(
            Schema::new(vec![
                Field::new("hash", DataType::Utf8, false),
                Field::new("author_name", DataType::Utf8, false),
                Field::new("author_email", DataType::Utf8, false),
                Field::new(
                    "author_time",
                    DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                    false,
                ),
                Field::new("author_tz_offset_secs", DataType::Int32, false),
                Field::new("title", DataType::Utf8, false),
                Field::new("body", DataType::Utf8, true), // nullable: one-line commits have no body
            ])
        )
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    // create a physical node,  with any additional relevant info that u think will come in handy when scanning ur data
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&[usize]>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(projection, self.schema())
    }
}

#[derive(Debug, Clone)]
struct CustomExec {
    db: GitLogsDataSource,
    projection: Option<Vec<usize>>,
    cache: Arc<PlanProperties>,
}

impl CustomExec {
    #[expect(clippy::needless_pass_by_value)]
    fn new(
        projections: Option<&[usize]>,
        schema: SchemaRef,
        db: GitLogsDataSource,
    ) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
        let cache = Self::compute_properties(projected_schema.clone());
        Self {
            db,
            projection: projections.map(|p| p.to_vec()),
            cache: Arc::new(cache),
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "CustomExec")
    }
}

impl ExecutionPlan for CustomExec {
    fn name(&self) -> &'static str {
        "CustomExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn replace_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    // using the data u gave us, we can proceed to scan ur data from ur custom data source and return `RecordBatch`s 
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // TODO; implement some form of projection/filter/limit pushdowns
        let batch_size = _context.session_config().batch_size();
        let table_schema = self.db.schema();

        let mut repo = gix::discover(&self.db.repo_path).map_err(git)?;
        repo.object_cache_size_if_unset(4 * 1024 * 1024);

        let mut hash = StringBuilder::new();
        let mut author_name = StringBuilder::new();
        let mut author_email = StringBuilder::new();
        let mut author_time = TimestampSecondBuilder::new().with_timezone("UTC");
        let mut tz_offset = Int32Builder::new();
        let mut title = StringBuilder::new();
        let mut body = StringBuilder::new();

        let mut batches = Vec::new();
        let mut rows = 0;

        // finish() empties each builder, so they're ready for the next batch
        macro_rules! flush {
            () => {
                batches.push(RecordBatch::try_new(
                    table_schema.clone(),
                    vec![
                        Arc::new(hash.finish()) as ArrayRef,
                        Arc::new(author_name.finish()),
                        Arc::new(author_email.finish()),
                        Arc::new(author_time.finish()),
                        Arc::new(tz_offset.finish()),
                        Arc::new(title.finish()),
                        Arc::new(body.finish()),
                    ],
                )?);
                rows = 0;
            };
        }

        let head = repo.head_id().map_err(git)?;
        for info in repo.rev_walk([head]).all().map_err(git)? {
            let commit = info.map_err(git)?.object().map_err(git)?;
            let author = commit.author().map_err(git)?;
            let time = author.time().map_err(git)?;
            let msg = commit.message().map_err(git)?;

            hash.append_value(commit.id.to_string());
            author_name.append_value(author.name.to_str_lossy());
            author_email.append_value(author.email.to_str_lossy());
            author_time.append_value(time.seconds);
            tz_offset.append_value(time.offset);
            title.append_value(msg.title.to_str_lossy());
            match msg.body {
                Some(b) => body.append_value(b.to_str_lossy()),
                None => body.append_null(),
            }

            rows += 1;
            if rows == batch_size {
                flush!();
            }
        }

        if rows > 0 {
            flush!(); // the last, partial batch
        }

        // ...and let MemoryStream apply the projection.
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            table_schema,
            self.projection.clone(),
        )?))
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}
