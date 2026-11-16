//! A minimal "Kylin-style" cube router on DataFusion.
//!
//! Base data: a `sales` table stored as Parquet.
//! Cube: `sales` pre-aggregated at `(region, category)` grain, stored as Parquet,
//! with `amount` (SUM), `cnt_rows`, `cnt_amount`, `min_amount` and `max_amount`.
//!
//! Two hit modes:
//!
//! - **Scan**: every aggregate is `SUM(<measure>)`. The cube is at a finer grain,
//!   so `SUM(SUM(amount)) = SUM(amount)`: registering `sales` from the cube Parquet
//!   is enough, the SQL is unchanged.
//! - **Rewrite**: `COUNT(*)`, `COUNT(x)`, `AVG(x)`, `MIN(x)` and `MAX(x)` cannot be
//!   answered by a scan swap. The logical plan's `Aggregate` is rewritten to read
//!   the cube's precomputed measures, and the original aggregate output names are
//!   preserved so projections/sorts above it keep working.
//!
//! Anything else falls back to the base Parquet.
//!
//! Swap `register_parquet` for an `IcebergTableProvider` to use Iceberg as the base
//! table; the cube side stays Parquet.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::functions_aggregate::expr_fn::{max, min, sum};
use datafusion::logical_expr::{
    binary_expr, cast, Aggregate, Expr, LogicalPlan, Operator,
};
use datafusion::prelude::*;

/// The pre-aggregated cube's metadata.
#[derive(Clone)]
struct Cube {
    name: String,
    path: PathBuf,
    /// Group-by columns the cube can answer.
    dimensions: Vec<String>,
    /// Raw measures of the base table. The cube stores `<measure>` as SUM,
    /// plus `cnt_rows`, `cnt_<measure>`, `min_<measure>` and `max_<measure>`.
    measures: Vec<String>,
}

/// "SQL hit" simulation: normalized SQL text -> cube.
struct Registry {
    by_query: HashMap<String, Cube>,
}

impl Registry {
    fn hit(&self, sql: &str) -> Option<&Cube> {
        self.by_query.get(&normalize(sql))
    }
}

fn normalize(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// How a matching cube can answer the query.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Only SUM over measures: scan the cube, no aggregate rewrite.
    Scan,
    /// Some aggregate needs precomputed measures: scan + rewrite Aggregate.
    Rewrite,
}

/// Walk down the plan to the first Aggregate (past Projection/Sort/Limit).
fn find_aggregate(plan: &LogicalPlan) -> Option<&Aggregate> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => Some(aggregate),
        _ => plan.inputs().into_iter().find_map(find_aggregate),
    }
}

/// Every column the plan reads below the aggregate: scan filters/projections, etc.
fn collect_columns(plan: &LogicalPlan, out: &mut HashSet<String>) {
    for expr in plan.expressions() {
        for column in expr.column_refs() {
            out.insert(column.name.clone());
        }
    }
    for input in plan.inputs() {
        collect_columns(input, out);
    }
}

/// Columns used by filters below the aggregate. Filters may only reference cube
/// dimensions: filtering the cube's pre-aggregated measure is not the same query.
fn collect_filter_columns(plan: &LogicalPlan, out: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Filter(filter) => {
            for column in filter.predicate.column_refs() {
                out.insert(column.name.clone());
            }
        }
        LogicalPlan::TableScan(scan) => {
            for filter in &scan.filters {
                for column in filter.column_refs() {
                    out.insert(column.name.clone());
                }
            }
        }
        _ => {}
    }
    for input in plan.inputs() {
        collect_filter_columns(input, out);
    }
}

/// The measure a `SUM`/`AVG`/`MIN`/`MAX` argument refers to, if it is a bare cube
/// measure column and the aggregate has no DISTINCT/FILTER/ORDER BY.
fn measure_arg(args: &[Expr], cube: &Cube) -> Option<String> {
    let [Expr::Column(column)] = args else {
        return None;
    };
    let name = column.name.to_lowercase();
    cube.measures
        .iter()
        .any(|measure| measure.eq_ignore_ascii_case(&name))
        .then_some(name)
}

fn no_modifiers(function: &datafusion::logical_expr::expr::AggregateFunction) -> bool {
    !function.params.distinct
        && function.params.filter.is_none()
        && function.params.order_by.is_empty()
}

/// `SUM(<measure>)`: the only aggregate a pure scan swap can answer.
fn is_plain_sum(expr: &Expr, cube: &Cube) -> bool {
    let Expr::AggregateFunction(function) = expr.clone().unalias() else {
        return false;
    };
    function.func.name().eq_ignore_ascii_case("sum")
        && no_modifiers(&function)
        && measure_arg(&function.params.args, cube).is_some()
}

/// Rewrite one aggregate over the base table into aggregates over cube measures.
///
/// `SUM(x)`   -> `SUM(x)`              (cube's `x` is already the sum)
/// `COUNT(*)` -> `SUM(cnt_rows)`
/// `COUNT(x)` -> `SUM(cnt_x)`
/// `AVG(x)`   -> `SUM(x) / SUM(cnt_x)`
/// `MIN(x)`   -> `MIN(min_x)`
/// `MAX(x)`   -> `MAX(max_x)`
fn rewrite_agg_expr(expr: &Expr, cube: &Cube) -> Option<Expr> {
    let Expr::AggregateFunction(function) = expr.clone().unalias() else {
        return None;
    };
    if !no_modifiers(&function) {
        return None;
    }

    let name = function.func.name().to_lowercase();
    let args = function.params.args.as_slice();
    match name.as_str() {
        "sum" => {
            let measure = measure_arg(args, cube)?;
            Some(sum(col(measure)))
        }
        "min" => {
            let measure = measure_arg(args, cube)?;
            Some(min(col(format!("min_{measure}"))))
        }
        "max" => {
            let measure = measure_arg(args, cube)?;
            Some(max(col(format!("max_{measure}"))))
        }
        "avg" => {
            let measure = measure_arg(args, cube)?;
            // Float64 division: cast the count so integer measures do not truncate.
            let total = sum(col(measure.clone()));
            let count = cast(sum(col(format!("cnt_{measure}"))), DataType::Float64);
            Some(binary_expr(total, Operator::Divide, count))
        }
        "count" => match args {
            // `COUNT(*)` is `count(Int64(1))` in a DataFusion logical plan.
            [Expr::Literal(..)] => Some(sum(col("cnt_rows"))),
            _ => {
                let measure = measure_arg(args, cube)?;
                Some(sum(col(format!("cnt_{measure}"))))
            }
        },
        _ => None,
    }
}

/// Whether the cube can answer the plan at all, and how.
fn cube_mode(cube: &Cube, plan: &LogicalPlan) -> Option<Mode> {
    let aggregate = find_aggregate(plan)?;

    // A filter/projection on a base-only column cannot be answered from the cube.
    // Only look below the Aggregate: projections above it reference aggregate
    // outputs (e.g. the synthetic column `sum(sales.amount)`), not base columns.
    let mut columns = HashSet::new();
    collect_columns(aggregate.input.as_ref(), &mut columns);
    for expr in aggregate.group_expr.iter().chain(aggregate.aggr_expr.iter()) {
        for column in expr.column_refs() {
            columns.insert(column.name.clone());
        }
    }
    let known = |name: &str| {
        cube.dimensions
            .iter()
            .chain(cube.measures.iter())
            .any(|column| column.eq_ignore_ascii_case(name))
    };
    if !columns.iter().all(|column| known(column)) {
        return None;
    }

    // A filter below the aggregate may only use dimensions. `WHERE amount > 10`
    // must scan the base table: cube.amount is already a SUM.
    let mut filter_columns = HashSet::new();
    collect_filter_columns(aggregate.input.as_ref(), &mut filter_columns);
    let is_dimension = |name: &str| {
        cube.dimensions
            .iter()
            .any(|dimension| dimension.eq_ignore_ascii_case(name))
    };
    if !filter_columns.iter().all(|column| is_dimension(column)) {
        return None;
    }

    // GROUP BY must be cube dimensions.
    let group_ok = aggregate.group_expr.iter().all(|expr| {
        match expr.clone().unalias() {
            Expr::Column(column) => cube
                .dimensions
                .iter()
                .any(|dimension| dimension.eq_ignore_ascii_case(&column.name)),
            _ => false,
        }
    });
    if !group_ok {
        return None;
    }

    if aggregate
        .aggr_expr
        .iter()
        .all(|expr| is_plain_sum(expr, cube))
    {
        return Some(Mode::Scan);
    }

    if aggregate
        .aggr_expr
        .iter()
        .all(|expr| rewrite_agg_expr(expr, cube).is_some())
    {
        return Some(Mode::Rewrite);
    }

    None
}

/// Replace the `sales` TableScan inside `plan` with the cube scan, keeping any
/// filters/projections in between (e.g. `WHERE region = 'east'`).
fn replace_sales_scan(plan: LogicalPlan, cube_scan: &LogicalPlan) -> Option<LogicalPlan> {
    let mut replaced = false;
    let transformed = plan
        .transform_up(|node| {
            if let LogicalPlan::TableScan(scan) = &node {
                if scan.table_name.table().eq_ignore_ascii_case("sales") {
                    replaced = true;
                    return Ok(Transformed::yes(cube_scan.clone()));
                }
            }
            Ok(Transformed::no(node))
        })
        .ok()?
        .data;
    replaced.then_some(transformed)
}

/// Replace the plan's `Aggregate` with one over the cube scan, aliasing each new
/// aggregate expression to the old output name so projections above it resolve.
fn rewrite_aggregate(
    aggregate: &Aggregate,
    cube: &Cube,
    cube_scan: &LogicalPlan,
) -> Option<LogicalPlan> {
    let group_count = aggregate.group_expr.len();
    let mut aggr_expr = Vec::with_capacity(aggregate.aggr_expr.len());
    for (index, expr) in aggregate.aggr_expr.iter().enumerate() {
        let rewritten = rewrite_agg_expr(expr, cube)?;
        let old_name = aggregate
            .schema
            .field(group_count + index)
            .name()
            .to_string();
        aggr_expr.push(rewritten.alias(old_name));
    }

    // Keep the input tree (filters/projections); only its `sales` scan changes.
    let input = replace_sales_scan(aggregate.input.as_ref().clone(), cube_scan)?;
    let rewritten = Aggregate::try_new(Arc::new(input), aggregate.group_expr.clone(), aggr_expr)
        .ok()?;
    Some(LogicalPlan::Aggregate(rewritten))
}

/// Swap the Aggregate node in `plan` for the cube-backed one.
fn rewrite_with_cube(
    plan: LogicalPlan,
    cube: &Cube,
    cube_scan: &LogicalPlan,
) -> Option<LogicalPlan> {
    let mut replaced = false;
    let transformed = plan
        .transform_up(|node| {
            if replaced {
                return Ok(Transformed::no(node));
            }
            if let LogicalPlan::Aggregate(aggregate) = &node {
                if let Some(rewritten) = rewrite_aggregate(aggregate, cube, cube_scan) {
                    replaced = true;
                    return Ok(Transformed::yes(rewritten));
                }
            }
            Ok(Transformed::no(node))
        })
        .ok()?
        .data;
    replaced.then_some(transformed)
}

fn new_context(config: &SessionConfig) -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config.clone())
        .build();
    SessionContext::new_with_state(state)
}

async fn register_sales(ctx: &SessionContext, path: &Path) -> Result<()> {
    ctx.register_parquet(
        "sales",
        path.to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await
}

/// Build the base Parquet table and the cube Parquet table from generated data.
async fn build_data(root: &Path) -> Result<()> {
    let _ = std::fs::remove_dir_all(root);
    let base_dir = root.join("base");
    let cube_dir = root.join("cube");
    std::fs::create_dir_all(&base_dir)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    std::fs::create_dir_all(&cube_dir)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;

    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("sale_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));

    // Deterministic sample rows: 4 regions x 3 categories.
    let rows = 10_000usize;
    let regions = ["east", "west", "north", "south"];
    let categories = ["books", "games", "food"];
    let ids: Vec<i64> = (0..rows as i64).collect();
    let region_values: Vec<&str> = (0..rows).map(|i| regions[i % regions.len()]).collect();
    let category_values: Vec<&str> = (0..rows)
        .map(|i| categories[(i / regions.len()) % categories.len()])
        .collect();
    let amount_values: Vec<f64> = (0..rows).map(|i| (i % 100) as f64 + 0.5).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(region_values)),
            Arc::new(StringArray::from(category_values)),
            Arc::new(Float64Array::from(amount_values)),
        ],
    )?;
    ctx.register_table(
        "sales",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;

    // Base table (the "Iceberg/Parquet" side of the demo).
    let copy_base = format!(
        "COPY (SELECT * FROM sales) TO '{}/' STORED AS PARQUET",
        base_dir.display()
    );
    ctx.sql(&copy_base).await?.collect().await?;

    // Cube: finer grain than the queries, with additive and non-additive measures.
    let copy_cube = format!(
        "COPY (SELECT region, category, \
         SUM(amount) AS amount, \
         COUNT(*) AS cnt_rows, \
         COUNT(amount) AS cnt_amount, \
         MIN(amount) AS min_amount, \
         MAX(amount) AS max_amount \
         FROM sales GROUP BY region, category) TO '{}/' STORED AS PARQUET",
        cube_dir.display()
    );
    ctx.sql(&copy_cube).await?.collect().await?;

    Ok(())
}

/// Run `sql` with table `sales` backed by `path`, printing the plan and timing.
async fn run_scan(config: &SessionConfig, path: &Path, sql: &str) -> Result<Vec<RecordBatch>> {
    let ctx = new_context(config);
    register_sales(&ctx, path).await?;

    println!("\nSQL: {sql}");
    println!("scan: {}", path.display());

    // EXPLAIN shows the actual scan operator and file group.
    // ctx.sql(&format!("EXPLAIN {sql}")).await?.show().await?;

    let started = Instant::now();
    let batches = ctx.sql(sql).await?.collect().await?;
    let elapsed = started.elapsed();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    println!("rows={rows}, elapsed={elapsed:?}");
    Ok(batches)
}

/// Run a rewritten plan over the cube context.
async fn run_rewritten(
    cube_ctx: &SessionContext,
    cube: &Cube,
    plan: LogicalPlan,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    println!("\nSQL: {sql}");
    println!("scan: {} + aggregate rewrite", cube.path.display());

    let df = DataFrame::new(cube_ctx.state(), plan);
    df.clone().explain(false, false)?.show().await?;

    let started = Instant::now();
    let batches = df.collect().await?;
    let elapsed = started.elapsed();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    println!("rows={rows}, elapsed={elapsed:?}");
    Ok(batches)
}

fn format_batches(batches: &[RecordBatch]) -> Result<String> {
    Ok(pretty_format_batches(batches)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?
        .to_string())
}

/// Compare the cube result with the same SQL over the base table.
fn check_equal(label: &str, cube: &[RecordBatch], base: &[RecordBatch]) -> Result<()> {
    let cube_text = format_batches(cube)?;
    let base_text = format_batches(base)?;
    if cube_text != base_text {
        return Err(DataFusionError::Execution(format!(
            "{label}: cube result differs from base\ncube:\n{cube_text}\nbase:\n{base_text}"
        )));
    }
    println!("check: {label} result == base result");
    Ok(())
}

/// Route one SQL through the cube registry (or force a hit for CLI testing).
#[allow(clippy::too_many_arguments)]
async fn run_one(
    sql: &str,
    force_hit: bool,
    registry: &Registry,
    cube: &Cube,
    config: &SessionConfig,
    validator: &SessionContext,
    cube_ctx: &SessionContext,
    cube_scan: &LogicalPlan,
    base: &Path,
) -> Result<()> {
    let plan = validator.state().create_logical_plan(sql).await?;
    let hit = if force_hit { Some(cube) } else { registry.hit(sql) };
    let mode = hit.and_then(|cube| cube_mode(cube, &plan));

    match (hit, mode) {
        (Some(cube), Some(Mode::Scan)) => {
            println!("\n=== HIT (scan): cube '{}' ===", cube.name);
            let cube_batches = run_scan(config, &cube.path, sql).await?;
            let base_batches = validator.sql(sql).await?.collect().await?;
            check_equal("scan", &cube_batches, &base_batches)?;
        }
        (Some(cube), Some(Mode::Rewrite)) => {
            println!("\n=== HIT (aggregate rewrite): cube '{}' ===", cube.name);
            let rewritten = rewrite_with_cube(plan, cube, cube_scan)
                .expect("cube_mode said the plan is rewriteable");
            let cube_batches = run_rewritten(cube_ctx, cube, rewritten, sql).await?;
            let base_batches = validator.sql(sql).await?.collect().await?;
            check_equal("aggregate rewrite", &cube_batches, &base_batches)?;
        }
        (Some(_), None) => {
            println!("\n=== MISS: configured, but the cube cannot answer it ===");
            run_scan(config, base, sql).await?;
        }
        (None, _) => {
            println!("\n=== MISS: no cube configured for this SQL ===");
            run_scan(config, base, sql).await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data");
    build_data(&root).await?;

    let base = root.join("base");
    let cube = Cube {
        name: "sales_by_region_category".to_string(),
        path: root.join("cube"),
        dimensions: vec!["region".to_string(), "category".to_string()],
        measures: vec!["amount".to_string()],
    };

    // A SUM-only query: a scan swap is enough.
    let hit_scan = "SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region";
    // COUNT/AVG/MIN/MAX need the precomputed measures.
    let hit_rewrite = "SELECT region, SUM(amount) AS total, COUNT(*) AS n, \
                       AVG(amount) AS avg_amount, MIN(amount) AS min_amount, \
                       MAX(amount) AS max_amount \
                       FROM sales GROUP BY region ORDER BY region";
    let hit_dimension_filter =
        "SELECT region, SUM(amount) AS total FROM sales WHERE region = 'east' GROUP BY region";
    let hit_rewrite_filter = "SELECT region, AVG(amount) AS avg_amount FROM sales \
                              WHERE region = 'east' GROUP BY region";
    // Configured, but DISTINCT is not precomputed.
    let configured_but_distinct =
        "SELECT region, COUNT(DISTINCT amount) AS n FROM sales GROUP BY region";
    // Configured, but `sale_id` is not in the cube.
    let configured_but_base_filter =
        "SELECT region, SUM(amount) AS total FROM sales WHERE sale_id > 100 GROUP BY region";
    // Configured, but a filter on a measure cannot use the pre-aggregated column.
    let configured_but_measure_filter =
        "SELECT region, SUM(amount) AS total FROM sales WHERE amount > 10 GROUP BY region";
    // No cube configured.
    let no_hit =
        "SELECT category, AVG(amount) AS avg_amount FROM sales GROUP BY category ORDER BY category";

    let mut by_query = HashMap::new();
    for sql in [
        hit_scan,
        hit_rewrite,
        hit_dimension_filter,
        hit_rewrite_filter,
        configured_but_distinct,
        configured_but_base_filter,
        configured_but_measure_filter,
    ] {
        by_query.insert(normalize(sql), cube.clone());
    }
    let registry = Registry { by_query };

    let config = SessionConfig::new().with_target_partitions(4);

    // Parse with the base table; execute the rewritten plan with the cube table.
    let validator = new_context(&config);
    register_sales(&validator, &base).await?;
    let cube_ctx = new_context(&config);
    register_sales(&cube_ctx, &cube.path).await?;
    let cube_scan = cube_ctx.table("sales").await?.logical_plan().clone();

    let builtin_queries = [
        hit_scan,
        hit_rewrite,
        hit_dimension_filter,
        hit_rewrite_filter,
        configured_but_distinct,
        configured_but_base_filter,
        configured_but_measure_filter,
        no_hit,
    ];

    // Usage:
    //   cargo run
    //   cargo run -- "SELECT ..."
    //   cargo run -- --hit "SELECT ..."
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => {
            for sql in builtin_queries {
                run_one(
                    sql,
                    false,
                    &registry,
                    &cube,
                    &config,
                    &validator,
                    &cube_ctx,
                    &cube_scan,
                    &base,
                )
                .await?;
            }
        }
        [sql] => {
            run_one(
                sql,
                false,
                &registry,
                &cube,
                &config,
                &validator,
                &cube_ctx,
                &cube_scan,
                &base,
            )
            .await?;
        }
        [flag, sql] if flag.as_str() == "--hit" => {
            run_one(
                sql,
                true,
                &registry,
                &cube,
                &config,
                &validator,
                &cube_ctx,
                &cube_scan,
                &base,
            )
            .await?;
        }
        _ => {
            eprintln!(
                "usage:\n  cargo run\n  cargo run -- \"SELECT ...\"\n  cargo run -- --hit \"SELECT ...\""
            );
        }
    }

    Ok(())
}
