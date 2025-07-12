/// Representation of a single query operation.
#[derive(Debug, PartialEq, Eq)]
pub enum QueryPlan {
    ReadParquet(String),
    Filter(String),
    Select(Vec<String>),
    GroupBy(String),
    Agg(String),
    Sort(String),
}

/// Parse a simple query string into a sequence of `QueryPlan` steps.
///
/// The parser expects lines in the form `df = df.<op>(...)` or the initial
/// `df = pl.read_parquet("path")`. Supported operations are:
/// `read_parquet`, `filter`, `select`, `groupby`, `agg` and `sort`.
///
/// On success a vector of steps is returned in the order they were parsed.
pub fn parse_query(query: &str) -> Result<Vec<QueryPlan>, String> {
    let mut plan = Vec::new();

    for line in query.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if let Some(rest) = line.strip_prefix("df = pl.read_parquet(") {
            if let Some(path) = rest.strip_suffix(')') {
                let path = path.trim().trim_matches('"');
                plan.push(QueryPlan::ReadParquet(path.to_string()));
                continue;
            }
        }

        if let Some(rest) = line.strip_prefix("df = df.filter(") {
            if let Some(expr) = rest.strip_suffix(')') {
                plan.push(QueryPlan::Filter(expr.trim().to_string()));
                continue;
            }
        }

        if let Some(rest) = line.strip_prefix("df = df.select(") {
            if let Some(cols) = rest.strip_suffix(')') {
                let cols = cols.trim().trim_start_matches('[').trim_end_matches(']');
                let columns = cols
                    .split(',')
                    .map(|c| c.trim().trim_matches('"').to_string())
                    .filter(|c| !c.is_empty())
                    .collect();
                plan.push(QueryPlan::Select(columns));
                continue;
            }
        }

        if let Some(rest) = line.strip_prefix("df = df.groupby(") {
            if let Some(rest) = rest.split_once(')') {
                let col = rest.0.trim().trim_matches('"');
                plan.push(QueryPlan::GroupBy(col.to_string()));

                let remaining = rest.1.trim();
                if remaining.is_empty() {
                    continue;
                }
                if let Some(arg) = remaining.strip_prefix(".agg(") {
                    if let Some(arg) = arg.strip_suffix(')') {
                        plan.push(QueryPlan::Agg(arg.trim().to_string()));
                        continue;
                    }
                }
            }
        }

        if let Some(rest) = line.strip_prefix("df = df.sort(") {
            if let Some(col) = rest.strip_suffix(')') {
                plan.push(QueryPlan::Sort(col.trim().trim_matches('"').to_string()));
                continue;
            }
        }

        if let Some(rest) = line.strip_prefix("df = df.agg(") {
            if let Some(arg) = rest.strip_suffix(')') {
                plan.push(QueryPlan::Agg(arg.trim().to_string()));
                continue;
            }
        }

        return Err(format!("Invalid operation: {}", line));
    }

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_query() {
        let q = r#"
            df = pl.read_parquet("data/sample.parquet")
            df = df.filter(pl.col("age") > 30)
            df = df.groupby("city").agg(pl.col("age").mean())
        "#;

        let plan = parse_query(q).unwrap();
        assert_eq!(
            plan,
            vec![
                QueryPlan::ReadParquet("data/sample.parquet".into()),
                QueryPlan::Filter("pl.col(\"age\") > 30".into()),
                QueryPlan::GroupBy("city".into()),
                QueryPlan::Agg("pl.col(\"age\").mean()".into()),
            ]
        );
    }

    #[test]
    fn reject_invalid_operation() {
        let q = "df = df.foo()";
        assert!(parse_query(q).is_err());
    }
}
