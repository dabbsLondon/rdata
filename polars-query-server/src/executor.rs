use once_cell::sync::Lazy;
use polars::prelude::*;
use regex::Regex;

use crate::parser::{parse_query, QueryPlan};

/// Execute a textual query plan and return the resulting DataFrame.
pub fn execute_plan(plan: &str) -> PolarsResult<DataFrame> {
    let steps = parse_query(plan).map_err(|e| PolarsError::ComputeError(e.into()))?;
    execute_steps(steps)
}

fn execute_steps(steps: Vec<QueryPlan>) -> PolarsResult<DataFrame> {
    let mut lf: Option<LazyFrame> = None;
    let mut group_by: Option<String> = None;
    let mut aggs: Vec<Expr> = Vec::new();

    for step in steps {
        match step {
            QueryPlan::ReadParquet(path) => {
                lf = Some(LazyFrame::scan_parquet(&path, Default::default())?);
            }
            QueryPlan::Filter(expr) => {
                if let Some(lf_val) = lf.take() {
                    lf = Some(lf_val.filter(parse_filter(&expr)?));
                }
            }
            QueryPlan::Select(cols) => {
                if let Some(lf_val) = lf.take() {
                    let exprs: Vec<Expr> = cols.iter().map(|c| col(c)).collect();
                    lf = Some(lf_val.select(exprs));
                }
            }
            QueryPlan::GroupBy(colname) => {
                group_by = Some(colname);
            }
            QueryPlan::Agg(expr) => {
                aggs.push(parse_agg(&expr)?);
            }
            QueryPlan::Sort(colname) => {
                if let Some(lf_val) = lf.take() {
                    lf = Some(lf_val.sort(&colname, Default::default()));
                }
            }
        }
    }

    if let Some(gb) = group_by {
        if let Some(lf_val) = lf.take() {
            lf = Some(lf_val.group_by([col(&gb)]).agg(aggs));
        }
    }

    lf.expect("no dataframe built").collect()
}

static FILTER_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"pl\.col\("(?P<col>[^"]+)"\)\s*(?P<op>>=|<=|==|!=|>|<)\s*(?P<val>.+)"#).unwrap()
});

fn parse_filter(expr: &str) -> PolarsResult<Expr> {
    if let Some(c) = FILTER_RE.captures(expr) {
        let col_name = c.name("col").unwrap().as_str();
        let op = c.name("op").unwrap().as_str();
        let val_str = c.name("val").unwrap().as_str().trim().trim_matches('"');
        let val_expr = if let Ok(v) = val_str.parse::<i64>() {
            lit(v)
        } else if let Ok(v) = val_str.parse::<f64>() {
            lit(v)
        } else {
            lit(val_str)
        };
        let column = col(col_name);
        let out = match op {
            ">" => column.gt(val_expr),
            "<" => column.lt(val_expr),
            ">=" => column.gt_eq(val_expr),
            "<=" => column.lt_eq(val_expr),
            "==" => column.eq(val_expr),
            "!=" => column.neq(val_expr),
            _ => unreachable!(),
        };
        Ok(out)
    } else {
        Err(PolarsError::ComputeError("unsupported filter".into()))
    }
}

static AGG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"pl\.col\("(?P<col>[^"]+)"\)\.(?P<func>\w+)\(\)"#).unwrap());

fn parse_agg(expr: &str) -> PolarsResult<Expr> {
    if let Some(c) = AGG_RE.captures(expr) {
        let col_name = c.name("col").unwrap().as_str();
        let func = c.name("func").unwrap().as_str();
        let column = col(col_name);
        let out = match func {
            "sum" => column.sum(),
            "mean" => column.mean(),
            "min" => column.min(),
            "max" => column.max(),
            "count" => column.count(),
            _ => return Err(PolarsError::ComputeError("unsupported agg".into())),
        };
        Ok(out)
    } else {
        Err(PolarsError::ComputeError("unsupported agg".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::ParquetWriter;
    use std::fs::File;
    use tempfile::NamedTempFile;

    #[test]
    fn execute_basic_plan() {
        let mut df = df!["name" => ["a", "b"], "age" => [20, 40]].unwrap();
        let file = NamedTempFile::new().unwrap();
        ParquetWriter::new(File::create(file.path()).unwrap())
            .finish(&mut df)
            .unwrap();
        let q = format!(
            "df = pl.read_parquet(\"{}\")\ndf = df.filter(pl.col(\"age\") > 30)",
            file.path().to_str().unwrap()
        );
        let out = execute_plan(&q).unwrap();
        assert_eq!(out.height(), 1);
    }

    #[test]
    fn parse_filter_numeric_and_string() {
        let expr = parse_filter("pl.col(\"val\") >= 2").unwrap();
        let df = df!["val" => [1,2,3]].unwrap();
        let out = df.lazy().filter(expr).collect().unwrap();
        assert_eq!(out.column("val").unwrap().i32().unwrap().get(0), Some(2));

        let expr2 = parse_filter("pl.col(\"name\") == \"b\"").unwrap();
        let df2 = df!["name" => ["a","b"]].unwrap();
        let out2 = df2.lazy().filter(expr2).collect().unwrap();
        assert_eq!(out2.height(), 1);
    }

    #[test]
    fn parse_filter_invalid() {
        assert!(parse_filter("foo").is_err());
    }

    #[test]
    fn parse_filter_all_ops() {
        let df = df!["val" => [1,2,3]].unwrap();
        let ops = [">", "<", ">=", "<=", "==", "!="];
        for op in ops {
            let expr = format!("pl.col(\"val\") {} 2", op);
            assert!(parse_filter(&expr).is_ok());
        }
        // Ensure one of them actually filters as expected
        let expr = parse_filter("pl.col(\"val\") > 2").unwrap();
        let out = df.lazy().filter(expr).collect().unwrap();
        assert_eq!(out.height(), 1);
    }

    #[test]
    fn parse_agg_invalid() {
        assert!(parse_agg("pl.col(\"val\").foo()").is_err());
    }

    #[test]
    fn parse_agg_all_funcs() {
        let funcs = ["sum", "min", "max", "count", "mean"];
        for f in funcs {
            let expr = format!("pl.col(\"val\").{}()", f);
            assert!(parse_agg(&expr).is_ok());
        }
    }

    #[test]
    fn execute_groupby_sort() {
        let mut df = df!["city" => ["a", "b", "a"], "val" => [1, 2, 3]].unwrap();
        let file = NamedTempFile::new().unwrap();
        ParquetWriter::new(File::create(file.path()).unwrap())
            .finish(&mut df)
            .unwrap();
        let q = format!(
            "df = pl.read_parquet(\"{}\")\ndf = df.groupby(\"city\").agg(pl.col(\"val\").sum())\ndf = df.sort(\"city\")",
            file.path().display()
        );
        let out = execute_plan(&q).unwrap();
        assert_eq!(out.height(), 2);
    }

    #[test]
    fn execute_select_and_sort() {
        let mut df = df!["name" => ["a","b"], "age" => [20,10]].unwrap();
        let file = NamedTempFile::new().unwrap();
        ParquetWriter::new(File::create(file.path()).unwrap())
            .finish(&mut df)
            .unwrap();
        let q = format!(
            "df = pl.read_parquet(\"{}\")\ndf = df.select([\"age\",\"name\"])\ndf = df.sort(\"age\")",
            file.path().display()
        );
        let out = execute_plan(&q).unwrap();
        assert_eq!(out.column("age").unwrap().i32().unwrap().get(0), Some(10));
    }

    #[test]
    fn execute_top_level_agg() {
        let mut df = df!["val" => [1,2]].unwrap();
        let file = NamedTempFile::new().unwrap();
        ParquetWriter::new(File::create(file.path()).unwrap())
            .finish(&mut df)
            .unwrap();
        let q = format!(
            "df = pl.read_parquet(\"{}\")\ndf = df.agg(pl.col(\"val\").sum())",
            file.path().display()
        );
        let out = execute_plan(&q).unwrap();
        assert_eq!(out.height(), 2);
    }

    #[test]
    fn parse_agg_mean() {
        let expr = parse_agg("pl.col(\"val\").mean()").unwrap().alias("avg");
        let df = df!["val" => [1,2,3]].unwrap();
        let out = df.lazy().select([expr]).collect().unwrap();
        let v = out.column("avg").unwrap().f64().unwrap().get(0).unwrap();
        assert!( (v - 2.0).abs() < 1e-6 );
    }
}
