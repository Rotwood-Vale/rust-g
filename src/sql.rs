use dashmap::DashMap;
use mysql_async::{
    OptsBuilder, Params, Pool, PoolConstraints, PoolOpts,
    consts::{ColumnFlags, ColumnType::*},
    prelude::Queryable,
};
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde_json::{Number, json, map::Map};
use std::error::Error;
use std::{collections::HashMap, sync::atomic::AtomicUsize};
use tokio::runtime::Handle;

static RUNTIME: Lazy<Handle> = Lazy::new(|| {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    let handle = runtime.handle().clone();

    std::thread::spawn(move || {
        runtime.block_on(std::future::pending::<()>());
    });

    handle
});

static QUERIES: Lazy<DashMap<usize, tokio::task::JoinHandle<String>>> = Lazy::new(DashMap::new);
static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(0);
// ----------------------------------------------------------------------------
// Interface

const DEFAULT_PORT: u16 = 3306;
// The `mysql` crate defaults to 10 and 100 for these, but that is too large.
const DEFAULT_MIN_CONNECTIONS: usize = 1;
const DEFAULT_MAX_CONNECTIONS: usize = 10;

#[derive(Deserialize)]
struct ConnectOptions {
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    pass: Option<String>,
    db_name: Option<String>,
    min_connections: Option<usize>,
    max_connections: Option<usize>,
}

byond_fn!(fn sql_connect_pool(options) {
    let options = match serde_json::from_str::<ConnectOptions>(options) {
        Ok(options) => options,
        Err(e) => return Some(err_to_json(e)),
    };
    Some(match sql_connect(options) {
        Ok(o) => o.to_string(),
        Err(e) => err_to_json(e)
    })
});

byond_fn!(fn sql_query_blocking(handle, query, params) {
    Some(match RUNTIME.block_on(do_query(handle, query, params)) {
        Ok(o) => o.to_string(),
        Err(e) => err_to_json(e)
    })
});

byond_fn!(fn sql_query_async(handle, query, params) {
    let handle = handle.to_owned();
    let query = query.to_owned();
    let params = params.to_owned();
    let join_handle = RUNTIME.spawn(async move {
        match do_query(&handle, &query, &params).await {
            Ok(o) => o.to_string(),
            Err(e) => err_to_json(e),
        }
    });
    let id = NEXT_QUERY_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    QUERIES.insert(id, join_handle);
    Some(id.to_string())
});

// hopefully won't panic if queries are running
byond_fn!(fn sql_disconnect_pool(handle) {
    let handle = match handle.parse::<usize>() {
        Ok(o) => o,
        Err(e) => return Some(err_to_json(e)),
    };
    Some(match POOL.remove(&handle) {
        Some((_, pool)) => {
            let _ = RUNTIME.block_on(pool.disconnect());
            json!({"status": "success"}).to_string()
        },
        None => json!({"status": "offline"}).to_string()
    })
});

byond_fn!(fn sql_connected(handle) {
    let handle = match handle.parse::<usize>() {
        Ok(o) => o,
        Err(e) => return Some(err_to_json(e)),
    };
    Some(
        match POOL.get(&handle) {
            Some(_) => json!({
                "status": "online"
            }).to_string(),
            None => json!({
                "status": "offline"
            }).to_string()
        }
    )
});

byond_fn!(fn sql_check_query(id) {
    let id = match id.parse::<usize>() {
        Ok(o) => o,
        Err(e) => return Some(err_to_json(e)),
    };
    match QUERIES.get(&id) {
        None => Some(json!({"status": "err", "data": "no such query"}).to_string()),
        Some(entry) => {
            if entry.is_finished() {
                drop(entry);
                let (_, join_handle) = QUERIES.remove(&id).unwrap();
                Some(RUNTIME.block_on(join_handle)
                    .unwrap_or_else(|e| err_to_json(e.to_string())))
            } else {
                Some(json!({"status": "running"}).to_string())
            }
        }
    }
});

// ----------------------------------------------------------------------------
// Main connect and query implementation

static POOL: Lazy<DashMap<usize, Pool>> = Lazy::new(DashMap::new);
static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn sql_connect(options: ConnectOptions) -> Result<serde_json::Value, Box<dyn Error + Send + Sync>> {
    let pool_constraints = PoolConstraints::new(
        options.min_connections.unwrap_or(DEFAULT_MIN_CONNECTIONS),
        options.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS),
    )
    .unwrap_or_else(|| {
        PoolConstraints::new(DEFAULT_MIN_CONNECTIONS, DEFAULT_MAX_CONNECTIONS).unwrap()
    });

    let pool_opts = PoolOpts::with_constraints(PoolOpts::new(), pool_constraints);
    let builder = OptsBuilder::default()
        .ip_or_hostname(options.host.unwrap_or_else(|| "localhost".to_string()))
        .tcp_port(options.port.unwrap_or(DEFAULT_PORT))
        // Work around addresses like `localhost:3307` defaulting to socket as
        // if the port were the default too.
        .prefer_socket(options.port.is_none_or(|p| p == DEFAULT_PORT))
        .user(options.user)
        .pass(options.pass)
        .db_name(options.db_name)
        .pool_opts(pool_opts);

    let pool = Pool::new(builder);

    let handle = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    POOL.insert(handle, pool);
    Ok(json!({
        "status": "ok",
        "handle": handle.to_string(),
    }))
}

async fn do_query(
    handle: &str,
    query: &str,
    params: &str,
) -> Result<serde_json::Value, Box<dyn Error + Send + Sync>> {
    let mut conn = {
        let pool = match POOL.get(&handle.parse()?) {
            Some(s) => s,
            None => return Ok(json!({"status": "offline"})),
        };
        pool.get_conn().await?
    };

    let mut query_result = conn.exec_iter(query, params_from_json(params)).await?;

    let mut columns = Vec::new();
    for col in query_result.columns_ref() {
        columns.push(json!({
            "name": col.name_str(),
        }));
    }

    let affected = query_result.affected_rows();
    let last_insert_id = query_result.last_insert_id();

    let raw_rows: Vec<mysql_async::Row> = query_result.collect().await?;
    drop(query_result);

    let mut rows: Vec<serde_json::Value> = Vec::new();
    for row in raw_rows {
        let mut json_row: Vec<serde_json::Value> = Vec::new();
        for (i, col) in row.columns_ref().iter().enumerate() {
            let ctype = col.column_type();
            let value = row
                .as_ref(i)
                .ok_or("length of row was smaller than column count")?;
            let converted = match value {
                mysql_async::Value::Bytes(b) => match ctype {
                    MYSQL_TYPE_VARCHAR | MYSQL_TYPE_STRING | MYSQL_TYPE_VAR_STRING => {
                        serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
                    }
                    MYSQL_TYPE_BLOB
                    | MYSQL_TYPE_LONG_BLOB
                    | MYSQL_TYPE_MEDIUM_BLOB
                    | MYSQL_TYPE_TINY_BLOB => {
                        if col.flags().contains(ColumnFlags::BINARY_FLAG) {
                            serde_json::Value::Array(
                                b.iter()
                                    .map(|x| serde_json::Value::Number(Number::from(*x)))
                                    .collect(),
                            )
                        } else {
                            serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
                        }
                    }
                    _ => serde_json::Value::Null,
                },
                mysql_async::Value::Float(f) => serde_json::Value::Number(
                    Number::from_f64(f64::from(*f)).unwrap_or_else(|| Number::from(0)),
                ),
                mysql_async::Value::Double(f) => serde_json::Value::Number(
                    Number::from_f64(*f).unwrap_or_else(|| Number::from(0)),
                ),
                mysql_async::Value::Int(i) => serde_json::Value::Number(Number::from(*i)),
                mysql_async::Value::UInt(u) => serde_json::Value::Number(Number::from(*u)),
                mysql_async::Value::Date(year, month, day, hour, minute, second, _ms) => {
                    serde_json::Value::String(format!(
                        "{year}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}"
                    ))
                }
                _ => serde_json::Value::Null,
            };
            json_row.push(converted);
        }
        rows.push(serde_json::Value::Array(json_row));
    }

    drop(conn);

    Ok(json!({
        "status": "ok",
        "affected": affected,
        "last_insert_id": last_insert_id,
        "columns": columns,
        "rows": rows,
    }))
}

// ----------------------------------------------------------------------------
// Helpers

fn err_to_json<E: std::fmt::Display>(e: E) -> String {
    json!({
        "status": "err",
        "data": &e.to_string()
    })
    .to_string()
}

fn json_to_mysql(val: serde_json::Value) -> mysql_async::Value {
    match val {
        serde_json::Value::Bool(b) => mysql_async::Value::UInt(b as u64),
        serde_json::Value::Number(i) => {
            if let Some(v) = i.as_u64() {
                mysql_async::Value::UInt(v)
            } else if let Some(v) = i.as_i64() {
                mysql_async::Value::Int(v)
            } else if let Some(v) = i.as_f64() {
                mysql_async::Value::Float(v as f32) // Loses precision.
            } else {
                mysql_async::Value::NULL
            }
        }
        serde_json::Value::String(s) => mysql_async::Value::Bytes(s.into()),
        serde_json::Value::Array(a) => mysql_async::Value::Bytes(
            a.into_iter()
                .map(|x| {
                    if let serde_json::Value::Number(n) = x {
                        n.as_u64().unwrap_or(0) as u8
                    } else {
                        0
                    }
                })
                .collect(),
        ),
        _ => mysql_async::Value::NULL,
    }
}

fn array_to_params(params: Vec<serde_json::Value>) -> Params {
    if params.is_empty() {
        Params::Empty
    } else {
        Params::Positional(params.into_iter().map(json_to_mysql).collect())
    }
}

fn object_to_params(params: Map<std::string::String, serde_json::Value>) -> Params {
    if params.is_empty() {
        Params::Empty
    } else {
        Params::Named(
            params
                .into_iter()
                .map(|(key, val)| {
                    let key_bytes: Vec<u8> = key.into_bytes();
                    (key_bytes, json_to_mysql(val))
                })
                .collect::<HashMap<_, _>>(),
        )
    }
}

fn params_from_json(params: &str) -> Params {
    match serde_json::from_str(params) {
        Ok(serde_json::Value::Object(o)) => object_to_params(o),
        Ok(serde_json::Value::Array(a)) => array_to_params(a),
        _ => Params::Empty,
    }
}
