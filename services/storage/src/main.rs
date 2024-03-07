use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::Path;

use anyhow::Context;
use futures_util::TryStreamExt;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpListener;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_addr")]
    addr: String,
    #[serde(default = "default_data_dir")]
    data_dir: String,
}

fn build_response(
    code: StatusCode,
    message: &'static str,
) -> anyhow::Result<Response<BoxBody<Bytes, std::io::Error>>> {
    let body = Full::new(Bytes::from(message))
        .map_err(|e| match e {})
        .boxed();
    Response::builder()
        .status(code)
        .body(body)
        .context("failed to build response")
}

fn build_response_string(
    code: StatusCode,
    message: String,
) -> anyhow::Result<Response<BoxBody<Bytes, std::io::Error>>> {
    let body = Full::new(Bytes::from(message))
        .map_err(|e| match e {})
        .boxed();
    Response::builder()
        .status(code)
        .body(body)
        .context("failed to build response")
}

async fn get_file(
    file: impl AsRef<Path>,
) -> anyhow::Result<Response<BoxBody<Bytes, std::io::Error>>> {
    let file = match File::open(file.as_ref()).await {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return build_response(StatusCode::NOT_FOUND, "404 Not Found")
        }
        Err(err) => {
            tracing::error!("failed to read file: {err:?}");
            return build_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "500 Internal Server Error",
            );
        }
    };
    let stream = tokio_util::io::ReaderStream::new(file);
    let stream = StreamBody::new(stream.map_ok(Frame::data)).boxed();
    Response::builder()
        .status(StatusCode::OK)
        .body(stream)
        .context("failed to build response")
}

async fn write_file(
    id: uuid::Uuid,
    file: impl AsRef<Path>,
    stream: &mut Incoming,
) -> anyhow::Result<Response<BoxBody<Bytes, std::io::Error>>> {
    let file = File::create(file).await.context("failed to create file")?;
    let mut writer = BufWriter::new(file);
    while let Some(frame) = stream.frame().await {
        let frame = frame.context("failed to fetch frame")?;
        if let Some(data) = frame.data_ref() {
            match writer.write(&data[..]).await {
                Ok(_) => {}
                Err(err) if err.kind() == ErrorKind::OutOfMemory => {
                    return build_response(
                        StatusCode::INSUFFICIENT_STORAGE,
                        "507 Insufficient Storage",
                    )
                }
                Err(err) => return Err(err).context("failed to write to file"),
            };
        } else {
            tracing::warn!("skipping frame. Received frame is not a data frame.");
        }
    }
    build_response_string(StatusCode::OK, id.to_string())
}

async fn handle_request(
    mut req: Request<hyper::body::Incoming>,
    config: &Config,
) -> anyhow::Result<Response<BoxBody<Bytes, std::io::Error>>> {
    let method = req.method();
    let path = req.uri().path();
    if let Method::POST = *method {
        if path == "/" {
            let id = uuid::Uuid::new_v4();
            if tokio::fs::try_exists(&path).await.unwrap_or(true) {
                return build_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "500 Internal Server Error",
                );
            }
            let path = format!("{}/{id}", config.data_dir);
            return write_file(id, path, req.body_mut()).await;
        } else {
            return build_response(StatusCode::NOT_FOUND, "404 Not Found");
        }
    }

    if !path.starts_with("/") || path.len() != 37 {
        return build_response(StatusCode::BAD_REQUEST, "400 Bad Request");
    }

    let uuid = match uuid::Uuid::parse_str(&path[1..]) {
        Ok(id) => id,
        Err(_) => return build_response(StatusCode::BAD_REQUEST, "400 Bad Request"),
    };
    let path = format!("{}/{uuid}", config.data_dir);
    match *method {
        Method::GET => get_file(&path).await,
        Method::PUT => write_file(uuid, path, req.body_mut()).await,
        Method::DELETE => {
            if tokio::fs::try_exists(&path).await.unwrap_or(false) {
                return build_response(StatusCode::NOT_FOUND, "404 Not Found");
            }
            if let Err(err) = tokio::fs::remove_file(&path).await {
                tracing::error!("{err}");
                return build_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "500 Internal Server Error",
                );
            }
            return build_response(StatusCode::OK, "200 OK");
        }
        _ => build_response(StatusCode::METHOD_NOT_ALLOWED, "405 Method Not Allowed"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    tracing::debug!("debug logs enabled");

    let config: Box<Config> = envy::from_env::<Config>()
        .context("failed to parse environment config")?
        .into();
    tracing::debug!("{config:#?}");
    let config: &'static Config = Box::leak(config);

    tracing::debug!(addr = config.addr);
    let addr: SocketAddr = config
        .addr
        .parse()
        .context("failed to parse socket address")?;

    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind socket");
    loop {
        let (stream, addr) = listener
            .accept()
            .await
            .context("failed to accept new connection")?;
        tracing::debug!(addr = format!("{addr}"));
        let io = TokioIo::new(stream);
        tokio::task::spawn(async move {
            if let Err(err) = auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, service_fn(move |req| handle_request(req, config)))
                .await
            {
                tracing::error!("{err}")
            }
        });
    }
}

fn default_addr() -> String {
    String::from("127.0.0.1:3000")
}

fn default_data_dir() -> String {
    String::from("/var/lib/storage")
}
