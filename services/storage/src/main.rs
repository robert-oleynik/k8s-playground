use std::net::SocketAddr;

use anyhow::Context;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};
use tower::{Service, ServiceBuilder, ServiceExt};

#[derive(Deserialize)]
pub struct Config {
    #[serde(default = "default_addr")]
    addr: String,
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
) -> anyhow::Result<Response<Full<Bytes>>> {
    let method = req.method();
    let path = req.uri().path();
    tracing::debug!(method = method.as_str(), path);
    if !path.starts_with("/") || path.len() != 37 {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Full::new(Bytes::from("400 Bad Request")))
            .context("failed to build BAD_REQUEST response");
    }
    let uuid = match uuid::Uuid::parse_str(&path[1..]) {
        Ok(id) => id,
        Err(_) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("400 Bad Request")))
                .context("failed to build BAD_REQUEST response")
        }
    };
    match *method {
        Method::GET => todo!(),
        Method::PUT => todo!(),
        Method::POST => todo!(),
        Method::DELETE => todo!(),
        _ => Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Full::new(Bytes::from("405 Method Not Allowed")))
            .context("failed to build METHOD_NOT_ALLOWED response"),
    }
}

async fn handle_new_conn(stream: TcpStream) -> anyhow::Result<()> {
    let io = TokioIo::new(stream);
    tokio::task::spawn(async move {
        if let Err(err) = auto::Builder::new(TokioExecutor::new())
            .serve_connection(io, hyper::service::service_fn(handle_request))
            .await
        {
            tracing::error!("{err}")
        }
    });
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    tracing::debug!("debug logs enabled");

    let config: Config = envy::from_env().context("failed to parse environment config")?;

    tracing::debug!(addr = config.addr);
    let addr: SocketAddr = config
        .addr
        .parse()
        .context("failed to parse socket address")?;

    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind socket");
    let mut service = ServiceBuilder::new()
        .concurrency_limit(1024)
        .service(tower::service_fn(handle_new_conn));
    loop {
        let ready = service
            .ready()
            .await
            .context("failed to for service to becom ready")?;
        let (stream, addr) = listener
            .accept()
            .await
            .context("failed to accept new connection")?;
        tracing::debug!(addr = format!("{addr}"));
        if let Err(err) = ready.call(stream).await.context("service call failed") {
            tracing::error!("{err}");
        }
    }
}

fn default_addr() -> String {
    String::from("127.0.0.1:3000")
}
