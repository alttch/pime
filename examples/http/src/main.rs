use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use pyo3::prelude::*;
use serde_value::Value;
use std::convert::Infallible;
use std::env;
use std::net::SocketAddr;
use tokio::signal::unix::{signal, SignalKind};

macro_rules! http_error {
    ($status: expr) => {
        Ok(Response::builder()
            .status($status)
            .body(Body::from(String::new()))
            .unwrap())
    };
}

async fn handler(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let (parts, _body) = req.into_parts();
    let path = parts.uri.path();
    if parts.method == Method::GET {
        let mut task = pime::PyTask::new0(Value::String(path.to_owned()));
        // exclusive tasks lock Python thread until finished
        // use with care!
        task.mark_exclusive();
        match pime::call(task).await {
            Ok(v) => match *v.unwrap() {
                Value::String(s) => Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(s))
                    .unwrap()),
                _ => {
                    http_error!(StatusCode::INTERNAL_SERVER_ERROR)
                }
            },
            Err(e) if e.kind == pime::ErrorKind::PyException => {
                match e.exception.unwrap().as_str() {
                    "KeyError" => http_error!(StatusCode::NOT_FOUND),
                    _ => http_error!(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
            Err(_) => {
                http_error!(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    } else {
        http_error!(StatusCode::METHOD_NOT_ALLOWED)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::task::spawn_blocking(move || {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let engine = pime::PySyncEngine::new(&py).unwrap();
            let cwd = env::current_dir().unwrap().to_str().unwrap().to_owned();
            engine.add_import_path(&cwd).unwrap();
            //engine.enable_debug().unwrap();
            engine.set_thread_pool_size(10, 10).unwrap();
            let module = py.import("myweb").unwrap();
            let broker = module.getattr("dispatcher").unwrap();
            engine.launch(&py, broker).unwrap();
        });
    });
    let addr = SocketAddr::from(([127, 0, 0, 1], 9922));
    let make_svc = make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(handler)) });
    tokio::spawn(async move {
        signal(SignalKind::terminate()).unwrap().recv().await;
        let _ = pime::stop().await;
        std::process::exit(0);
    });
    loop {
        let server = Server::bind(&addr).serve(make_svc);
        let _ = server.await;
    }
}
