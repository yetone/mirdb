//! HTTP server implementation using Hyper.
//!
//! Owner: Scenario 1 - HTTP Server Setup
//!
//! This module provides the HTTP server that runs alongside
//! the Memcached protocol server on a configurable port.

use std::net::SocketAddr;

use futures::future::Future;
use hyper::rt;
use hyper::service::service_fn_ok;
use hyper::{Body, Request, Response, Server};

use super::routes::handle_request;

/// Run the HTTP server on the specified address.
///
/// This function spawns the HTTP server using the Hyper runtime.
/// It handles incoming HTTP requests and routes them to the appropriate handlers.
///
/// # Arguments
///
/// * `addr` - The socket address to bind the HTTP server to
///
/// # Returns
///
/// An `impl Future` that represents the running server.
pub fn run_http_server(addr: SocketAddr) -> impl Future<Item = (), Error = ()> {
    let make_service = || service_fn_ok(|req: Request<Body>| -> Response<Body> { handle_request(req) });

    Server::bind(&addr)
        .serve(make_service)
        .map_err(move |e| {
            eprintln!("HTTP server error on {}: {}", addr, e);
        })
}

/// Start the HTTP server with informative startup message.
///
/// This function is the main entry point for the HTTP server.
/// It prints a startup message and runs the server.
///
/// # Arguments
///
/// * `addr` - The socket address to bind the HTTP server to
pub fn start_http_server(addr: SocketAddr) {
    println!("HTTP server listening on http://{}", addr);

    let server = run_http_server(addr);
    rt::run(server);
}
