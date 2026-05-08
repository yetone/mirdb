/**
 * HTTP web server for serving the MirDB homepage.
 * Owner: Scenario 1 - Homepage HTTP Endpoint
 *
 * Sets up an HTTP listener that serves the homepage and static assets.
 * Uses a lightweight threaded TCP server to avoid conflicts with tokio 0.1.
 *
 * Expected exports:
 * - start_http_server(addr, homepage_handler) -> Result: Binds HTTP server to address
 * - HomepageService: Service implementation for HTTP requests
 * - route_request(path) -> Response: Routes incoming requests to appropriate handlers
 */

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;

use crate::homepage;

/// Starts the HTTP server on the given address.
/// Spawns a new thread so the server runs concurrently with the memcached server.
pub fn start_http_server(addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    println!("HTTP server listening on http://{}", addr);

    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    thread::spawn(move || {
                        homepage::handle_request(&mut stream);
                    });
                }
                Err(e) => {
                    eprintln!("HTTP connection error: {}", e);
                }
            }
        }
    });

    Ok(())
}
