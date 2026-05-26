use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::http::response::HttpResponse;
use crate::http::router;
use crate::store::Store;

pub fn start_http_server(addr: std::net::SocketAddr, store: Arc<Store>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    thread::spawn(move || {
        for stream in listener.incoming() {
            if let Ok(stream) = stream {
                let store = store.clone();
                thread::spawn(move || {
                    handle_connection(stream, store);
                });
            }
        }
    });
    Ok(())
}

fn handle_connection(mut stream: TcpStream, store: Arc<Store>) {
    let mut buf = [0u8; 4096];
    let n = match stream.read(&mut buf) {
        Ok(n) => n,
        Err(_) => return,
    };
    if n == 0 {
        return;
    }

    let request = match String::from_utf8(buf[..n].to_vec()) {
        Ok(s) => s,
        Err(_) => return,
    };

    let parts: Vec<&str> = request.lines().next().unwrap_or("").split_whitespace().collect();
    if parts.len() < 2 {
        return;
    }

    let method = parts[0];
    let path = parts[1];

    let response = router::dispatch(path, method, store);
    write_response(&mut stream, response);
}

fn write_response(stream: &mut TcpStream, response: HttpResponse) {
    let status_text = match response.status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        304 => "Not Modified",
        500 => "Internal Server Error",
        _ => "Unknown",
    };

    let mut headers_str = String::new();
    for (k, v) in &response.headers {
        headers_str.push_str(&format!("{}: {}\r\n", k, v));
    }

    let response_str = format!(
        "HTTP/1.1 {} {}\r\n{}\r\n",
        response.status,
        status_text,
        headers_str,
    );

    let _ = stream.write_all(response_str.as_bytes());
    let _ = stream.write_all(&response.body);
}
