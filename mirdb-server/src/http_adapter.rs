use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::http_routes::{handle_request, parse_request};
use crate::options::Options;

pub struct HttpServer {
    opt: Arc<Options>,
    shutting_down: Arc<AtomicBool>,
}

impl HttpServer {
    pub fn new(opt: Options) -> Self {
        HttpServer {
            opt: Arc::new(opt),
            shutting_down: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Relaxed);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Relaxed)
    }
}

pub fn run_http_server(addr: &str, server: Arc<HttpServer>) -> std::io::Result<thread::JoinHandle<()>> {
    let listener = TcpListener::bind(addr)?;
    println!("HTTP adapter listening on http://{}", addr);

    let handle = thread::spawn(move || {
        listener.set_nonblocking(true).ok();
        for stream in listener.incoming() {
            if server.is_shutting_down() {
                break;
            }
            match stream {
                Ok(mut stream) => {
                    let opt = server.opt.clone();
                    let shutting_down = server.shutting_down.clone();
                    thread::spawn(move || {
                        stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
                        if let Some(request) = parse_request(&mut stream) {
                            let response = handle_request(&request, &opt, &shutting_down);
                            let _ = stream.write_all(response.as_bytes());
                        }
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    eprintln!("HTTP connection error: {}", e);
                }
            }
        }
    });

    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::get_test_opt;

    #[test]
    fn test_http_server_new() {
        let opt = get_test_opt();
        let server = HttpServer::new(opt);
        assert!(!server.is_shutting_down());
    }

    #[test]
    fn test_http_server_shutdown() {
        let opt = get_test_opt();
        let server = HttpServer::new(opt);
        server.shutdown();
        assert!(server.is_shutting_down());
    }
}
