use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::http_routes::{handle_request, parse_request};
use crate::options::Options;
use crate::store::Store;

pub struct HttpServer {
    opt: Arc<Options>,
    store: Arc<Store>,
    shutting_down: Arc<AtomicBool>,
}

impl HttpServer {
    pub fn new(opt: Options, store: Arc<Store>) -> Self {
        HttpServer {
            opt: Arc::new(opt),
            store,
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
                    let store = server.store.clone();
                    let shutting_down = server.shutting_down.clone();
                    thread::spawn(move || {
                        stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
                        if let Some(request) = parse_request(&mut stream) {
                            let response = handle_request(&request, &opt, &store, &shutting_down);
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

/// Starts an HTTP server on a random available port for testing.
/// Returns the port number the server is listening on.
pub fn start_http_server_on_random_port(store: Arc<Store>) -> std::io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let store = store.clone();
                    thread::spawn(move || {
                        let mut stream = stream;
                        stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
                        if let Some(request) = parse_request(&mut stream) {
                            let opt = Arc::new(Options::default());
                            let shutting_down = Arc::new(AtomicBool::new(false));
                            let response = handle_request(&request, &opt, &store, &shutting_down);
                            let _ = stream.write_all(response.as_bytes());
                        }
                    });
                }
                Err(e) => {
                    eprintln!("HTTP connection error: {}", e);
                }
            }
        }
    });
    Ok(port)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::get_test_opt;

    fn make_test_store() -> Arc<Store> {
        let opt = get_test_opt();
        Arc::new(Store::new(opt).unwrap())
    }

    #[test]
    fn test_http_server_new() {
        let opt = get_test_opt();
        let store = make_test_store();
        let server = HttpServer::new(opt, store);
        assert!(!server.is_shutting_down());
    }

    #[test]
    fn test_http_server_shutdown() {
        let opt = get_test_opt();
        let store = make_test_store();
        let server = HttpServer::new(opt, store);
        server.shutdown();
        assert!(server.is_shutting_down());
    }
}
