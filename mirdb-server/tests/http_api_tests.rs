use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mirdb::http_adapter::{run_http_server, HttpServer};
use mirdb::options::Options;

fn send_request(host: &str, request: &str) -> String {
    try_send_request(host, request).expect("Failed to connect to server")
}

fn try_send_request(host: &str, request: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(host)?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                response.push_str(&String::from_utf8_lossy(&buf[..n]));
                if n < buf.len() {
                    break;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(e),
        }
    }
    Ok(response)
}

fn get_test_opt() -> Options {
    let mut opt = Options::default();
    opt.work_dir = "/tmp/mirdbtest_http".to_string();
    opt.mem_table_max_size = 1;
    opt.imm_mem_table_max_count = 1;
    opt
}

fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

#[test]
fn test_get_config_returns_200_with_expected_fields() {
    let port = get_available_port();
    let addr = format!("127.0.0.1:{}", port);
    let opt = get_test_opt();

    let server = Arc::new(HttpServer::new(opt));
    let handle = run_http_server(&addr, server.clone()).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    let request = format!(
        "GET /api/config HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    );
    let response = send_request(&addr, &request);

    assert!(response.contains("HTTP/1.1 200 OK"), "Expected 200 OK, got: {}", response);
    assert!(
        response.contains("\"addr\":"),
        "Expected addr field, got: {}",
        response
    );
    assert!(
        response.contains("\"max_level\":"),
        "Expected max_level field, got: {}",
        response
    );
    assert!(
        response.contains("\"work_dir\":"),
        "Expected work_dir field, got: {}",
        response
    );
    assert!(
        response.contains("\"sst_max_size\":"),
        "Expected sst_max_size field, got: {}",
        response
    );
    assert!(
        response.contains("\"mem_table_max_size\":"),
        "Expected mem_table_max_size field, got: {}",
        response
    );
    assert!(
        response.contains("\"mem_table_max_height\":"),
        "Expected mem_table_max_height field, got: {}",
        response
    );
    assert!(
        response.contains("\"imm_mem_table_max_count\":"),
        "Expected imm_mem_table_max_count field, got: {}",
        response
    );
    assert!(
        response.contains("\"block_size\":"),
        "Expected block_size field, got: {}",
        response
    );
    assert!(
        response.contains("\"block_restart_interval\":"),
        "Expected block_restart_interval field, got: {}",
        response
    );
    assert!(
        response.contains("\"l0_compaction_trigger\":"),
        "Expected l0_compaction_trigger field, got: {}",
        response
    );
    assert!(
        response.contains("\"thread_sleep_ms\":"),
        "Expected thread_sleep_ms field, got: {}",
        response
    );

    server.shutdown();
    let _ = handle.join();
}

#[test]
fn test_get_health_returns_200_healthy_and_fast() {
    let port = get_available_port();
    let addr = format!("127.0.0.1:{}", port);
    let opt = get_test_opt();

    let server = Arc::new(HttpServer::new(opt));
    let handle = run_http_server(&addr, server.clone()).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    let request = format!(
        "GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    );

    let start = Instant::now();
    let response = send_request(&addr, &request);
    let elapsed = start.elapsed();

    assert!(
        response.contains("HTTP/1.1 200 OK"),
        "Expected 200 OK, got: {}",
        response
    );
    assert!(
        response.contains("\"status\":\"healthy\""),
        "Expected healthy status, got: {}",
        response
    );
    assert!(
        elapsed < Duration::from_millis(100),
        "Response took too long: {:?}",
        elapsed
    );

    server.shutdown();
    let _ = handle.join();
}

#[test]
fn test_get_health_when_shutting_down_returns_503() {
    let port = get_available_port();
    let addr = format!("127.0.0.1:{}", port);
    let opt = get_test_opt();

    let server = Arc::new(HttpServer::new(opt));
    let handle = run_http_server(&addr, server.clone()).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    server.shutdown();
    std::thread::sleep(Duration::from_millis(100));

    let request = format!(
        "GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    );
    match try_send_request(&addr, &request) {
        Ok(response) => {
            assert!(
                response.contains("HTTP/1.1 503"),
                "Expected 503, got: {}",
                response
            );
            assert!(
                response.contains("\"status\":\"unhealthy\""),
                "Expected unhealthy status, got: {}",
                response
            );
        }
        Err(e) => {
            assert!(
                e.kind() == std::io::ErrorKind::ConnectionRefused,
                "Expected connection refused, got: {:?}",
                e
            );
        }
    }

    let _ = handle.join();
}

#[test]
fn test_cors_preflight_options_returns_204() {
    let port = get_available_port();
    let addr = format!("127.0.0.1:{}", port);
    let opt = get_test_opt();

    let server = Arc::new(HttpServer::new(opt));
    let handle = run_http_server(&addr, server.clone()).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    let request = format!(
        "OPTIONS /api/config HTTP/1.1\r\nHost: localhost\r\nOrigin: http://localhost:3000\r\nAccess-Control-Request-Method: GET\r\nConnection: close\r\n\r\n"
    );
    let response = send_request(&addr, &request);

    assert!(
        response.contains("HTTP/1.1 204 No Content"),
        "Expected 204, got: {}",
        response
    );
    assert!(
        response.contains("Access-Control-Allow-Origin:"),
        "Expected CORS origin header, got: {}",
        response
    );
    assert!(
        response.contains("Access-Control-Allow-Methods:"),
        "Expected CORS methods header, got: {}",
        response
    );

    server.shutdown();
    let _ = handle.join();
}

#[test]
fn test_post_config_returns_405() {
    let port = get_available_port();
    let addr = format!("127.0.0.1:{}", port);
    let opt = get_test_opt();

    let server = Arc::new(HttpServer::new(opt));
    let handle = run_http_server(&addr, server.clone()).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    let request = format!(
        "POST /api/config HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
    );
    let response = send_request(&addr, &request);

    assert!(
        response.contains("HTTP/1.1 405"),
        "Expected 405, got: {}",
        response
    );

    server.shutdown();
    let _ = handle.join();
}
