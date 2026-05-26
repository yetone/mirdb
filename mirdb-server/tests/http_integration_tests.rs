use std::io::{Read, Write};
use std::net::{TcpStream, SocketAddr};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use mirdb::http::handlers::api_status;
use mirdb::http::server::start_http_server;
use mirdb::options::Options;
use mirdb::request::{GetterType, Request, SetterType};
use mirdb::slice::Slice;
use mirdb::store::Store;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn get_test_opt() -> Options {
    let mut opt = Options::default();
    opt.work_dir = format!("/tmp/mirdbtest_http_{}", std::process::id());
    opt.mem_table_max_size = 1024 * 1024;
    opt.imm_mem_table_max_count = 16;
    opt
}

fn http_get(addr: SocketAddr, path: &str) -> (u16, String, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    let request = format!("GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", path);
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => response.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("Read error: {}", e),
        }
    }

    let response_str = String::from_utf8_lossy(&response);
    let first_line = response_str.lines().next().unwrap_or("");
    let status = first_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let mut headers = String::new();
    let mut in_body = false;
    let mut body_start = 0;
    for (i, line) in response_str.lines().enumerate() {
        if line.is_empty() {
            in_body = true;
            body_start = response_str.match_indices("\r\n\r\n").next().map(|(i, _)| i + 4)
                .or_else(|| response_str.match_indices("\n\n").next().map(|(i, _)| i + 2))
                .unwrap_or(0);
            break;
        }
        headers.push_str(line);
        headers.push('\n');
    }

    let body = if in_body && body_start < response.len() {
        response[body_start..].to_vec()
    } else {
        Vec::new()
    };

    (status, headers, body)
}

fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

#[test]
fn test_concurrent_memcached_and_http() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    api_status::clear_cache();

    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());
    let http_port = find_free_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", http_port).parse().unwrap();

    start_http_server(addr, store.clone()).unwrap();
    thread::sleep(Duration::from_millis(100));

    let http_errors = Arc::new(AtomicUsize::new(0));
    let memcached_errors = Arc::new(AtomicUsize::new(0));

    let http_errors_clone = http_errors.clone();
    let memcached_errors_clone = memcached_errors.clone();
    let store_clone = store.clone();

    let http_handle = thread::spawn(move || {
        for _ in 0..50 {
            let (status, _, _) = http_get(addr, "/api/status");
            if status != 200 {
                http_errors_clone.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let memcached_handle = thread::spawn(move || {
        for i in 0..50 {
            let key = Slice::from(format!("key{}", i));
            let payload = Slice::from(format!("value{}", i));
            let r = store_clone.apply(Request::Setter {
                setter: SetterType::Set,
                key: key.clone(),
                flags: 0,
                ttl: 60,
                bytes: payload.len(),
                payload: payload.clone(),
                no_reply: false,
            });
            if r.is_err() {
                memcached_errors_clone.fetch_add(1, Ordering::SeqCst);
                continue;
            }

            let r = store_clone.apply(Request::Getter {
                getter: GetterType::Get,
                keys: vec![key.clone()],
            });
            if r.is_err() {
                memcached_errors_clone.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    http_handle.join().unwrap();
    memcached_handle.join().unwrap();

    assert_eq!(http_errors.load(Ordering::SeqCst), 0, "HTTP requests had errors");
    assert_eq!(memcached_errors.load(Ordering::SeqCst), 0, "Memcached operations had errors");
}

#[test]
fn test_status_cache_behavior() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    api_status::clear_cache();

    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());
    let http_port = find_free_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", http_port).parse().unwrap();

    start_http_server(addr, store.clone()).unwrap();
    thread::sleep(Duration::from_millis(100));

    let start = Instant::now();
    let mut cache_hits = 0;
    let mut cache_misses = 0;

    for _ in 0..100 {
        let (status, headers, _) = http_get(addr, "/api/status");
        assert_eq!(status, 200, "Status endpoint should return 200");

        if headers.contains("X-Cache: HIT") {
            cache_hits += 1;
        } else if headers.contains("X-Cache: MISS") {
            cache_misses += 1;
        }
    }

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "100 requests should complete quickly, took {:?}",
        elapsed
    );
    assert!(cache_hits > 0, "Should have cache hits");
    assert!(cache_misses > 0, "Should have at least one cache miss");
}

#[test]
fn test_concurrent_http_endpoints() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    api_status::clear_cache();

    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());
    let http_port = find_free_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", http_port).parse().unwrap();

    start_http_server(addr, store.clone()).unwrap();
    thread::sleep(Duration::from_millis(100));

    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..10 {
        let errors_clone = errors.clone();
        let handle = thread::spawn(move || {
            for _ in 0..20 {
                let (status1, _, _) = http_get(addr, "/");
                let (status2, _, _) = http_get(addr, "/api/status");
                if status1 != 200 || status2 != 200 {
                    errors_clone.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(errors.load(Ordering::SeqCst), 0, "No concurrent requests should fail");
}

#[test]
fn test_status_cache_expiration() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    api_status::clear_cache();

    let opt = get_test_opt();
    let store = Arc::new(Store::new(opt).unwrap());
    let http_port = find_free_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", http_port).parse().unwrap();

    start_http_server(addr, store.clone()).unwrap();
    thread::sleep(Duration::from_millis(100));

    let (_, headers1, _) = http_get(addr, "/api/status");
    assert!(headers1.contains("X-Cache: MISS"), "First request should be a cache miss");

    let (_, headers2, _) = http_get(addr, "/api/status");
    assert!(headers2.contains("X-Cache: HIT"), "Second request should be a cache hit");

    thread::sleep(Duration::from_millis(1100));

    let (_, headers3, _) = http_get(addr, "/api/status");
    assert!(
        headers3.contains("X-Cache: MISS"),
        "Request after cache expiry should be a cache miss"
    );
}
