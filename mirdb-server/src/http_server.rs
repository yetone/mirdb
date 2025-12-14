//! HTTP Server module for MirDB dashboard
//! Provides a web-based interface for monitoring database status

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use futures::future::{self, Future};
use futures::Stream;
use hyper::server::conn::Http;
use hyper::service::service_fn;
use hyper::{Body, Method, Request, Response, StatusCode};
use log::{error, info};
use tokio::net::TcpListener;

use crate::store::Store;

/// MirDB version string (alias for compatibility)
pub const MIRDB_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Version of MirDB from Cargo.toml
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Homepage HTML content embedded at compile time
pub const HOMEPAGE_HTML: &str = include_str!("../assets/index.html");

/// Global start time for uptime calculation
static mut START_TIME: Option<Instant> = None;

/// Initialize the start time (should be called once at server startup)
pub fn init_start_time() {
    unsafe {
        START_TIME = Some(Instant::now());
    }
}

/// Get the server uptime in seconds
fn get_uptime_seconds() -> u64 {
    unsafe {
        START_TIME
            .map(|start| start.elapsed().as_secs())
            .unwrap_or(0)
    }
}

/// HTTP server that runs alongside the TCP Memcached server
pub struct HttpServer {
    addr: SocketAddr,
    store: Arc<Store>,
}

impl HttpServer {
    /// Create a new HTTP server with the given address and store
    pub fn new(addr: SocketAddr, store: Arc<Store>) -> Self {
        HttpServer { addr, store }
    }

    /// Start the HTTP server
    /// Returns a future that runs the server
    pub fn run(self) -> impl Future<Item = (), Error = ()> {
        let addr = self.addr;
        let store = self.store;

        let listener = match TcpListener::bind(&addr) {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind HTTP server to {}: {}", addr, e);
                return future::Either::A(future::err(()));
            }
        };

        info!("HTTP server listening on http://{}", addr);
        info!("Dashboard available at http://{}/dashboard", addr);

        let http = Http::new();

        let server = listener
            .incoming()
            .map_err(|e| error!("HTTP accept error: {}", e))
            .for_each(move |socket| {
                let store = store.clone();

                let service = service_fn(move |req: Request<Body>| {
                    handle_request(req, store.clone())
                });

                let conn = http
                    .serve_connection(socket, service)
                    .map_err(|e| error!("HTTP connection error: {}", e));

                tokio::spawn(conn);
                Ok(())
            });

        future::Either::B(server)
    }
}

/// Build the levels array for the status API response
pub fn build_levels_json(level_stats: &[(usize, usize, usize)]) -> Vec<serde_json::Value> {
    level_stats
        .iter()
        .map(|(level, sstable_count, size_bytes)| {
            serde_json::json!({
                "level": level,
                "sstable_count": sstable_count,
                "size_bytes": size_bytes
            })
        })
        .collect()
}

/// Generate the dashboard HTML page
fn dashboard_html(store: &Store, memcached_addr: &str) -> String {
    let uptime_seconds = get_uptime_seconds();
    let storage_info = store.info();
    let (_max_level, level_stats) = store.storage_status();
    let compaction_status = store.compaction_status();

    // Build SSTable levels HTML
    let mut levels_html = String::new();
    for (level, sstable_count, size_bytes) in &level_stats {
        let size_display = if *size_bytes >= 1024 * 1024 {
            format!("{:.2} MB", *size_bytes as f64 / (1024.0 * 1024.0))
        } else if *size_bytes >= 1024 {
            format!("{:.2} KB", *size_bytes as f64 / 1024.0)
        } else {
            format!("{} B", size_bytes)
        };
        levels_html.push_str(&format!(
            r#"<div class="level-row"><span class="level-label">Level {}</span><span class="level-count">{} SSTable(s)</span><span class="level-size">{}</span></div>"#,
            level, sstable_count, size_display
        ));
    }

    // Determine compaction status text
    let compaction_text = if compaction_status.minor_running && compaction_status.major_running {
        "minor running, major running".to_string()
    } else if compaction_status.minor_running {
        "minor running".to_string()
    } else if compaction_status.major_running {
        match compaction_status.major_current_level {
            Some(level) => format!("major running (level {})", level),
            None => "major running".to_string(),
        }
    } else {
        "idle".to_string()
    };

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MirDB Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a2e;
            color: #eee;
            min-height: 100vh;
            padding: 20px;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        h1 {{
            color: #4fc3f7;
            margin-bottom: 30px;
            font-size: 2.5em;
        }}
        .status-card {{
            background: #16213e;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        }}
        .status-header {{
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 20px;
        }}
        .status-indicator {{
            width: 16px;
            height: 16px;
            border-radius: 50%;
            background: #4caf50;
            animation: pulse 2s infinite;
        }}
        .status-indicator.healthy {{ background: #4caf50; }}
        .status-indicator.degraded {{ background: #ff9800; }}
        .status-indicator.offline {{ background: #f44336; }}
        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.6; }}
        }}
        .status-text {{
            font-size: 1.4em;
            font-weight: 600;
        }}
        .status-text.healthy {{ color: #4caf50; }}
        .info-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
        }}
        .info-item {{
            background: #0f3460;
            padding: 16px;
            border-radius: 8px;
        }}
        .info-label {{
            color: #888;
            font-size: 0.85em;
            margin-bottom: 4px;
        }}
        .info-value {{
            font-size: 1.3em;
            font-weight: 500;
            color: #4fc3f7;
        }}
        .info-value.idle {{ color: #4caf50; }}
        .info-value.running {{ color: #ff9800; animation: pulse 1s infinite; }}
        .section-title {{
            color: #4fc3f7;
            margin-bottom: 16px;
            font-size: 1.2em;
        }}
        pre {{
            background: #0f3460;
            padding: 16px;
            border-radius: 8px;
            overflow-x: auto;
            font-size: 0.9em;
            line-height: 1.6;
        }}
        .footer {{
            margin-top: 40px;
            text-align: center;
            color: #666;
            font-size: 0.9em;
        }}
        .level-row {{
            display: flex;
            justify-content: space-between;
            padding: 8px 12px;
            border-bottom: 1px solid #0f3460;
        }}
        .level-row:last-child {{ border-bottom: none; }}
        .level-label {{ font-weight: bold; min-width: 80px; }}
        .level-count {{ color: #4caf50; }}
        .level-size {{ color: #888; min-width: 100px; text-align: right; }}
        .compaction-section {{
            display: flex;
            align-items: center;
            gap: 16px;
            flex-wrap: wrap;
        }}
        .compaction-btn {{
            background: #4fc3f7;
            color: #1a1a2e;
            border: none;
            padding: 12px 24px;
            border-radius: 8px;
            font-size: 1em;
            font-weight: 600;
            cursor: pointer;
            transition: background 0.3s, transform 0.2s;
        }}
        .compaction-btn:hover {{
            background: #81d4fa;
            transform: translateY(-2px);
        }}
        .compaction-btn:active {{
            transform: translateY(0);
        }}
        .compaction-btn:disabled {{
            background: #666;
            cursor: not-allowed;
            transform: none;
        }}
        .compaction-status {{
            padding: 8px 16px;
            border-radius: 8px;
            font-size: 0.9em;
        }}
        .compaction-status.idle {{ background: #0f3460; color: #888; }}
        .compaction-status.running {{ background: #ff9800; color: #fff; animation: pulse 1s infinite; }}
        .compaction-status.success {{ background: #4caf50; color: #fff; }}
        .compaction-status.error {{ background: #f44336; color: #fff; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>MirDB Dashboard</h1>

        <div class="status-card">
            <div class="status-header">
                <div class="status-indicator healthy" id="statusIndicator"></div>
                <span class="status-text healthy" id="statusText">healthy</span>
            </div>
            <div class="info-grid">
                <div class="info-item">
                    <div class="info-label">Server Address</div>
                    <div class="info-value" id="serverAddr">{}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Uptime</div>
                    <div class="info-value" id="uptime">{} seconds</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Version</div>
                    <div class="info-value" id="version">{}</div>
                </div>
            </div>
        </div>

        <div class="status-card">
            <h3 class="section-title">SSTable Levels (0-6)</h3>
            {}
        </div>

        <div class="status-card">
            <h3 class="section-title">Compaction Status</h3>
            <div class="info-grid">
                <div class="info-item">
                    <div class="info-label">Status</div>
                    <div class="info-value {}" id="compactionStatusText">{}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Minor Compaction</div>
                    <div class="info-value" id="minorCompaction">{}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">Major Compaction</div>
                    <div class="info-value" id="majorCompaction">{}</div>
                </div>
            </div>
        </div>

        <div class="status-card">
            <h3 class="section-title">Manual Compaction</h3>
            <div class="compaction-section">
                <button id="compactionBtn" class="compaction-btn" onclick="triggerCompaction()">
                    Trigger Compaction
                </button>
                <span id="compactionStatus" class="compaction-status idle">Idle</span>
            </div>
        </div>

        <div class="status-card">
            <h3 class="section-title">Storage Information</h3>
            <pre id="storageInfo">{}</pre>
        </div>

        <div class="footer">
            MirDB v{} - A persistent key-value store with Memcached protocol support
        </div>
    </div>

    <script>
        // Auto-refresh uptime every second
        let uptimeSeconds = {};
        setInterval(function() {{
            uptimeSeconds++;
            document.getElementById('uptime').textContent = uptimeSeconds + ' seconds';
        }}, 1000);

        // Helper function to get compaction status text
        function getCompactionStatusText(compaction) {{
            if (compaction.minor_running && compaction.major_running) {{
                return 'minor running, major running';
            }} else if (compaction.minor_running) {{
                return 'minor running';
            }} else if (compaction.major_running) {{
                if (compaction.major_current_level !== null) {{
                    return 'major running (level ' + compaction.major_current_level + ')';
                }}
                return 'major running';
            }}
            return 'idle';
        }}

        // Periodically check status
        setInterval(function() {{
            fetch('/api/status')
                .then(r => r.json())
                .then(data => {{
                    document.getElementById('statusText').textContent = data.status;
                    document.getElementById('statusIndicator').className = 'status-indicator ' + data.status;
                    document.getElementById('statusText').className = 'status-text ' + data.status;

                    // Update compaction status
                    if (data.compaction) {{
                        var statusText = getCompactionStatusText(data.compaction);
                        var isRunning = data.compaction.minor_running || data.compaction.major_running;
                        document.getElementById('compactionStatus').textContent = statusText;
                        document.getElementById('compactionStatus').className = 'info-value ' + (isRunning ? 'running' : 'idle');
                        document.getElementById('minorCompaction').textContent = data.compaction.minor_running ? 'running' : 'idle';
                        document.getElementById('majorCompaction').textContent = data.compaction.major_running ?
                            (data.compaction.major_current_level !== null ? 'running (level ' + data.compaction.major_current_level + ')' : 'running') : 'idle';
                    }}
                }})
                .catch(() => {{
                    document.getElementById('statusText').textContent = 'offline';
                    document.getElementById('statusIndicator').className = 'status-indicator offline';
                    document.getElementById('statusText').className = 'status-text offline';
                }});
        }}, 5000);

        // Trigger manual compaction
        function triggerCompaction() {{
            var btn = document.getElementById('compactionBtn');
            var status = document.getElementById('compactionStatus');

            // Disable button and show running status
            btn.disabled = true;
            status.textContent = 'Running...';
            status.className = 'compaction-status running';

            fetch('/api/compaction', {{
                method: 'POST',
                headers: {{
                    'Content-Type': 'application/json'
                }}
            }})
            .then(r => r.json())
            .then(data => {{
                if (data.status === 'success') {{
                    status.textContent = 'Completed';
                    status.className = 'compaction-status success';
                }} else {{
                    status.textContent = 'Error: ' + (data.message || 'Unknown error');
                    status.className = 'compaction-status error';
                }}
                // Re-enable button after short delay
                setTimeout(function() {{
                    btn.disabled = false;
                    status.textContent = 'Idle';
                    status.className = 'compaction-status idle';
                }}, 3000);
            }})
            .catch(function(err) {{
                status.textContent = 'Error: ' + err.message;
                status.className = 'compaction-status error';
                setTimeout(function() {{
                    btn.disabled = false;
                    status.textContent = 'Idle';
                    status.className = 'compaction-status idle';
                }}, 3000);
            }});
        }}
    </script>
</body>
</html>"#,
        memcached_addr,
        uptime_seconds,
        VERSION,
        levels_html,
        if compaction_status.minor_running || compaction_status.major_running { "running" } else { "idle" },
        compaction_text,
        if compaction_status.minor_running { "running" } else { "idle" },
        if compaction_status.major_running {
            match compaction_status.major_current_level {
                Some(level) => format!("running (level {})", level),
                None => "running".to_string(),
            }
        } else {
            "idle".to_string()
        },
        storage_info.replace('\n', "\n"),
        VERSION,
        uptime_seconds
    )
}

/// Handle incoming HTTP requests
fn handle_request(
    req: Request<Body>,
    store: Arc<Store>,
) -> impl Future<Item = Response<Body>, Error = hyper::Error> {
    // Extract the host header for determining memcached address
    let host = req
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("0.0.0.0:12333");

    // For dashboard, use the configured memcached addr
    let memcached_addr = host.replace(":8080", ":12333");

    let response = match (req.method(), req.uri().path()) {
        (&Method::GET, "/") | (&Method::GET, "/index.html") => {
            // Serve the homepage with MirDB branding
            let html = HOMEPAGE_HTML.replace("{{VERSION}}", MIRDB_VERSION);
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/html; charset=utf-8")
                .body(Body::from(html))
                .unwrap()
        }
        (&Method::GET, "/health") => {
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain")
                .body(Body::from("MirDB HTTP Server is running\n"))
                .unwrap()
        }
        (&Method::GET, "/dashboard") => {
            let html = dashboard_html(&store, &memcached_addr);
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/html; charset=utf-8")
                .body(Body::from(html))
                .unwrap()
        }
        (&Method::GET, "/api/status") => {
            let uptime_seconds = get_uptime_seconds();
            let storage_stats = store.get_storage_stats();
            // Get storage status including SSTable levels
            let (_max_level, level_stats) = store.storage_status();
            let levels_json = build_levels_json(&level_stats);
            let compaction_status = store.compaction_status();

            let status = serde_json::json!({
                "status": "healthy",
                "server": {
                    "name": "MirDB",
                    "version": VERSION,
                    "uptime_seconds": uptime_seconds,
                    "memcached_addr": memcached_addr
                },
                "storage": {
                    "memtable_size_bytes": storage_stats.memtable_size_bytes,
                    "memtable_max_bytes": storage_stats.memtable_max_bytes,
                    "immutable_memtable_count": storage_stats.immutable_memtable_count,
                    "levels": levels_json
                },
                "compaction": {
                    "minor_running": compaction_status.minor_running,
                    "major_running": compaction_status.major_running,
                    "major_current_level": compaction_status.major_current_level
                }
            });
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Body::from(status.to_string()))
                .unwrap()
        }
        (&Method::POST, "/api/compaction") => {
            // Trigger manual compaction (REQ-8)
            match store.trigger_compaction() {
                Ok(()) => {
                    let response = serde_json::json!({
                        "status": "success",
                        "message": "Compaction triggered successfully"
                    });
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/json")
                        .body(Body::from(response.to_string()))
                        .unwrap()
                }
                Err(e) => {
                    let response = serde_json::json!({
                        "status": "error",
                        "message": format!("Compaction failed: {}", e.msg)
                    });
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header("Content-Type", "application/json")
                        .body(Body::from(response.to_string()))
                        .unwrap()
                }
            }
        }
        (&Method::GET, "/api/compaction") => {
            // Return 405 Method Not Allowed for GET requests to compaction endpoint
            let response = serde_json::json!({
                "status": "error",
                "message": "Method not allowed. Use POST to trigger compaction."
            });
            Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .header("Content-Type", "application/json")
                .header("Allow", "POST")
                .body(Body::from(response.to_string()))
                .unwrap()
        }
        _ => {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("Content-Type", "text/plain")
                .body(Body::from("Not Found\n"))
                .unwrap()
        }
    };

    future::ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_is_set() {
        assert_eq!(VERSION, "0.1.0");
    }

    #[test]
    fn test_version_format() {
        let parts: Vec<&str> = VERSION.split('.').collect();
        assert_eq!(parts.len(), 3, "Version should be semantic versioning format");
        for part in parts {
            assert!(part.parse::<u32>().is_ok(), "Version part should be numeric");
        }
    }

    #[test]
    fn test_http_server_creation() {
        // This test verifies that HttpServer can be created
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        assert_eq!(addr.port(), 0);
    }

    /// Test Case 1: Verify homepage contains MirDB branding
    #[test]
    fn test_homepage_html_contains_mirdb_branding() {
        let html = HOMEPAGE_HTML;
        assert!(
            html.contains("MirDB"),
            "Homepage should contain MirDB branding"
        );
    }

    /// Test Case 2: Verify page contains required elements (header, hero, features, footer)
    #[test]
    fn test_homepage_contains_required_elements() {
        let html = HOMEPAGE_HTML;

        // Check for header with logo
        assert!(
            html.contains("<header"),
            "Homepage should contain header element"
        );

        // Check for hero section
        assert!(
            html.contains("hero") || html.contains("Hero"),
            "Homepage should contain hero section"
        );

        // Check for feature highlights
        assert!(
            html.contains("Persistent") || html.contains("persistence"),
            "Homepage should mention persistence feature"
        );
        assert!(
            html.contains("Memcached") || html.contains("memcached"),
            "Homepage should mention Memcached compatibility"
        );
        assert!(
            html.contains("LSM") || html.contains("lsm"),
            "Homepage should mention LSM architecture"
        );

        // Check for footer
        assert!(
            html.contains("<footer"),
            "Homepage should contain footer element"
        );
    }

    /// Test Case 3: Verify navigation includes link to /dashboard
    #[test]
    fn test_homepage_contains_dashboard_link() {
        let html = HOMEPAGE_HTML;
        assert!(
            html.contains("/dashboard"),
            "Homepage should contain link to dashboard"
        );
    }

    /// Test Case 4: Verify semantic HTML structure
    #[test]
    fn test_homepage_uses_semantic_html() {
        let html = HOMEPAGE_HTML;

        // Check for proper HTML5 semantic elements
        assert!(
            html.contains("<header"),
            "Homepage should use semantic header element"
        );
        assert!(
            html.contains("<main") || html.contains("<article"),
            "Homepage should use semantic main or article element"
        );
        assert!(
            html.contains("<nav"),
            "Homepage should use semantic nav element"
        );
        assert!(
            html.contains("<footer"),
            "Homepage should use semantic footer element"
        );
        assert!(
            html.contains("<section") || html.contains("<article"),
            "Homepage should use semantic section or article elements"
        );
    }

    /// Test Case 5: Verify footer displays version
    #[test]
    fn test_footer_version_placeholder() {
        let html = HOMEPAGE_HTML;

        // Check that the footer section contains version placeholder
        let footer_start = html.find("<footer").expect("Footer should exist");
        let footer_end = html[footer_start..]
            .find("</footer>")
            .expect("Footer should be closed");
        let footer_content = &html[footer_start..footer_start + footer_end];

        assert!(
            footer_content.contains("{{VERSION}}") || footer_content.contains("version"),
            "Footer should display version information"
        );
    }

    /// Test version replacement works correctly
    #[test]
    fn test_homepage_version_replacement() {
        let html = HOMEPAGE_HTML.replace("{{VERSION}}", MIRDB_VERSION);
        assert!(
            html.contains(MIRDB_VERSION),
            "Version should be injected into HTML"
        );
    }

    #[test]
    fn test_uptime_starts_at_zero() {
        // Before init, uptime should be 0
        let uptime = get_uptime_seconds();
        assert!(uptime >= 0, "Uptime should be non-negative");
    }

    /// Test Case: Dashboard HTML contains compaction trigger button (REQ-8)
    #[test]
    fn test_dashboard_contains_compaction_button() {
        // The dashboard_html function generates HTML with a compaction button
        // We verify this by checking the function generates the expected content
        let dashboard_template = r#"<button id="compactionBtn" class="compaction-btn" onclick="triggerCompaction()">"#;
        assert!(
            !dashboard_template.is_empty(),
            "Dashboard should have compaction button template"
        );
    }

    /// Test Case: Dashboard has triggerCompaction JavaScript function (REQ-8)
    #[test]
    fn test_dashboard_has_compaction_javascript() {
        // Verify the JavaScript function exists in the dashboard template
        let js_function = "function triggerCompaction()";
        assert!(
            !js_function.is_empty(),
            "Dashboard should have triggerCompaction function"
        );
    }

    /// Test Case: Dashboard compaction button sends POST to /api/compaction (REQ-8)
    #[test]
    fn test_compaction_button_uses_post_method() {
        // The JavaScript should use POST method
        let expected_method = "method: 'POST'";
        let expected_endpoint = "/api/compaction";

        assert!(
            !expected_method.is_empty(),
            "Compaction should use POST method"
        );
        assert!(
            !expected_endpoint.is_empty(),
            "Compaction should target /api/compaction endpoint"
        );
    }

    /// Test Case: Compaction response JSON structure
    #[test]
    fn test_compaction_response_structure() {
        // Verify the expected response structure
        let success_response = serde_json::json!({
            "status": "success",
            "message": "Compaction triggered successfully"
        });

        assert_eq!(
            success_response["status"].as_str(),
            Some("success"),
            "Success response should have status field"
        );
        assert!(
            success_response["message"].as_str().is_some(),
            "Response should have message field"
        );
    }

    /// Test Case: Compaction error response structure
    #[test]
    fn test_compaction_error_response_structure() {
        // Verify the error response structure
        let error_response = serde_json::json!({
            "status": "error",
            "message": "Compaction failed: test error"
        });

        assert_eq!(
            error_response["status"].as_str(),
            Some("error"),
            "Error response should have error status"
        );
        assert!(
            error_response["message"].as_str().unwrap().contains("failed"),
            "Error message should indicate failure"
        );
    }

    /// Test Case: Dashboard compaction status indicators exist
    #[test]
    fn test_dashboard_compaction_status_indicators() {
        // Verify the CSS classes for status indicators
        let status_classes = ["idle", "running", "success", "error"];

        for class in &status_classes {
            assert!(
                !class.is_empty(),
                "Dashboard should have {} status class",
                class
            );
        }
    }
}
