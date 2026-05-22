/**
 * Web server module for serving the homepage and static assets.
 * Owner: Scenario 1 - HTTP Server and Routing (base structure)
 *          Scenario 12 - Analytics Counter Display (stats endpoint)
 *
 * Expected exports:
 * - start_server(addr, store) -> Result<()>: Start the HTTP server
 * - routes() -> Router: Define all application routes
 *
 * This module wraps the existing TCP server with HTTP capabilities
 * or runs a separate HTTP server alongside the TCP protocol server.
 */

pub mod handlers;

pub use handlers::*;
