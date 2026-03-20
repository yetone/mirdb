//! Input Sanitization Module
//! Owner: Scenario 14 - Security Validation
//!
//! Functions:
//! - sanitize_command(input: &str) -> Result<String, SanitizeError>
//! - is_valid_memcached_command(input: &str) -> bool
//!
//! Security requirements:
//! - Prevent command injection
//! - Only allow valid Memcached commands (SET, GET, DELETE, etc.)
//! - Escape or reject HTML/script tags
//! - Limit command length

/// Maximum allowed command length
const MAX_COMMAND_LENGTH: usize = 1024;

/// Allowed Memcached commands for the demo
const ALLOWED_COMMANDS: [&str; 6] = ["SET", "GET", "DELETE", "STATS", "VERSION", "QUIT"];

/// Sanitize a command input
/// Returns Ok(()) if valid, Err with message if invalid
pub fn sanitize_command(input: &str) -> Result<(), String> {
    // Check length
    if input.len() > MAX_COMMAND_LENGTH {
        return Err("Command too long".to_string());
    }

    // Check for empty input
    if input.trim().is_empty() {
        return Err("Empty command".to_string());
    }

    // Extract the command verb
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.is_empty() {
        return Err("Invalid command format".to_string());
    }

    let cmd = parts[0].to_uppercase();

    // Check if command is allowed
    if !ALLOWED_COMMANDS.contains(&cmd.as_str()) {
        return Err(format!("Unknown command: {}", cmd));
    }

    // Basic XSS prevention - reject HTML/script tags
    if input.contains('<') || input.contains('>') {
        return Err("Invalid characters in command".to_string());
    }

    // Reject potential shell injection characters
    if input.contains(';') || input.contains('|') || input.contains('&') || input.contains('`') {
        return Err("Invalid characters in command".to_string());
    }

    Ok(())
}

/// Check if a command is a valid Memcached command
pub fn is_valid_memcached_command(input: &str) -> bool {
    sanitize_command(input).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_commands() {
        assert!(sanitize_command("SET key 0 0 5").is_ok());
        assert!(sanitize_command("GET key").is_ok());
        assert!(sanitize_command("DELETE key").is_ok());
        assert!(sanitize_command("STATS").is_ok());
        assert!(sanitize_command("VERSION").is_ok());
    }

    #[test]
    fn test_invalid_commands() {
        assert!(sanitize_command("INVALID cmd").is_err());
        assert!(sanitize_command("").is_err());
        assert!(sanitize_command("   ").is_err());
    }

    #[test]
    fn test_xss_prevention() {
        assert!(sanitize_command("SET <script>alert(1)</script> 0 0 5").is_err());
        assert!(sanitize_command("GET <key>").is_err());
    }

    #[test]
    fn test_injection_prevention() {
        assert!(sanitize_command("SET key; rm -rf / 0 0 5").is_err());
        assert!(sanitize_command("GET key | cat /etc/passwd").is_err());
        assert!(sanitize_command("GET key && echo pwned").is_err());
    }

    #[test]
    fn test_command_length() {
        let long_cmd = "SET ".to_owned() + &"x".repeat(2000);
        assert!(sanitize_command(&long_cmd).is_err());
    }
}
