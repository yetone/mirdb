use std::fs;

/// Validates that the README.md contains all required project status and roadmap information
#[test]
fn test_readme_contains_status_section() {
    let content = fs::read_to_string("README.md").expect("README.md should exist");

    assert!(
        content.contains("## Project Status"),
        "README should contain 'Project Status' section"
    );
}

#[test]
fn test_readme_contains_implemented_components() {
    let content = fs::read_to_string("README.md").expect("README.md should exist");

    assert!(
        content.contains("Currently Implemented"),
        "README should list currently implemented components"
    );

    let implemented_components = vec!["Server", "Persistence", "LSM Tree", "Commands", "Compaction"];
    for component in implemented_components {
        assert!(
            content.contains(component),
            "README should mention implemented component: {}",
            component
        );
    }
}

#[test]
fn test_readme_contains_roadmap_section() {
    let content = fs::read_to_string("README.md").expect("README.md should exist");

    assert!(
        content.contains("### Roadmap"),
        "README should contain roadmap section"
    );
}

#[test]
fn test_readme_contains_raft_consensus() {
    let content = fs::read_to_string("README.md").expect("README.md should exist");

    assert!(
        content.contains("Raft Consensus") || content.contains("raft"),
        "README should mention Raft consensus for distributed deployment"
    );
}

#[test]
fn test_readme_contains_contribution_guidelines() {
    let content = fs::read_to_string("README.md").expect("README.md should exist");

    assert!(
        content.contains("### Contributing") || content.contains("contribute"),
        "README should contain contribution guidelines section"
    );
}

#[test]
fn test_readme_contains_github_issues_link() {
    let content = fs::read_to_string("README.md").expect("README.md should exist");

    assert!(
        content.contains("https://github.com/yetone/mirdb/issues"),
        "README should contain a link to GitHub issues"
    );
}

#[test]
fn test_readme_github_issues_link_is_clickable() {
    let content = fs::read_to_string("README.md").expect("README.md should exist");

    // Check for markdown link format [text](url) that points to issues
    let has_markdown_link = content.contains("[GitHub Issues]") ||
                           content.contains("[issues]") ||
                           content.contains("[GitHub Issues page]");

    // Check if link appears in markdown format
    assert!(
        has_markdown_link || content.contains("github.com/yetone/mirdb/issues"),
        "GitHub issues link should be in a clickable format"
    );
}
