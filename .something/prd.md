# Product Homepage Design - Product Requirements Document

## Executive Summary

### Problem Statement
MirDB is a high-performance persistent key-value store with Memcached protocol compatibility, built in Rust. Despite its powerful features including LSM-tree storage, skip list memtables, and automatic compaction, the project lacks a dedicated homepage to showcase its capabilities, attract developers, and facilitate adoption.

### Proposed Solution
Create a compelling product homepage for MirDB that effectively communicates the product's value proposition, technical capabilities, and ease of use. The homepage will serve as the primary entry point for developers discovering MirDB and will provide clear paths to getting started, documentation, and community engagement.

### Expected Impact
- **Increased Adoption**: Clear value proposition and easy onboarding will attract more developers
- **Reduced Friction**: Quick start guides and visual demonstrations will lower the barrier to entry
- **Community Growth**: Prominent links to GitHub and community resources will foster engagement
- **Brand Recognition**: Professional presentation will establish MirDB as a credible database solution

### Success Metrics
- Homepage bounce rate below 40%
- Click-through rate to documentation/GitHub above 25%
- Time to first interaction (e.g., copying install command) under 30 seconds
- Mobile responsiveness score above 90 on Google PageSpeed Insights

---

## Requirements & Scope

### Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| REQ-1 | Display hero section with MirDB logo, tagline, and primary call-to-action | Must |
| REQ-2 | Show animated usage demonstration (existing usage.gif asset) | Must |
| REQ-3 | Present key features section highlighting core capabilities | Must |
| REQ-4 | Provide quick start installation commands with copy functionality | Must |
| REQ-5 | Include navigation to GitHub repository | Must |
| REQ-6 | Display current project status and CI badge | Should |
| REQ-7 | Show feature roadmap (completed and planned items from TODO) | Should |
| REQ-8 | Provide code examples demonstrating basic operations | Should |
| REQ-9 | Include footer with author attribution and links | Should |
| REQ-10 | Support dark/light theme toggle | Could |

### Non-Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| NFR-1 | Page load time under 3 seconds on 3G connection | Must |
| NFR-2 | Fully responsive across desktop, tablet, and mobile devices | Must |
| NFR-3 | Accessible to WCAG 2.1 AA standards | Must |
| NFR-4 | SEO-optimized with appropriate meta tags and semantic HTML | Should |
| NFR-5 | Cross-browser compatibility (Chrome, Firefox, Safari, Edge) | Must |
| NFR-6 | Minimal external dependencies for fast loading | Should |

### Out of Scope
- User authentication or account management
- Interactive database playground or REPL
- Documentation hosting (separate concern)
- Blog or news section
- Multi-language internationalization (initial release)
- Backend API or server-side rendering

### Success Criteria
- All "Must" priority requirements implemented and functional
- Passes Google Lighthouse performance audit with score > 80
- Renders correctly on all major browsers and devices
- GitHub link is prominent and easily discoverable
- Users can copy installation command within one click

---

## User Experience & Interface

### User Journey

```
Discovery → Landing → Understanding → Evaluation → Action
    |           |           |             |          |
    v           v           v             v          v
Search/    Hero with   Features &   Demo GIF &   Install
Referral   tagline     benefits     examples     or GitHub
```

### Target Users
1. **Backend Developers** seeking a fast, persistent key-value store
2. **DevOps Engineers** looking for Memcached-compatible alternatives
3. **Rust Enthusiasts** interested in database implementations
4. **System Architects** evaluating storage solutions

### Interface Requirements

#### Hero Section
- MirDB logo prominently displayed (using existing logo.gif)
- Tagline: "A Persistent Key-Value Store with Memcached Protocol"
- Subtitle emphasizing: "Built in Rust for speed and reliability"
- Primary CTA: "Get Started" (links to quick start section)
- Secondary CTA: "View on GitHub"

#### Key Features Section
- **Memcached Protocol**: Drop-in replacement with familiar API
- **Persistent Storage**: LSM-tree based durability
- **High Performance**: Skip list memtables for fast operations
- **Automatic Compaction**: Minor and major compaction support
- **Configurable**: Flexible TOML-based configuration
- **Async I/O**: Built on Tokio for efficient networking

#### Usage Demonstration
- Embed existing usage.gif showing terminal interaction
- Caption explaining the demonstration
- Optional: animated code snippets for key operations (get, set, delete)

#### Quick Start Section
```bash
# Clone the repository
git clone https://github.com/yetone/mirdb

# Build with Cargo
cd mirdb && cargo build --release

# Run the server
./target/release/mirdb-server -c etc/mirdb.toml
```

#### Operations Examples
```
# Set a value
set mykey 0 3600 5
hello

# Get a value
get mykey

# Delete a value
delete mykey
```

#### Roadmap Section
- Completed: Tokio with memcached protocol, memtable with skiplist, minor compaction, major compaction
- Planned: Raft consensus support

#### Footer
- Author: yetone (yetoneful@gmail.com)
- GitHub link
- CircleCI build status badge

### Accessibility Considerations
- All images must have descriptive alt text
- Color contrast ratio minimum 4.5:1 for text
- Keyboard navigation support for all interactive elements
- Skip links for screen reader users
- Reduced motion option for animated content

---

## Technical Considerations

### High-Level Technical Approach
The homepage will be built as a static site to maximize performance, minimize hosting complexity, and align with the project's developer-focused audience. Static hosting enables deployment via GitHub Pages without additional infrastructure.

### Integration Points
- **GitHub Repository**: Links to https://github.com/yetone/mirdb
- **CircleCI**: Badge integration for build status
- **Static Assets**: Existing logo.gif and usage.gif from /assets directory

### Key Technical Constraints
- Must work without JavaScript for core content (progressive enhancement)
- Should minimize external font and script dependencies
- Must be deployable to GitHub Pages or similar static hosting
- Image assets (logo.gif, usage.gif) are ~8.5MB combined; lazy loading required

### Performance Considerations
- Implement lazy loading for GIF animations
- Use modern image formats (WebP) with GIF fallbacks
- Minimize CSS and inline critical styles
- Consider converting large GIFs to video format for better compression

---

## Dependencies & Assumptions

### Dependencies
- **GitHub Pages**: For static site hosting (or equivalent)
- **Existing Assets**: logo.gif and usage.gif in /assets directory
- **CircleCI Integration**: For build badge display

### Assumptions
- The homepage will be hosted under the MirDB GitHub repository or organization
- Developers visiting the page are familiar with command-line tools
- The Memcached protocol is a known concept to the target audience
- Users have Rust/Cargo installed or will install it for building from source

### Cross-Team Coordination
- None required - single maintainer project (yetone)

---

## Appendices

### A. Existing Brand Assets
| Asset | Location | Size |
|-------|----------|------|
| Logo | /assets/logo.gif | 2.5 MB |
| Usage Demo | /assets/usage.gif | 6.0 MB |

### B. Configuration Reference
Default server configuration (etc/mirdb.toml):
```toml
addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdb"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500
```

### C. Supported Memcached Operations
Based on codebase analysis:
- **Getters**: get, gets
- **Setters**: set, add, replace, append, prepend
- **Deleters**: delete
- **Management**: info, major_compaction

### D. ASCII Art Logo (for terminal display)
```
  __  __ _     ___  ___
 |  \/  (_)_ _|   \| _ )
 | |\/| | | '_| |) | _ \
 |_|  |_|_|_| |___/|___/
```
