# Product Homepage Design - Product Requirements Document

## Executive Summary

### Problem Statement
MirDB is a powerful persistent key-value store with memcached protocol compatibility, but it currently lacks a dedicated product homepage to effectively communicate its value proposition, features, and technical capabilities to potential users and contributors.

### Proposed Solution
Create a modern, responsive product homepage that showcases MirDB's unique features—persistence, memcached compatibility, and LSM-tree architecture—while providing clear documentation entry points, quick-start guides, and community engagement pathways.

### Expected Impact
- **Increased Adoption**: Clear value proposition helps developers understand when and why to use MirDB
- **Reduced Onboarding Friction**: Quick-start guides and documentation links enable faster getting started
- **Community Growth**: Easy access to contribution guidelines and repository links encourages open-source participation
- **Credibility**: Professional presentation establishes MirDB as a mature, production-ready solution

### Success Metrics
- Homepage bounce rate < 40%
- Time to "Getting Started" click < 30 seconds average
- Documentation page visits from homepage > 50% of visitors
- GitHub stars/forks correlation with homepage launch

---

## Requirements & Scope

### Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| REQ-1 | Display hero section with MirDB logo, tagline, and primary value proposition | Must |
| REQ-2 | Showcase key features: persistence, memcached protocol, LSM-tree architecture | Must |
| REQ-3 | Provide quick-start code snippet demonstrating basic usage | Must |
| REQ-4 | Include call-to-action buttons for documentation, GitHub, and getting started | Must |
| REQ-5 | Display architecture overview diagram showing data flow (WAL → Memtable → SSTable) | Should |
| REQ-6 | Present supported memcached commands (SET, GET, DELETE, etc.) | Should |
| REQ-7 | Show performance characteristics and configuration options | Should |
| REQ-8 | Include project status/roadmap showing completed and planned features | Should |
| REQ-9 | Provide footer with links to license, contributing guidelines, and community | Could |
| REQ-10 | Support responsive design for mobile and tablet devices | Must |

### Non-Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| NFR-1 | Page load time < 3 seconds on 3G connection | Must |
| NFR-2 | Achieve Lighthouse performance score > 90 | Should |
| NFR-3 | Support all modern browsers (Chrome, Firefox, Safari, Edge - last 2 versions) | Must |
| NFR-4 | Meet WCAG 2.1 AA accessibility standards | Should |
| NFR-5 | Static site generation for easy hosting on GitHub Pages | Should |

### Out of Scope
- User authentication or login functionality
- Interactive playground or REPL environment
- Blog or news section
- Multi-language internationalization (English only for initial release)
- Backend API or database integration
- Analytics dashboard (can use third-party like Plausible/Fathom)

### Success Criteria
- All "Must" priority requirements implemented and tested
- Homepage successfully deployed and accessible
- Positive feedback from initial user testing (minimum 5 users)
- No critical accessibility issues identified

---

## User Experience & Interface

### Target Audience
1. **Backend Developers**: Seeking a persistent caching solution with familiar memcached interface
2. **DevOps Engineers**: Evaluating database solutions for infrastructure projects
3. **Open Source Contributors**: Looking for interesting Rust projects to contribute to
4. **Technical Decision Makers**: Comparing key-value store options

### User Journey

```
Landing → Understand Value Prop → Explore Features → View Quick Start → Access Documentation/GitHub
   |              |                      |                  |                     |
   ↓              ↓                      ↓                  ↓                     ↓
Hero Section → Feature Cards → Architecture → Code Snippet → CTA Buttons
```

### Page Structure

1. **Hero Section**
   - MirDB animated logo (existing GIF)
   - Tagline: "A Persistent Key-Value Store with Memcached Protocol"
   - Brief description of core benefits
   - Primary CTA: "Get Started" / "View on GitHub"
   - CI/CD status badge

2. **Key Features Section**
   - Feature cards highlighting:
     - Memcached Protocol Compatibility
     - Persistent Storage (unlike vanilla memcached)
     - LSM-Tree Architecture
     - Write-Ahead Log for Durability
     - Configurable Compaction

3. **Architecture Overview**
   - Visual diagram of data flow
   - Brief explanation of LSM-tree approach
   - Links to detailed documentation

4. **Quick Start Section**
   - Installation command
   - Basic usage example (set/get operations)
   - Connection snippet using standard memcached client

5. **Supported Commands**
   - Grid/table of memcached commands
   - MirDB-specific extensions (INFO, MAJOR_COMPACTION)

6. **Project Status**
   - Completed features checklist
   - Roadmap items (e.g., Raft consensus)

7. **Footer**
   - License information
   - Contributing link
   - Community/support links

### Interface Requirements
- Dark mode support (developer-friendly)
- Syntax highlighting for code snippets
- Copy-to-clipboard functionality for commands
- Smooth scroll navigation
- Mobile-responsive hamburger menu

---

## Technical Considerations

### Technology Stack Options
The homepage should be a static site for simplicity and easy hosting. Recommended approaches:
- **Static Site Generator**: Hugo, Jekyll, or Eleventy
- **Modern Framework**: Astro, Next.js (static export), or SvelteKit
- **Hosting**: GitHub Pages (primary), Netlify, or Vercel

### Content Sources
- README.md for primary copy
- Existing animated logo from assets/logo.gif
- Architecture diagrams created with Mermaid or draw.io
- Code snippets from documentation

### Integration Points
- GitHub API for star count display (optional)
- CircleCI badge integration (existing)

### Performance Considerations
- Lazy load below-fold images
- Minimize JavaScript bundle size
- Use modern image formats (WebP with fallbacks)
- Implement critical CSS inlining

---

## Dependencies & Assumptions

### Dependencies
- GitHub repository access for deployment
- Domain/subdomain decision (e.g., mirdb.io, yetone.github.io/mirdb)
- Design assets (logo already exists)

### Assumptions
- English-only content is acceptable for initial release
- Existing README content can be repurposed for homepage copy
- No backend services required (static hosting)
- Current CI/CD (CircleCI) can be extended for homepage deployment

---

## Appendices

### A. MirDB Feature Summary (from knowledge base)

**Core Capabilities:**
- Memcached text protocol compatibility
- Persistent storage via SSTables
- LSM-tree architecture with 7-level compaction
- Write-ahead logging for crash recovery
- Skip list memtable implementation
- Snappy compression support
- Cuckoo filter for efficient lookups

**Default Configuration:**
- Listen address: `0.0.0.0:12333`
- Memtable max size: 4MB
- SSTable max size: 100MB
- Block size: 4KB
- Max LSM levels: 7

**Supported Commands:**
- Storage: SET, ADD, REPLACE, APPEND, PREPEND
- Retrieval: GET, GETS
- Deletion: DELETE
- MirDB-specific: INFO, MAJOR_COMPACTION

### B. Architecture Diagram (for reference)

```
Write Request
    │
    ▼
┌─────────┐
│   WAL   │──── Write-Ahead Log for durability
└────┬────┘
     │
     ▼
┌──────────┐
│ Memtable │──── Active in-memory skip list
└────┬─────┘
     │ (when full)
     ▼
┌────────────────┐
│ Imm Memtables  │──── Immutable memtables queue
└───────┬────────┘
        │ (minor compaction)
        ▼
┌───────────────┐
│ Level 0 SSTs  │──── Recently flushed, may overlap
└───────┬───────┘
        │ (major compaction)
        ▼
┌───────────────┐
│ Level 1+ SSTs │──── Sorted, non-overlapping within level
└───────────────┘
```

### C. Competitive Positioning

| Feature | MirDB | Memcached | Redis |
|---------|-------|-----------|-------|
| Persistence | ✅ | ❌ | ✅ |
| Memcached Protocol | ✅ | ✅ | ❌ |
| In-Memory Speed | ✅ | ✅ | ✅ |
| LSM-Tree Storage | ✅ | ❌ | ❌ |
| Written in Rust | ✅ | ❌ (C) | ❌ (C) |
