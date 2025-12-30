#!/usr/bin/env node

/**
 * Static Site Generator for MirDB Homepage
 *
 * This script generates index.html from content.json, separating content from presentation.
 * To update content, edit content.json and run: node build.js
 *
 * NFR-4: Content must be easily maintainable without requiring code changes
 */

const fs = require('fs');
const path = require('path');

const CONTENT_FILE = path.join(__dirname, 'content.json');
const OUTPUT_FILE = path.join(__dirname, 'index.html');

function loadContent() {
  const raw = fs.readFileSync(CONTENT_FILE, 'utf8');
  return JSON.parse(raw);
}

function escapeHtml(text) {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function generateFeatureCards(features) {
  return features.map(f => `
        <div class="feature-card">
          <h3>${escapeHtml(f.title)}</h3>
          <p>${escapeHtml(f.description)}</p>
        </div>`).join('');
}

function generateComparisonRows(rows) {
  return rows.map((row, index) => {
    const mirdbClass = row.mirdb.supported === true ? 'comparison-yes' :
                       row.mirdb.supported === false ? 'comparison-no' : 'comparison-neutral';
    const memcachedClass = row.memcached.supported === true ? 'comparison-yes' :
                           row.memcached.supported === false ? 'comparison-no' : 'comparison-neutral';
    const mirdbIcon = row.mirdb.supported === true ? '<span class="check-icon">&#10003;</span>' :
                      row.mirdb.supported === false ? '<span class="cross-icon">&#10007;</span>' : '';
    const memcachedIcon = row.memcached.supported === true ? '<span class="check-icon">&#10003;</span>' :
                          row.memcached.supported === false ? '<span class="cross-icon">&#10007;</span>' : '';

    const testId = index < 2 ? ` data-testid="comparison-${row.feature.toLowerCase().replace(/\s+/g, '-')}"` : '';

    return `
        <div class="comparison-row feature-row"${testId}>
          <div class="comparison-feature">${escapeHtml(row.feature)}</div>
          <div class="comparison-mirdb ${mirdbClass}">
            ${mirdbIcon}
            ${escapeHtml(row.mirdb.description)}
          </div>
          <div class="comparison-memcached ${memcachedClass}">
            ${memcachedIcon}
            ${escapeHtml(row.memcached.description)}
          </div>
        </div>`;
  }).join('');
}

function generateArchitectureConcepts(concepts) {
  return concepts.map(c => `
          <li><strong>${escapeHtml(c.term)}:</strong> ${escapeHtml(c.description)}</li>`).join('');
}

function generateConfigList(config) {
  return config.map(c => `
        <li><strong>${escapeHtml(c.name)}:</strong> ${escapeHtml(c.value)}</li>`).join('');
}

function generateImplementedFeatures(features) {
  return features.map(f => `
            <li class="status-item implemented" data-testid="feature-implemented">
              <span class="status-badge implemented-badge">Implemented</span>
              ${escapeHtml(f)}
            </li>`).join('');
}

function generatePlannedFeatures(features) {
  return features.map(f => `
            <li class="status-item planned" data-testid="feature-planned">
              <span class="status-badge planned-badge">Planned</span>
              ${escapeHtml(f)}
            </li>`).join('');
}

function generateCommands(commands) {
  return commands.map(c => `
              <li class="command-item" data-testid="command-${c.name.toLowerCase().replace(/_/g, '-')}"><code>${escapeHtml(c.name)}</code> - ${escapeHtml(c.description)}</li>`).join('');
}

function generateFooterLinks(links) {
  return links.map(l => `
        <a href="${escapeHtml(l.href)}" target="_blank" rel="noopener noreferrer">${escapeHtml(l.text)}</a>`).join('');
}

function generateHtml(content) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="description" content="${escapeHtml(content.site.description)}">
  <meta property="og:title" content="${escapeHtml(content.site.title)}">
  <meta property="og:description" content="${escapeHtml(content.site.description)}">
  <meta property="og:image" content="${escapeHtml(content.site.ogImage)}">
  <meta property="og:type" content="website">
  <title>${escapeHtml(content.site.title)}</title>
  <link rel="stylesheet" href="styles.css">
</head>
<body>
  <header class="hero" data-testid="hero">
    <div class="hero-content">
      <h1>${escapeHtml(content.hero.title)}</h1>
      <p class="tagline" data-testid="tagline">${escapeHtml(content.hero.tagline)}</p>
      <p class="hero-description">
        ${escapeHtml(content.hero.description)}
      </p>
      <div class="hero-ctas">
        <a href="${escapeHtml(content.hero.primaryCta.href)}" class="btn btn-primary">${escapeHtml(content.hero.primaryCta.text)}</a>
        <a href="${escapeHtml(content.hero.secondaryCta.href)}" class="btn btn-secondary" target="_blank" rel="noopener noreferrer">${escapeHtml(content.hero.secondaryCta.text)}</a>
      </div>
    </div>
  </header>

  <main>
    <section id="features" class="features">
      <h2>Key Features</h2>
      <div class="feature-grid">${generateFeatureCards(content.features)}
      </div>
    </section>

    <section id="comparison" class="comparison" data-testid="comparison">
      <h2>MirDB vs Standard Memcached</h2>
      <p class="comparison-intro">${escapeHtml(content.comparison.intro)}</p>
      <div class="comparison-table">
        <div class="comparison-row comparison-header">
          <div class="comparison-feature">Feature</div>
          <div class="comparison-mirdb">MirDB</div>
          <div class="comparison-memcached">Memcached</div>
        </div>${generateComparisonRows(content.comparison.rows)}
      </div>
    </section>

    <section id="quick-start" class="quick-start">
      <h2>Quick Start</h2>
      <div class="code-block">
        <pre><code>${escapeHtml(content.quickStart.code)}</code></pre>
      </div>
    </section>

    <section id="architecture" class="architecture" data-testid="architecture">
      <h2>How It Works</h2>
      <p class="architecture-intro">${escapeHtml(content.architecture.intro)}</p>
      <div class="architecture-diagram-container">
        <svg
          class="architecture-diagram"
          data-testid="architecture-diagram"
          role="img"
          aria-label="${escapeHtml(content.architecture.diagramAlt)}"
          viewBox="0 0 600 450"
          xmlns="http://www.w3.org/2000/svg"
        >
          <!-- Background -->
          <rect width="600" height="450" fill="#1a1a2e"/>

          <!-- Title -->
          <text x="300" y="30" text-anchor="middle" fill="#e0e0e0" font-size="16" font-weight="bold">LSM Tree Data Flow</text>

          <!-- Write Request -->
          <rect x="240" y="50" width="120" height="35" rx="5" fill="#4a90d9" stroke="#6ba3e0" stroke-width="2"/>
          <text x="300" y="72" text-anchor="middle" fill="#ffffff" font-size="12" font-weight="bold">Write Request</text>

          <!-- Arrow to WAL -->
          <line x1="300" y1="85" x2="300" y2="105" stroke="#6ba3e0" stroke-width="2" marker-end="url(#arrowhead)"/>

          <!-- WAL Box -->
          <rect x="215" y="110" width="170" height="45" rx="5" fill="#2d5a3d" stroke="#4a8c5e" stroke-width="2"/>
          <text x="300" y="128" text-anchor="middle" fill="#ffffff" font-size="13" font-weight="bold">WAL</text>
          <text x="300" y="145" text-anchor="middle" fill="#b0d4b8" font-size="10">(Write-Ahead Log)</text>

          <!-- Arrow to Memtable -->
          <line x1="300" y1="155" x2="300" y2="175" stroke="#6ba3e0" stroke-width="2" marker-end="url(#arrowhead)"/>

          <!-- Memtable Box -->
          <rect x="215" y="180" width="170" height="45" rx="5" fill="#5a3d6b" stroke="#8a5e9c" stroke-width="2"/>
          <text x="300" y="198" text-anchor="middle" fill="#ffffff" font-size="13" font-weight="bold">Memtable</text>
          <text x="300" y="215" text-anchor="middle" fill="#d4b0e8" font-size="10">(In-memory Skip List)</text>

          <!-- Arrow to Immutable Memtables with label -->
          <line x1="300" y1="225" x2="300" y2="245" stroke="#6ba3e0" stroke-width="2" marker-end="url(#arrowhead)"/>
          <text x="355" y="238" fill="#888" font-size="9" font-style="italic">when full</text>

          <!-- Immutable Memtables Box -->
          <rect x="190" y="250" width="220" height="40" rx="5" fill="#4a4a6a" stroke="#7070a0" stroke-width="2"/>
          <text x="300" y="275" text-anchor="middle" fill="#ffffff" font-size="12" font-weight="bold">Immutable Memtables</text>

          <!-- Arrow to Level 0 with label -->
          <line x1="300" y1="290" x2="300" y2="310" stroke="#6ba3e0" stroke-width="2" marker-end="url(#arrowhead)"/>
          <text x="380" y="303" fill="#888" font-size="9" font-style="italic">minor compaction</text>

          <!-- Level 0 SSTables -->
          <rect x="140" y="315" width="320" height="40" rx="5" fill="#6b4a3d" stroke="#9c6b5e" stroke-width="2"/>
          <text x="300" y="332" text-anchor="middle" fill="#ffffff" font-size="12" font-weight="bold">Level 0 SSTables</text>
          <text x="300" y="347" text-anchor="middle" fill="#e8c4b0" font-size="9">(May overlap, recently flushed)</text>

          <!-- Arrow to Level 1+ with label -->
          <line x1="300" y1="355" x2="300" y2="375" stroke="#6ba3e0" stroke-width="2" marker-end="url(#arrowhead)"/>
          <text x="380" y="368" fill="#888" font-size="9" font-style="italic">major compaction</text>

          <!-- Level 1+ SSTables -->
          <rect x="100" y="380" width="400" height="45" rx="5" fill="#3d5a6b" stroke="#5e8a9c" stroke-width="2"/>
          <text x="300" y="400" text-anchor="middle" fill="#ffffff" font-size="12" font-weight="bold">Level 1+ SSTables</text>
          <text x="300" y="417" text-anchor="middle" fill="#b0d4e8" font-size="9">(Sorted, non-overlapping within level)</text>

          <!-- Arrowhead marker definition -->
          <defs>
            <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
              <polygon points="0 0, 10 3.5, 0 7" fill="#6ba3e0"/>
            </marker>
          </defs>

          <!-- Legend -->
          <rect x="20" y="110" width="12" height="12" fill="#2d5a3d" stroke="#4a8c5e"/>
          <text x="38" y="120" fill="#888" font-size="10">Durability</text>

          <rect x="20" y="130" width="12" height="12" fill="#5a3d6b" stroke="#8a5e9c"/>
          <text x="38" y="140" fill="#888" font-size="10">Memory</text>

          <rect x="20" y="150" width="12" height="12" fill="#6b4a3d" stroke="#9c6b5e"/>
          <text x="38" y="160" fill="#888" font-size="10">Disk (L0)</text>

          <rect x="20" y="170" width="12" height="12" fill="#3d5a6b" stroke="#5e8a9c"/>
          <text x="38" y="180" fill="#888" font-size="10">Disk (L1+)</text>
        </svg>
      </div>
      <div class="architecture-description">
        <h3>Key Concepts</h3>
        <ul class="architecture-concepts">${generateArchitectureConcepts(content.architecture.concepts)}
        </ul>
      </div>
    </section>

    <section id="config" class="configuration">
      <h2>Default Configuration</h2>
      <ul class="config-list">${generateConfigList(content.configuration)}
      </ul>
    </section>

    <section id="project-status" class="project-status" data-testid="project-status">
      <h2>Project Status</h2>
      <div class="status-grid">
        <div class="status-column">
          <h3 class="status-header implemented-header" data-testid="implemented-header">
            <span class="status-icon implemented-icon">&#10003;</span>
            Implemented Features
          </h3>
          <ul class="status-list implemented-list" data-testid="implemented-features">${generateImplementedFeatures(content.projectStatus.implemented)}
          </ul>
          <div class="supported-commands" data-testid="supported-commands">
            <h4>Supported Commands</h4>
            <ul class="command-list">${generateCommands(content.projectStatus.commands)}
            </ul>
          </div>
        </div>
        <div class="status-column">
          <h3 class="status-header planned-header" data-testid="planned-header">
            <span class="status-icon planned-icon">&#9711;</span>
            Planned Features
          </h3>
          <ul class="status-list planned-list" data-testid="planned-features">${generatePlannedFeatures(content.projectStatus.planned)}
          </ul>
        </div>
      </div>
    </section>
  </main>

  <footer class="footer">
    <div class="footer-content">
      <p>&copy; ${escapeHtml(content.site.copyright)}</p>
      <nav class="footer-links">${generateFooterLinks(content.footer.links)}
      </nav>
    </div>
  </footer>
</body>
</html>
`;
}

function main() {
  console.log('Loading content from content.json...');
  const content = loadContent();

  console.log('Generating HTML...');
  const html = generateHtml(content);

  console.log('Writing index.html...');
  fs.writeFileSync(OUTPUT_FILE, html, 'utf8');

  console.log('Build complete!');
}

main();
