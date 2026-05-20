/**
 * Roadmap Section Unit Tests
 * Owner: Scenario 5 - Status & Roadmap Section
 *
 * Tests:
 * - CI badge element and link
 * - Completed features checklist
 * - Planned features with "Coming Soon" labels
 * - Badge fallback for image load failure
 * - CSS styling for roadmap section
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.resolve(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

function extractSection(html, sectionId) {
  const regex = new RegExp(`<section id="${sectionId}"[^>]*>(.*?)</section>`, 's');
  const match = html.match(regex);
  return match ? match[1] : '';
}

describe('Roadmap Section - Structure', () => {
  let sectionHtml;

  beforeAll(() => {
    sectionHtml = extractSection(htmlContent, 'roadmap');
  });

  test('roadmap section exists in HTML', () => {
    expect(sectionHtml).not.toBe('');
  });

  test('section has a title', () => {
    expect(sectionHtml).toContain('<h2 class="section-title">');
    expect(sectionHtml).toContain('Status &amp; Roadmap');
  });

  test('section is wrapped in a container', () => {
    expect(sectionHtml).toContain('class="container"');
  });

  test('section has roadmap-content wrapper', () => {
    expect(sectionHtml).toContain('class="roadmap-content"');
  });
});

describe('Roadmap Section - CI Badge', () => {
  let sectionHtml;

  beforeAll(() => {
    sectionHtml = extractSection(htmlContent, 'roadmap');
  });

  test('CI badge link exists and points to CircleCI', () => {
    expect(sectionHtml).toContain('href="https://circleci.com/gh/yetone/mirdb"');
  });

  test('CI badge image has correct src', () => {
    expect(sectionHtml).toContain('https://circleci.com/gh/yetone/mirdb.svg?style=shield');
  });

  test('CI badge image has alt text', () => {
    expect(sectionHtml).toContain('alt="CircleCI build status"');
  });

  test('CI badge link has title attribute for tooltip', () => {
    expect(sectionHtml).toContain('title="CircleCI build status: yetone/mirdb"');
  });

  test('CI badge link has aria-label for accessibility', () => {
    expect(sectionHtml).toContain('aria-label="CircleCI build status for yetone/mirdb"');
  });

  test('CI badge link has target="_blank" and rel="noopener noreferrer"', () => {
    expect(sectionHtml).toContain('target="_blank"');
    expect(sectionHtml).toContain('rel="noopener noreferrer"');
  });

  test('CI badge has lazy loading', () => {
    expect(sectionHtml).toContain('loading="lazy"');
  });

  test('CI badge has fallback element for image load failure', () => {
    expect(sectionHtml).toContain('class="ci-badge-fallback"');
    expect(sectionHtml).toContain('onerror="this.style.display=\'none\'; this.nextElementSibling.style.display=\'inline-flex\';"');
  });

  test('fallback element is initially hidden', () => {
    expect(sectionHtml).toContain('style="display:none;"');
  });

  test('fallback element has a status dot', () => {
    expect(sectionHtml).toContain('class="ci-badge-fallback-dot"');
  });
});

describe('Roadmap Section - Completed Features', () => {
  let sectionHtml;

  beforeAll(() => {
    sectionHtml = extractSection(htmlContent, 'roadmap');
  });

  test('completed features section exists', () => {
    expect(sectionHtml).toContain('class="roadmap-completed"');
  });

  test('completed section has a heading', () => {
    expect(sectionHtml).toContain('Completed');
  });

  test('completed list has role=list', () => {
    expect(sectionHtml).toContain('role="list"');
  });

  test('completed items use roadmap-item--completed class', () => {
    const completedClassMatches = sectionHtml.match(/roadmap-item--completed/g);
    expect(completedClassMatches).toHaveLength(4);
  });

  test('completed items have check indicators', () => {
    const checkMatches = sectionHtml.match(/&#10003;/g);
    expect(checkMatches).toHaveLength(4);
  });

  test('Tokio with Memcached protocol is listed as completed', () => {
    expect(sectionHtml).toContain('Tokio with Memcached protocol');
  });

  test('Memtable with SkipList is listed as completed', () => {
    expect(sectionHtml).toContain('Memtable with SkipList');
  });

  test('Minor compaction is listed as completed', () => {
    expect(sectionHtml).toContain('Minor compaction');
  });

  test('Major compaction is listed as completed', () => {
    expect(sectionHtml).toContain('Major compaction');
  });
});

describe('Roadmap Section - Planned Features', () => {
  let sectionHtml;

  beforeAll(() => {
    sectionHtml = extractSection(htmlContent, 'roadmap');
  });

  test('planned features section exists', () => {
    expect(sectionHtml).toContain('class="roadmap-planned"');
  });

  test('planned section has a heading', () => {
    expect(sectionHtml).toContain('Coming Soon');
  });

  test('planned items use roadmap-item--planned class', () => {
    expect(sectionHtml).toContain('roadmap-item--planned');
  });

  test('Raft consensus is listed as planned', () => {
    expect(sectionHtml).toContain('Raft consensus');
  });

  test('planned items have "Coming Soon" label', () => {
    expect(sectionHtml).toContain('class="roadmap-coming-soon-label"');
    expect(sectionHtml).toContain('Coming Soon');
  });

  test('planned items have unchecked indicator (empty checkbox)', () => {
    expect(sectionHtml).toContain('&#9711;');
  });
});

describe('Roadmap Section - CSS', () => {
  const cssPath = path.resolve(__dirname, '../../css/roadmap.css');
  let cssContent;

  beforeAll(() => {
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  });

  test('roadmap.css styles the roadmap section', () => {
    expect(cssContent).toContain('#roadmap');
  });

  test('roadmap.css styles the CI badge', () => {
    expect(cssContent).toContain('.ci-badge');
    expect(cssContent).toContain('.ci-badge-link');
  });

  test('roadmap.css styles completed items', () => {
    expect(cssContent).toContain('.roadmap-item--completed');
  });

  test('roadmap.css styles planned items', () => {
    expect(cssContent).toContain('.roadmap-item--planned');
  });

  test('roadmap.css styles "Coming Soon" label', () => {
    expect(cssContent).toContain('.roadmap-coming-soon-label');
  });

  test('roadmap.css has responsive breakpoints', () => {
    expect(cssContent).toContain('@media');
  });

  test('roadmap.css has hover effects on badge link', () => {
    expect(cssContent).toContain('.ci-badge-link:hover');
  });

  test('roadmap.css has fallback styling', () => {
    expect(cssContent).toContain('.ci-badge-fallback');
    expect(cssContent).toContain('.ci-badge-fallback-dot');
  });

  test('roadmap.css uses CSS custom properties', () => {
    expect(cssContent).toContain('var(--color-primary)');
    expect(cssContent).toContain('var(--color-accent)');
    expect(cssContent).toContain('var(--color-text)');
  });
});
