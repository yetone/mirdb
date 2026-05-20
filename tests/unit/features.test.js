/**
 * Features Section Unit Tests
 * Owner: Scenario 3 - Features Section
 *
 * Tests:
 * - DOM structure of feature cards
 * - Card content verification
 * - Icon presence and accessibility
 * - Grid layout structure
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.resolve(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

// Helper to parse HTML without a full DOM parser
function extractSection(html, sectionId) {
  const regex = new RegExp(`<section id="${sectionId}"[^>]*>(.*?)</section>`, 's');
  const match = html.match(regex);
  return match ? match[1] : '';
}

function extractCards(sectionHtml) {
  const cards = [];
  const cardRegex = /<article class="feature-card"[^>]*>(.*?)<\/article>/gs;
  let match;
  while ((match = cardRegex.exec(sectionHtml)) !== null) {
    cards.push(match[1]);
  }
  return cards;
}

describe('Features Section - Structure', () => {
  let sectionHtml;
  let cards;

  beforeAll(() => {
    sectionHtml = extractSection(htmlContent, 'features');
    cards = extractCards(sectionHtml);
  });

  test('features section exists in HTML', () => {
    expect(sectionHtml).not.toBe('');
  });

  test('exactly 4 feature cards are present', () => {
    expect(cards).toHaveLength(4);
  });

  test('section has a title', () => {
    expect(sectionHtml).toContain('<h2 class="section-title">');
    expect(sectionHtml).toContain('Key Features');
  });

  test('cards are wrapped in a features-grid container', () => {
    expect(sectionHtml).toContain('class="features-grid"');
  });

  test('cards are wrapped in a container', () => {
    expect(sectionHtml).toContain('class="container"');
  });
});

describe('Features Section - Card Content', () => {
  let cards;

  beforeAll(() => {
    const sectionHtml = extractSection(htmlContent, 'features');
    cards = extractCards(sectionHtml);
  });

  test('Card 1: Tokio Async Runtime', () => {
    const card = cards[0];
    expect(card).toContain('Tokio Async Runtime');
    expect(card).toMatch(new RegExp('High-performance asynchronous I/O', 'i'));
  });

  test('Card 2: Memcached Protocol', () => {
    const card = cards[1];
    expect(card).toContain('Memcached Protocol');
    expect(card).toMatch(new RegExp('Drop-in compatibility with existing tools', 'i'));
  });

  test('Card 3: SkipList Memtable', () => {
    const card = cards[2];
    expect(card).toContain('SkipList Memtable');
    expect(card).toMatch(new RegExp('efficient in-memory data structure', 'i'));
  });

  test('Card 4: Compaction', () => {
    const card = cards[3];
    expect(card).toContain('Compaction');
    expect(card).toMatch(new RegExp('minor and major compaction', 'i'));
  });
});

describe('Features Section - Icons and Accessibility', () => {
  let cards;

  beforeAll(() => {
    const sectionHtml = extractSection(htmlContent, 'features');
    cards = extractCards(sectionHtml);
  });

  test('each card has an icon element', () => {
    cards.forEach((card, index) => {
      expect(card).toContain('class="feature-icon"');
      expect(card).toContain('<svg');
    });
  });

  test('each icon has an aria-label', () => {
    cards.forEach((card) => {
      expect(card).toMatch(/aria-label="[^"]*icon"/);
    });
  });

  test('each card has a title (h3)', () => {
    cards.forEach((card) => {
      expect(card).toMatch(/<h3 class="feature-title"/);
    });
  });

  test('each card has a description (p)', () => {
    cards.forEach((card) => {
      expect(card).toMatch(/<p class="feature-description"/);
    });
  });

  test('cards use semantic article elements', () => {
    const sectionHtml = extractSection(htmlContent, 'features');
    const articleMatches = sectionHtml.match(/<article class="feature-card"/g);
    expect(articleMatches).toHaveLength(4);
  });
});

describe('Features Section - CSS', () => {
  const cssPath = path.resolve(__dirname, '../../css/features.css');
  let cssContent;

  beforeAll(() => {
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  });

  test('features.css defines grid layout', () => {
    expect(cssContent).toContain('display: grid');
    expect(cssContent).toContain('grid-template-columns');
  });

  test('features.css defines 4-column grid for desktop', () => {
    expect(cssContent).toContain('repeat(4, 1fr)');
  });

  test('features.css defines 2-column grid for tablet', () => {
    expect(cssContent).toContain('repeat(2, 1fr)');
  });

  test('features.css defines 1-column grid for mobile', () => {
    expect(cssContent).toContain('1fr');
  });

  test('features.css styles feature cards', () => {
    expect(cssContent).toContain('.feature-card');
    expect(cssContent).toContain('.feature-icon');
    expect(cssContent).toContain('.feature-title');
    expect(cssContent).toContain('.feature-description');
  });

  test('features.css has hover effects', () => {
    expect(cssContent).toContain(':hover');
    expect(cssContent).toContain('transition');
  });

  test('features.css has responsive breakpoints', () => {
    expect(cssContent).toContain('@media');
    expect(cssContent).toContain('max-width: 1023px');
    expect(cssContent).toContain('max-width: 767px');
  });
});
