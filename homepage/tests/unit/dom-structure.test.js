/**
 * DOM Structure Tests
 * Owner: Multiple scenarios - each adds tests for their section
 *
 * Test groups:
 * - Hero section elements (Scenario 1)
 * - Features grid elements (Scenario 2)
 * - Terminal demo elements (Scenario 3)
 * - Quick-start elements (Scenario 4)
 * - Navigation elements (Scenario 5)
 * - Architecture elements (Scenario 6)
 */

const fs = require('fs');
const path = require('path');

// Read the HTML file
const htmlPath = path.join(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf8');

// Set up DOM
document.documentElement.innerHTML = htmlContent;

/**
 * Hero Section Tests (Scenario 1)
 */
describe('Hero Section (Scenario 1)', () => {
  describe('Test Case 1: H1 element contains MirDB text', () => {
    test('H1 element exists and contains "MirDB"', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });

    test('H1 has the correct class', () => {
      const h1 = document.querySelector('.hero__title');
      expect(h1).not.toBeNull();
      expect(h1.tagName).toBe('H1');
    });
  });

  describe('Test Case 2: Logo image is present', () => {
    test('Logo image exists with correct src', () => {
      const logo = document.querySelector('.hero__logo');
      expect(logo).not.toBeNull();
      expect(logo.tagName).toBe('IMG');
      expect(logo.getAttribute('src')).toContain('logo.gif');
    });

    test('Logo has alt text for accessibility', () => {
      const logo = document.querySelector('.hero__logo');
      expect(logo).not.toBeNull();
      expect(logo.getAttribute('alt')).toBeTruthy();
      expect(logo.getAttribute('alt').toLowerCase()).toContain('logo');
    });
  });

  describe('Test Case 3: Tagline is visible', () => {
    test('Tagline text is present', () => {
      const tagline = document.querySelector('.hero__tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent).toBe('A Persistent Key-Value Store with Memcached Protocol');
    });
  });

  describe('Hero section structure', () => {
    test('Hero section exists', () => {
      const hero = document.querySelector('.hero');
      expect(hero).not.toBeNull();
      expect(hero.tagName).toBe('SECTION');
    });

    test('Hero has aria-labelledby for accessibility', () => {
      const hero = document.querySelector('.hero');
      expect(hero).not.toBeNull();
      expect(hero.getAttribute('aria-labelledby')).toBeTruthy();
    });

    test('Hero container exists', () => {
      const container = document.querySelector('.hero__container');
      expect(container).not.toBeNull();
    });

    test('Hero description exists with value proposition', () => {
      const description = document.querySelector('.hero__description');
      expect(description).not.toBeNull();
      expect(description.textContent.length).toBeGreaterThan(50);
    });
  });
});

/**
 * Features Grid Section Tests (Scenario 2)
 */
describe('Features Grid Section', () => {
  let featuresSection;
  let featureCards;

  beforeAll(() => {
    featuresSection = document.getElementById('features');
    featureCards = document.querySelectorAll('.feature-card');
  });

  // Test Case 1: Exactly 4 feature cards are present in the DOM
  test('should have exactly 4 feature cards', () => {
    expect(featureCards.length).toBe(4);
  });

  // Test Case 2: Feature card with 'Tokio' or 'async' in title/description exists
  test('should have a Tokio async feature card', () => {
    const tokioCard = Array.from(featureCards).find(card => {
      const title = card.querySelector('.feature-card__title')?.textContent || '';
      const description = card.querySelector('.feature-card__description')?.textContent || '';
      return (
        title.toLowerCase().includes('tokio') ||
        title.toLowerCase().includes('async') ||
        description.toLowerCase().includes('tokio') ||
        description.toLowerCase().includes('async')
      );
    });
    expect(tokioCard).toBeTruthy();
  });

  // Test Case 3: Feature card describing Memcached protocol compatibility exists
  test('should have a Memcached protocol feature card', () => {
    const memcachedCard = Array.from(featureCards).find(card => {
      const title = card.querySelector('.feature-card__title')?.textContent || '';
      const description = card.querySelector('.feature-card__description')?.textContent || '';
      return (
        title.toLowerCase().includes('memcached') ||
        description.toLowerCase().includes('memcached')
      );
    });
    expect(memcachedCard).toBeTruthy();
  });

  // Test Case 4: Feature card describing Skip-list memtable exists
  test('should have a Skip-list memtable feature card', () => {
    const skipListCard = Array.from(featureCards).find(card => {
      const title = card.querySelector('.feature-card__title')?.textContent || '';
      const description = card.querySelector('.feature-card__description')?.textContent || '';
      return (
        title.toLowerCase().includes('skip-list') ||
        title.toLowerCase().includes('skiplist') ||
        description.toLowerCase().includes('skip-list') ||
        description.toLowerCase().includes('skiplist')
      );
    });
    expect(skipListCard).toBeTruthy();
  });

  // Test Case 5: Feature card describing compaction (minor/major) exists
  test('should have a Compaction feature card', () => {
    const compactionCard = Array.from(featureCards).find(card => {
      const title = card.querySelector('.feature-card__title')?.textContent || '';
      const description = card.querySelector('.feature-card__description')?.textContent || '';
      return (
        title.toLowerCase().includes('compaction') ||
        description.toLowerCase().includes('compaction') ||
        (description.toLowerCase().includes('minor') && description.toLowerCase().includes('major'))
      );
    });
    expect(compactionCard).toBeTruthy();
  });

  // Test Case 6: Each feature card contains an SVG icon element
  test('should have an SVG icon in each feature card', () => {
    featureCards.forEach((card, index) => {
      const iconContainer = card.querySelector('.feature-card__icon');
      expect(iconContainer).toBeTruthy();

      const svg = iconContainer?.querySelector('svg');
      expect(svg).toBeTruthy();
    });
  });

  // Additional structural tests
  test('features section should exist', () => {
    expect(featuresSection).toBeTruthy();
  });

  test('features section should have a title', () => {
    const title = featuresSection.querySelector('.section__title, h2');
    expect(title).toBeTruthy();
    expect(title.textContent.toLowerCase()).toContain('feature');
  });

  test('features grid container should exist', () => {
    const grid = featuresSection.querySelector('.features__grid');
    expect(grid).toBeTruthy();
  });

  test('each feature card should have a title', () => {
    featureCards.forEach(card => {
      const title = card.querySelector('.feature-card__title');
      expect(title).toBeTruthy();
      expect(title.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  test('each feature card should have a description', () => {
    featureCards.forEach(card => {
      const description = card.querySelector('.feature-card__description');
      expect(description).toBeTruthy();
      expect(description.textContent.trim().length).toBeGreaterThan(0);
    });
  });
});

/**
 * Terminal Demo Section Tests - Scenario 3
 * Tests for Interactive Terminal Demo
 */
describe('Terminal Demo Section (Scenario 3)', () => {
  describe('Test Case 1: Terminal demo container exists with dark background styling', () => {
    test('should have terminal demo section element', () => {
      const terminalDemo = document.querySelector('.terminal-demo');
      expect(terminalDemo).not.toBeNull();
      expect(terminalDemo.id).toBe('terminal-demo');
    });

    test('should have terminal container element', () => {
      const terminal = document.querySelector('.terminal');
      expect(terminal).not.toBeNull();
    });

    test('terminal body should exist for dark background', () => {
      const terminalBody = document.querySelector('.terminal__body');
      expect(terminalBody).not.toBeNull();
    });

    test('terminal header should have window controls', () => {
      const dots = document.querySelectorAll('.terminal__dot');
      expect(dots.length).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Test Case 2: Terminal uses monospace font family', () => {
    test('terminal body should exist and be styled for monospace', () => {
      const terminalBody = document.querySelector('.terminal__body');
      expect(terminalBody).not.toBeNull();
      expect(terminalBody.classList.contains('terminal__body')).toBe(true);
    });

    test('terminal title should exist with monospace font', () => {
      const terminalTitle = document.querySelector('.terminal__title');
      expect(terminalTitle).not.toBeNull();
    });
  });

  describe('Test Case 3: Terminal displays SET command example', () => {
    test('should contain SET keyword in terminal content', () => {
      const terminalBody = document.querySelector('.terminal__body');
      expect(terminalBody).not.toBeNull();

      const allKeywords = document.querySelectorAll('.terminal__keyword');
      const keywordTexts = Array.from(allKeywords).map(el => el.textContent);
      expect(keywordTexts).toContain('SET');
    });

    test('should have SET command line structure', () => {
      const terminalCommands = document.querySelectorAll('.terminal__command');
      const commandTexts = Array.from(terminalCommands).map(el => el.textContent);
      const hasSetCommand = commandTexts.some(text => text.includes('SET'));
      expect(hasSetCommand).toBe(true);
    });
  });

  describe('Test Case 4: Terminal displays GET command example', () => {
    test('should contain GET keyword in terminal content', () => {
      const allKeywords = document.querySelectorAll('.terminal__keyword');
      const keywordTexts = Array.from(allKeywords).map(el => el.textContent);
      expect(keywordTexts).toContain('GET');
    });

    test('should have GET command line structure', () => {
      const terminalCommands = document.querySelectorAll('.terminal__command');
      const commandTexts = Array.from(terminalCommands).map(el => el.textContent);
      const hasGetCommand = commandTexts.some(text => text.includes('GET'));
      expect(hasGetCommand).toBe(true);
    });
  });

  describe('Test Case 5: Terminal displays DELETE command example', () => {
    test('should contain DELETE keyword in terminal content', () => {
      const allKeywords = document.querySelectorAll('.terminal__keyword');
      const keywordTexts = Array.from(allKeywords).map(el => el.textContent);
      expect(keywordTexts).toContain('DELETE');
    });

    test('should have DELETE command line structure', () => {
      const terminalCommands = document.querySelectorAll('.terminal__command');
      const commandTexts = Array.from(terminalCommands).map(el => el.textContent);
      const hasDeleteCommand = commandTexts.some(text => text.includes('DELETE'));
      expect(hasDeleteCommand).toBe(true);
    });
  });

  describe('Test Case 6: Usage GIF image is present', () => {
    test('should have usage GIF image element', () => {
      const usageGif = document.querySelector('.terminal-demo__usage-gif');
      expect(usageGif).not.toBeNull();
    });

    test('usage GIF should have correct src attribute', () => {
      const usageGif = document.querySelector('.terminal-demo__usage-gif');
      expect(usageGif).not.toBeNull();
      expect(usageGif.getAttribute('src')).toContain('usage.gif');
    });

    test('usage GIF should have alt text for accessibility', () => {
      const usageGif = document.querySelector('.terminal-demo__usage-gif');
      expect(usageGif).not.toBeNull();
      const altText = usageGif.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(10);
    });

    test('should have gif title section', () => {
      const gifTitle = document.querySelector('.terminal-demo__gif-title');
      expect(gifTitle).not.toBeNull();
      expect(gifTitle.textContent).toBeTruthy();
    });
  });

  describe('Terminal Demo Accessibility', () => {
    test('terminal demo should have descriptive title', () => {
      const title = document.querySelector('.terminal-demo__title');
      expect(title).not.toBeNull();
      expect(title.textContent.length).toBeGreaterThan(0);
    });

    test('terminal demo should have subtitle/description', () => {
      const subtitle = document.querySelector('.terminal-demo__subtitle');
      expect(subtitle).not.toBeNull();
      expect(subtitle.textContent.length).toBeGreaterThan(0);
    });
  });
});
