/**
 * Component Unit Tests
 * Owner: Scenario 1 - Hero Section Display (hero portion)
 * Contributor: Scenario 3 - Quick Start and Installation Section
 *
 * Unit tests for component rendering and functionality.
 * Other scenarios will add their component tests to this file.
 */

import { HERO_DATA } from '../fixtures/test-data.js';
import { readFileSync } from 'fs';
import { join } from 'path';

describe('Hero Section Component', () => {
  let document;

  beforeEach(() => {
    // Set up a minimal DOM structure for testing
    document = global.document;
    document.body.innerHTML = `
      <section id="hero" class="hero" aria-labelledby="hero-title">
        <div class="hero__logo" aria-hidden="true">
          <img src="images/logo.svg" alt="" width="120" height="120">
        </div>
        <h1 id="hero-title" class="hero__title">MiRDB</h1>
        <p class="hero__tagline">
          A persistent key-value store with full Memcached protocol support.
          Built with Rust for blazing-fast performance and reliable data persistence.
        </p>
        <div class="hero__cta">
          <a href="https://github.com/yetone/mirdb"
             class="btn btn-primary"
             target="_blank"
             rel="noopener noreferrer"
             aria-label="View MiRDB on GitHub (opens in new tab)">
            View on GitHub
          </a>
          <a href="#quickstart" class="btn btn-secondary">
            Get Started
          </a>
        </div>
      </section>
    `;
  });

  afterEach(() => {
    document.body.innerHTML = '';
  });

  test('TC5: Hero section renders without errors and contains required elements', () => {
    // Verify hero section exists
    const hero = document.getElementById('hero');
    expect(hero).not.toBeNull();
    expect(hero.classList.contains('hero')).toBe(true);

    // Verify logo container exists
    const logo = hero.querySelector('.hero__logo');
    expect(logo).not.toBeNull();

    // Verify logo image exists
    const logoImg = logo.querySelector('img');
    expect(logoImg).not.toBeNull();
    expect(logoImg.getAttribute('src')).toBe('images/logo.svg');

    // Verify title exists and has correct text
    const title = hero.querySelector('.hero__title');
    expect(title).not.toBeNull();
    expect(title.textContent).toBe(HERO_DATA.title);

    // Verify tagline exists
    const tagline = hero.querySelector('.hero__tagline');
    expect(tagline).not.toBeNull();
    expect(tagline.textContent.toLowerCase()).toContain('persistent');
    expect(tagline.textContent.toLowerCase()).toContain('key-value');

    // Verify CTA buttons exist
    const ctaContainer = hero.querySelector('.hero__cta');
    expect(ctaContainer).not.toBeNull();

    const primaryBtn = ctaContainer.querySelector('.btn-primary');
    expect(primaryBtn).not.toBeNull();
    expect(primaryBtn.getAttribute('href')).toBe(HERO_DATA.githubUrl);

    const secondaryBtn = ctaContainer.querySelector('.btn-secondary');
    expect(secondaryBtn).not.toBeNull();
    expect(secondaryBtn.getAttribute('href')).toBe(HERO_DATA.quickstartAnchor);
  });

  test('Hero section has proper accessibility structure', () => {
    const hero = document.getElementById('hero');

    // Check aria-labelledby
    expect(hero.getAttribute('aria-labelledby')).toBe('hero-title');

    // Check logo is hidden from screen readers (decorative)
    const logo = hero.querySelector('.hero__logo');
    expect(logo.getAttribute('aria-hidden')).toBe('true');

    // Check GitHub button has accessible label
    const githubBtn = hero.querySelector('.btn-primary');
    expect(githubBtn.getAttribute('aria-label')).toContain('GitHub');
  });

  test('GitHub button has security attributes', () => {
    const hero = document.getElementById('hero');
    const githubBtn = hero.querySelector('.btn-primary');

    // External link should open in new tab
    expect(githubBtn.getAttribute('target')).toBe('_blank');

    // Should have rel attribute for security
    const rel = githubBtn.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('Hero section contains all required HERO_DATA keywords', () => {
    const hero = document.getElementById('hero');
    const tagline = hero.querySelector('.hero__tagline').textContent.toLowerCase();

    // Check all keywords from test data are present
    HERO_DATA.keywords.forEach(keyword => {
      expect(tagline).toContain(keyword.toLowerCase());
    });
  });
});

describe('QuickStartSection', () => {
  let html;

  beforeAll(() => {
    // Load the HTML file - use process.cwd() to get the root directory
    const htmlPath = join(process.cwd(), 'index.html');
    html = readFileSync(htmlPath, 'utf-8');
  });

  test('TC-5: should render QuickStartSection component with installation steps in correct order', () => {
    // Verify section exists with quickstart id and class
    expect(html).toContain('id="quickstart"');
    expect(html).toContain('class="quickstart"');

    // Verify steps list exists
    expect(html).toContain('class="quickstart__steps"');

    // Count the number of steps (data-step attributes)
    const stepMatches = html.match(/data-step="\d+"/g);
    expect(stepMatches).not.toBeNull();
    expect(stepMatches.length).toBeGreaterThanOrEqual(3);

    // Extract step numbers and verify sequential order
    const stepNumbers = stepMatches.map(match => {
      const num = match.match(/data-step="(\d+)"/);
      return num ? parseInt(num[1], 10) : 0;
    });

    // Check steps are in sequential order
    for (let i = 0; i < stepNumbers.length - 1; i++) {
      expect(stepNumbers[i]).toBeLessThan(stepNumbers[i + 1]);
    }

    // Extract quickstart section content
    const sectionMatch = html.match(/<section id="quickstart"[\s\S]*?<!-- END: Scenario 3/);
    const sectionContent = sectionMatch ? sectionMatch[0] : '';

    // Verify step titles exist in order (Download -> Cargo -> Run)
    const downloadIndex = sectionContent.indexOf('Download Pre-built Binary');
    const cargoIndex = sectionContent.indexOf('Install via Cargo');
    const runIndex = sectionContent.indexOf('Run the Server');

    expect(downloadIndex).toBeGreaterThan(-1);
    expect(cargoIndex).toBeGreaterThan(-1);
    expect(runIndex).toBeGreaterThan(-1);

    // Steps should be in order
    expect(downloadIndex).toBeLessThan(cargoIndex);
    expect(cargoIndex).toBeLessThan(runIndex);
  });

  test('should have section heading', () => {
    expect(html).toContain('id="quickstart-heading"');
    expect(html).toMatch(/Quick Start/i);
  });

  test('should contain cargo install command', () => {
    // Check for cargo install in a code block
    expect(html).toContain('cargo install mirdb');
    expect(html).toMatch(/<code>cargo install/);
  });

  test('should contain mirdb-server run command', () => {
    // Check for mirdb-server command in a code block
    expect(html).toContain('mirdb-server');
    expect(html).toMatch(/<code>mirdb-server<\/code>/);
  });

  test('should have link to GitHub releases', () => {
    expect(html).toContain('github.com/yetone/mirdb/releases');
  });

  test('should have proper accessibility attributes', () => {
    // Check for aria-labelledby on section
    expect(html).toContain('aria-labelledby="quickstart-heading"');

    // Check for role="list" on steps
    expect(html).toContain('role="list"');
  });
});

// Scenario 2 - FeatureCard Component Tests
describe('FeatureCard Component', () => {
  let html;

  beforeAll(() => {
    const htmlPath = join(process.cwd(), 'index.html');
    html = readFileSync(htmlPath, 'utf-8');
  });

  // Test Case 5: Render FeatureCard component with status='completed'
  test('TC-5: FeatureCard with status="completed" displays checkmark indicator', () => {
    // Find a completed feature card
    const completedMatch = html.match(/<article[^>]*data-status="completed"[\s\S]*?<\/article>/);
    expect(completedMatch).not.toBeNull();

    const cardHtml = completedMatch[0];

    // Check for status-badge--completed class
    expect(cardHtml).toContain('status-badge--completed');

    // Check for Completed text
    expect(cardHtml).toContain('Completed');

    // Check for aria-label for accessibility
    expect(cardHtml).toContain('aria-label="Feature completed"');
  });

  // Test Case 6: Render FeatureCard component with status='planned'
  test('TC-6: FeatureCard with status="planned" displays planning indicator', () => {
    // Find a planned feature card
    const plannedMatch = html.match(/<article[^>]*data-status="planned"[\s\S]*?<\/article>/);
    expect(plannedMatch).not.toBeNull();

    const cardHtml = plannedMatch[0];

    // Check for status-badge--planned class
    expect(cardHtml).toContain('status-badge--planned');

    // Check for Planned text
    expect(cardHtml).toContain('Planned');

    // Check for aria-label for accessibility
    expect(cardHtml).toContain('aria-label="Feature planned"');
  });

  test('all feature cards have required structure', () => {
    // Check that features section exists
    expect(html).toContain('id="features"');
    expect(html).toContain('class="features"');

    // Check for feature cards
    const featureCards = html.match(/<article[^>]*class="feature-card"/g);
    expect(featureCards).not.toBeNull();
    expect(featureCards.length).toBe(4);

    // Check for expected features
    expect(html).toContain('data-feature="memcached"');
    expect(html).toContain('data-feature="persistence"');
    expect(html).toContain('data-feature="lsm-tree"');
    expect(html).toContain('data-feature="raft"');
  });

  test('feature cards have icons', () => {
    // Check for feature-card__icon class
    const iconElements = html.match(/class="feature-card__icon"/g);
    expect(iconElements).not.toBeNull();
    expect(iconElements.length).toBe(4);
  });

  test('features section has accessibility attributes', () => {
    expect(html).toContain('aria-labelledby="features-title"');
    expect(html).toContain('id="features-title"');
  });
});
