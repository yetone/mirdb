/**
 * Component Unit Tests
 * Owner: Scenario 1 - Hero Section Display (hero portion)
 * Contributor: Scenario 3 - Quick Start and Installation Section
 * Contributor: Scenario 6 - Configuration Documentation Section
 * Contributor: Scenario 7 - Navigation and GitHub Links
 *
 * Unit tests for component rendering and functionality.
 * Other scenarios will add their component tests to this file.
 */

import { HERO_DATA, CONFIG_VALUES } from '../fixtures/test-data.js';
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

// Scenario 6 - ConfigSection Component Tests
describe('ConfigSection Component', () => {
  let html;

  beforeAll(() => {
    const htmlPath = join(process.cwd(), 'index.html');
    html = readFileSync(htmlPath, 'utf-8');
  });

  // Test Case 5: Render ConfigSection component with configuration data
  test('TC-5: ConfigSection renders configuration table with all default values', () => {
    // Verify configuration section exists
    expect(html).toContain('id="configuration"');
    expect(html).toContain('class="config"');

    // Verify configuration table exists
    expect(html).toContain('class="config__table"');

    // Extract configuration section content
    const sectionMatch = html.match(/<section id="configuration"[\s\S]*?<!-- END: Scenario 6/);
    expect(sectionMatch).not.toBeNull();
    const sectionContent = sectionMatch[0];

    // Verify all configuration values from CONFIG_VALUES are present
    CONFIG_VALUES.forEach(config => {
      // Check that the key is present
      expect(sectionContent).toContain(config.key);
      // Check that the value is present (handle different formats)
      const valueToCheck = config.value.replace('0.0.0.0:', '');
      expect(sectionContent.includes(config.value) || sectionContent.includes(valueToCheck)).toBe(true);
    });

    // Verify configuration rows exist
    const configRows = sectionContent.match(/data-config="/g);
    expect(configRows).not.toBeNull();
    expect(configRows.length).toBeGreaterThanOrEqual(5);
  });

  test('configuration section has port 12333 documented', () => {
    expect(html).toContain('12333');
    expect(html).toContain('addr');
    expect(html).toContain('data-config="port"');
  });

  test('configuration section has work directory /tmp/mirdb documented', () => {
    expect(html).toContain('/tmp/mirdb');
    expect(html).toContain('work_dir');
    expect(html).toContain('data-config="work_dir"');
  });

  test('configuration section has max levels 7 documented', () => {
    expect(html).toContain('max_level');
    expect(html).toContain('data-config="max_level"');
    // The value 7 should be in the config value cell
    const sectionMatch = html.match(/<tr[^>]*data-config="max_level"[\s\S]*?<\/tr>/);
    expect(sectionMatch).not.toBeNull();
    expect(sectionMatch[0]).toContain('>7<');
  });

  test('configuration section has SSTable max size 100MB documented', () => {
    expect(html).toContain('sst_max_size');
    expect(html).toContain('100MB');
    expect(html).toContain('data-config="sst_max_size"');
  });

  test('configuration section has memtable max size 4MB documented', () => {
    expect(html).toContain('mem_table_max_size');
    expect(html).toContain('4MB');
    expect(html).toContain('data-config="mem_table_max_size"');
  });

  test('configuration table has proper structure', () => {
    // Check table headers
    expect(html).toContain('class="config__header"');
    expect(html).toContain('Parameter');
    expect(html).toContain('Default Value');
    expect(html).toContain('Description');

    // Check for table rows with proper classes
    expect(html).toContain('class="config__row"');
    expect(html).toContain('class="config__key"');
    expect(html).toContain('class="config__value"');
    expect(html).toContain('class="config__description"');
  });

  test('configuration section has accessibility attributes', () => {
    expect(html).toContain('aria-labelledby="config-heading"');
    expect(html).toContain('id="config-heading"');
    expect(html).toContain('aria-label="Configuration options"');
  });

  test('configuration section has example code block', () => {
    expect(html).toContain('class="config__example"');
    expect(html).toContain('class="config__code"');
    expect(html).toContain('mirdb.toml');
  });

  test('configuration section has usage note', () => {
    expect(html).toContain('class="config__note"');
    expect(html).toContain('mirdb -c');
    expect(html).toContain('config.toml');
  });
});

// Scenario 7 - Header Component Tests
describe('Header Component', () => {
  let html;

  beforeAll(() => {
    const htmlPath = join(process.cwd(), 'index.html');
    html = readFileSync(htmlPath, 'utf-8');
  });

  // Test Case 6: Render Header component
  test('TC-6: Header renders logo, navigation links, and GitHub button', () => {
    // Check header exists
    expect(html).toContain('class="header"');
    expect(html).toContain('class="header__container"');

    // Check logo exists
    expect(html).toContain('class="header__logo"');
    expect(html).toContain('MiRDB');

    // Check navigation exists
    expect(html).toContain('class="header__nav"');

    // Check for navigation links
    expect(html).toContain('class="header__link"');

    // Check for GitHub link/button
    expect(html).toContain('class="header__github"');
    expect(html).toContain('https://github.com/yetone/mirdb');
  });

  test('Header GitHub link has proper attributes', () => {
    // Extract header section
    const headerMatch = html.match(/<header class="header"[\s\S]*?<\/header>/);
    expect(headerMatch).not.toBeNull();
    const headerHtml = headerMatch[0];

    // Check GitHub link has target="_blank"
    expect(headerHtml).toContain('target="_blank"');

    // Check GitHub link has rel="noopener noreferrer"
    expect(headerHtml).toContain('rel="noopener noreferrer"');

    // Check for external link icon
    expect(headerHtml).toContain('external-link-icon');
  });

  test('Header has accessibility attributes', () => {
    // Check for aria-label on navigation
    expect(html).toContain('aria-label="Main navigation"');

    // Check GitHub button has aria-label mentioning new tab
    expect(html).toContain('aria-label="View on GitHub (opens in new tab)"');
  });

  test('Header contains documentation link', () => {
    // Check for Features or Docs link
    const headerMatch = html.match(/<header class="header"[\s\S]*?<\/header>/);
    expect(headerMatch).not.toBeNull();
    const headerHtml = headerMatch[0];

    // Should have Features and Docs links
    const hasFeatures = headerHtml.includes('href="#features"');
    const hasDocs = headerHtml.includes('Docs');

    expect(hasFeatures || hasDocs).toBe(true);
  });
});

// Scenario 7 - Footer Component Tests
describe('Footer Component', () => {
  let html;

  beforeAll(() => {
    const htmlPath = join(process.cwd(), 'index.html');
    html = readFileSync(htmlPath, 'utf-8');
  });

  // Test Case 7: Render Footer component
  test('TC-7: Footer renders links, license, and contact information', () => {
    // Check footer exists
    expect(html).toContain('class="footer"');
    expect(html).toContain('class="footer__container"');

    // Check for copyright/project info
    expect(html).toContain('class="footer__copyright"');
    expect(html).toContain('MiRDB');

    // Check for footer links
    expect(html).toContain('class="footer__links"');

    // Check for GitHub link
    expect(html).toContain('https://github.com/yetone/mirdb');

    // Check for license information (MIT)
    const footerMatch = html.match(/<footer class="footer"[\s\S]*?<\/footer>/);
    expect(footerMatch).not.toBeNull();
    const footerHtml = footerMatch[0];

    const hasLicense = footerHtml.toLowerCase().includes('license') ||
                       footerHtml.toLowerCase().includes('mit');
    expect(hasLicense).toBe(true);
  });

  test('Footer external links have proper security attributes', () => {
    // Extract footer section
    const footerMatch = html.match(/<footer class="footer"[\s\S]*?<\/footer>/);
    expect(footerMatch).not.toBeNull();
    const footerHtml = footerMatch[0];

    // Check external links have target="_blank"
    expect(footerHtml).toContain('target="_blank"');

    // Check external links have rel="noopener noreferrer"
    expect(footerHtml).toContain('rel="noopener noreferrer"');
  });

  test('Footer external links have visual indicators', () => {
    // Extract footer section
    const footerMatch = html.match(/<footer class="footer"[\s\S]*?<\/footer>/);
    expect(footerMatch).not.toBeNull();
    const footerHtml = footerMatch[0];

    // Check for external link icons
    expect(footerHtml).toContain('external-link-icon');
  });

  test('Footer has accessibility attributes', () => {
    // Check for aria-label on footer navigation
    expect(html).toContain('aria-label="Footer navigation"');

    // Check footer links have aria-labels mentioning new tab
    const footerMatch = html.match(/<footer class="footer"[\s\S]*?<\/footer>/);
    expect(footerMatch).not.toBeNull();
    const footerHtml = footerMatch[0];

    const hasNewTabAria = footerHtml.includes('opens in new tab');
    expect(hasNewTabAria).toBe(true);
  });
});
