/**
 * User Journey - New Developer Tests
 * Scenario: Verify complete user journey for a new developer evaluating MirDB (US-1, US-2)
 *
 * This test suite validates the experience of a new developer discovering and
 * evaluating MirDB through the homepage.
 */

const fs = require('fs');
const path = require('path');

describe('User Journey - New Developer', () => {
  let document;
  let html;

  beforeAll(() => {
    // Load the index.html file
    const htmlPath = path.resolve(__dirname, '../index.html');
    html = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  /**
   * Test Case 1: Load homepage and measure time to first contentful paint
   * Expected: Hero section content visible within 3 seconds
   * Type: e2e
   */
  describe('Test Case 1: Homepage Load Performance', () => {
    test('should have hero section as first major content section', () => {
      // Hero section should be the first major content section after header
      const main = document.querySelector('main');
      expect(main).not.toBeNull();

      const firstSection = main.querySelector('section');
      expect(firstSection).not.toBeNull();
      expect(firstSection.id).toBe('hero');
    });

    test('hero section should contain essential content without external dependencies', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      // Check for essential text content that renders immediately
      const h1 = heroSection.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toBeTruthy();

      // Check for tagline
      const tagline = heroSection.querySelector('.tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent).toBeTruthy();
    });

    test('page should have minimal blocking resources', () => {
      // Check for reasonable CSS and JS file count (minimize render-blocking resources)
      const styleLinks = document.querySelectorAll('link[rel="stylesheet"]');
      const scripts = document.querySelectorAll('script[src]');

      // Should have minimal external resources for fast loading
      expect(styleLinks.length).toBeLessThanOrEqual(3);
      expect(scripts.length).toBeLessThanOrEqual(3);
    });

    test('hero section should not depend on JavaScript for initial render', () => {
      // Core hero content should be in the HTML, not dynamically generated
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      // All essential elements should be present in static HTML
      const heroContent = heroSection.querySelector('.hero-content');
      expect(heroContent).not.toBeNull();

      // Logo, heading, tagline should be static
      expect(heroSection.querySelector('img')).not.toBeNull();
      expect(heroSection.querySelector('h1')).not.toBeNull();
      expect(heroSection.querySelector('.tagline')).not.toBeNull();
    });
  });

  /**
   * Test Case 2: Check hero section communicates purpose
   * Expected: Tagline clearly states 'key-value store' and 'memcached'
   * Type: integration
   */
  describe('Test Case 2: Hero Section Value Proposition', () => {
    test('should contain "key-value store" in hero text', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      const heroText = heroSection.textContent.toLowerCase();
      expect(heroText).toContain('key-value');
    });

    test('should contain "memcached" in hero text', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      const heroText = heroSection.textContent.toLowerCase();
      expect(heroText).toContain('memcached');
    });

    test('tagline should clearly communicate purpose', () => {
      const tagline = document.querySelector('#hero .tagline');
      expect(tagline).not.toBeNull();

      const taglineText = tagline.textContent.toLowerCase();
      // Tagline should mention both key aspects: key-value and memcached
      expect(taglineText).toContain('key-value');
      expect(taglineText).toContain('memcached');
    });

    test('hero should mention persistence (key differentiator)', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      const heroText = heroSection.textContent.toLowerCase();
      expect(heroText).toContain('persistent');
    });
  });

  /**
   * Test Case 3: Navigate from hero to quick-start
   * Expected: CTA button or nav link successfully scrolls to quick-start section
   * Type: e2e
   */
  describe('Test Case 3: Navigation to Quick-Start', () => {
    test('should have CTA button linking to quick-start section', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      // Look for "Get Started" CTA button
      const ctaLinks = heroSection.querySelectorAll('a');
      let quickStartCTA = null;

      for (const link of ctaLinks) {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        if (href.includes('quick-start') || text.includes('get started') || text.includes('start')) {
          quickStartCTA = link;
          break;
        }
      }

      expect(quickStartCTA).not.toBeNull();
    });

    test('CTA button should link to #quick-start', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      const getStartedLink = heroSection.querySelector('a[href="#quick-start"]');
      expect(getStartedLink).not.toBeNull();
      expect(getStartedLink.getAttribute('href')).toBe('#quick-start');
    });

    test('quick-start section should exist as link target', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).not.toBeNull();
      expect(quickStartSection.tagName.toLowerCase()).toBe('section');
    });

    test('navigation menu should also link to quick-start', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();

      const navLinks = nav.querySelectorAll('a');
      let quickStartNavLink = null;

      for (const link of navLinks) {
        const href = link.getAttribute('href') || '';
        if (href === '#quick-start' || href.includes('quick-start')) {
          quickStartNavLink = link;
          break;
        }
      }

      expect(quickStartNavLink).not.toBeNull();
    });
  });

  /**
   * Test Case 4: Follow quick-start instructions
   * Expected: Instructions include all necessary steps: clone/install, configure, run, connect
   * Type: integration
   */
  describe('Test Case 4: Quick-Start Instructions Completeness', () => {
    test('should include clone/install instructions', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).not.toBeNull();

      const sectionText = quickStartSection.textContent.toLowerCase();

      // Should have clone instructions
      expect(sectionText).toContain('clone');

      // Should have build/install instructions
      const hasBuildOrInstall = sectionText.includes('build') || sectionText.includes('install');
      expect(hasBuildOrInstall).toBe(true);
    });

    test('should include configuration instructions', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).not.toBeNull();

      const sectionText = quickStartSection.textContent.toLowerCase();
      expect(sectionText).toContain('config');
    });

    test('should include run/start instructions', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).not.toBeNull();

      const sectionText = quickStartSection.textContent.toLowerCase();

      // Should have run command
      const hasRunInstruction = sectionText.includes('cargo run') ||
                                 sectionText.includes('run ') ||
                                 sectionText.includes('start');
      expect(hasRunInstruction).toBe(true);
    });

    test('should include connect/usage instructions', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).not.toBeNull();

      const sectionText = quickStartSection.textContent.toLowerCase();

      // Should explain how to connect
      const hasConnectInstruction = sectionText.includes('connect') ||
                                     sectionText.includes('telnet') ||
                                     sectionText.includes('nc ') ||
                                     sectionText.includes('client');
      expect(hasConnectInstruction).toBe(true);
    });

    test('should have code blocks with copy-paste ready commands', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).not.toBeNull();

      // Should have code blocks
      const codeBlocks = quickStartSection.querySelectorAll('pre code');
      expect(codeBlocks.length).toBeGreaterThan(0);

      // Code blocks should contain actual commands
      let hasGitCommand = false;
      let hasCargoCommand = false;
      let hasUsageExample = false;

      for (const code of codeBlocks) {
        const text = code.textContent;
        if (text.includes('git clone')) hasGitCommand = true;
        if (text.includes('cargo')) hasCargoCommand = true;
        if (text.includes('set ') || text.includes('get ')) hasUsageExample = true;
      }

      expect(hasGitCommand).toBe(true);
      expect(hasCargoCommand).toBe(true);
      expect(hasUsageExample).toBe(true);
    });

    test('code blocks should show expected output', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).not.toBeNull();

      const codeBlocks = quickStartSection.querySelectorAll('pre code');
      let hasExpectedOutput = false;

      for (const code of codeBlocks) {
        const text = code.textContent;
        // Should show expected responses like STORED, VALUE, END, etc.
        if (text.includes('STORED') || text.includes('VALUE') || text.includes('END')) {
          hasExpectedOutput = true;
          break;
        }
      }

      expect(hasExpectedOutput).toBe(true);
    });
  });

  /**
   * Test Case 5: Click GitHub link
   * Expected: GitHub link navigates to valid repository URL
   * Type: e2e
   */
  describe('Test Case 5: GitHub Link Accessibility', () => {
    test('should have GitHub link in hero section', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      const links = heroSection.querySelectorAll('a');
      let githubLink = null;

      for (const link of links) {
        const href = link.getAttribute('href') || '';
        if (href.includes('github.com')) {
          githubLink = link;
          break;
        }
      }

      expect(githubLink).not.toBeNull();
    });

    test('GitHub link should point to valid repository URL pattern', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      const githubLink = heroSection.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();

      const href = githubLink.getAttribute('href');
      // Should match GitHub repo URL pattern
      expect(href).toMatch(/^https:\/\/github\.com\/[\w-]+\/[\w-]+$/);
    });

    test('GitHub link should include "mirdb" in URL', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      const githubLink = heroSection.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();

      const href = githubLink.getAttribute('href').toLowerCase();
      expect(href).toContain('mirdb');
    });

    test('GitHub link should open in new tab', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      const githubLink = heroSection.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();

      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    test('GitHub link should have security attributes', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      const githubLink = heroSection.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();

      const rel = githubLink.getAttribute('rel') || '';
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('GitHub link should also be accessible in footer', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const footerGithubLink = footer.querySelector('a[href*="github.com"]');
      expect(footerGithubLink).not.toBeNull();

      const href = footerGithubLink.getAttribute('href');
      expect(href).toMatch(/^https:\/\/github\.com\/[\w-]+\/[\w-]+$/);
    });

    test('GitHub link should also be accessible in navigation', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();

      const navGithubLink = nav.querySelector('a[href*="github.com"]');
      expect(navGithubLink).not.toBeNull();

      const href = navGithubLink.getAttribute('href');
      expect(href).toMatch(/^https:\/\/github\.com\/[\w-]+\/[\w-]+$/);
    });
  });

  /**
   * Additional User Journey Steps - Simulating actual user behavior
   */
  describe('Complete User Journey Flow', () => {
    test('Step 1: Landing - Hero section is immediately visible and clear', () => {
      const heroSection = document.querySelector('#hero');
      expect(heroSection).not.toBeNull();

      // Should have visible project name
      const projectName = heroSection.querySelector('h1');
      expect(projectName).not.toBeNull();
      expect(projectName.textContent.toLowerCase()).toContain('mirdb');
    });

    test('Step 2: Understanding - Value proposition within 5 seconds read', () => {
      const tagline = document.querySelector('#hero .tagline');
      expect(tagline).not.toBeNull();

      // Tagline should be concise (readable within 5 seconds)
      const taglineWords = tagline.textContent.trim().split(/\s+/).length;
      expect(taglineWords).toBeLessThanOrEqual(15);

      // Should mention key differentiators
      const heroText = document.querySelector('#hero').textContent.toLowerCase();
      expect(heroText).toContain('persistent');
      expect(heroText).toContain('memcached');
    });

    test('Step 3: Explore - Features section answers "why choose MirDB"', () => {
      const featuresSection = document.querySelector('#features');
      expect(featuresSection).not.toBeNull();

      const featureCards = featuresSection.querySelectorAll('.feature-card, [data-feature], article');
      expect(featureCards.length).toBeGreaterThanOrEqual(3);

      // Should cover the three key features
      const featuresText = featuresSection.textContent.toLowerCase();
      expect(featuresText).toContain('memcached');
      expect(featuresText).toContain('persistent');
      expect(featuresText).toContain('performance');
    });

    test('Step 4: Try - Quick-start is comprehensive and actionable', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).not.toBeNull();

      // Should have multiple code examples
      const codeBlocks = quickStartSection.querySelectorAll('pre code');
      expect(codeBlocks.length).toBeGreaterThanOrEqual(3);

      // Should have clear headings guiding the user
      const headings = quickStartSection.querySelectorAll('h2, h3');
      expect(headings.length).toBeGreaterThanOrEqual(2);
    });

    test('Step 5: Access - GitHub links are easily findable', () => {
      // GitHub should be accessible from multiple locations
      const heroGithub = document.querySelector('#hero a[href*="github.com"]');
      const navGithub = document.querySelector('nav a[href*="github.com"]');
      const footerGithub = document.querySelector('footer a[href*="github.com"]');

      // At least 2 of 3 locations should have GitHub link
      const githubLinkCount = [heroGithub, navGithub, footerGithub].filter(Boolean).length;
      expect(githubLinkCount).toBeGreaterThanOrEqual(2);
    });
  });
});
