/**
 * Tests for Project Status Badges (CI/build status)
 * Verifies that project status badges are displayed correctly
 * REQ-7: Should display CI/build status badges
 */

const fs = require('fs');
const path = require('path');

describe('Project Status Badges', () => {
  let htmlContent;
  let document;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Parse HTML using jsdom
    const { JSDOM } = require('jsdom');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Test Case 1: CircleCI badge image present and loaded', () => {
    test('CircleCI badge image should be present in the badges section', () => {
      // Find the badges container
      const badgesSection = document.querySelector('.badges');
      expect(badgesSection).not.toBeNull();

      // Find the CircleCI badge image
      const badgeImg = badgesSection.querySelector('img');
      expect(badgeImg).not.toBeNull();

      // Verify it's a CircleCI badge
      const imgSrc = badgeImg.getAttribute('src');
      expect(imgSrc).toContain('circleci.com');
      expect(imgSrc).toContain('mirdb');
    });

    test('CircleCI badge image should have proper alt text', () => {
      const badgesSection = document.querySelector('.badges');
      const badgeImg = badgesSection.querySelector('img');

      const altText = badgeImg.getAttribute('alt');
      expect(altText).not.toBeNull();
      expect(altText.toLowerCase()).toContain('circleci');
    });

    test('CircleCI badge image URL should use shield style', () => {
      const badgesSection = document.querySelector('.badges');
      const badgeImg = badgesSection.querySelector('img');

      const imgSrc = badgeImg.getAttribute('src');
      expect(imgSrc).toContain('style=shield');
    });
  });

  describe('Test Case 2: Badge links to CircleCI build page', () => {
    test('CI badge should be wrapped in a link to CircleCI build page', () => {
      const badgesSection = document.querySelector('.badges');
      const badgeLink = badgesSection.querySelector('a');

      expect(badgeLink).not.toBeNull();

      // Verify link points to CircleCI
      const href = badgeLink.getAttribute('href');
      expect(href).toContain('circleci.com');
      expect(href).toContain('mirdb');
    });

    test('Badge link should open in new tab for security', () => {
      const badgesSection = document.querySelector('.badges');
      const badgeLink = badgesSection.querySelector('a');

      expect(badgeLink.getAttribute('target')).toBe('_blank');
      expect(badgeLink.getAttribute('rel')).toContain('noopener');
    });

    test('Badge link URL should match the repository build page pattern', () => {
      const badgesSection = document.querySelector('.badges');
      const badgeLink = badgesSection.querySelector('a');

      const href = badgeLink.getAttribute('href');
      // CircleCI build pages follow pattern: https://circleci.com/gh/{owner}/{repo}
      expect(href).toBe('https://circleci.com/gh/yetone/mirdb');
    });
  });

  describe('Test Case 3: Badge image loads correctly (not broken)', () => {
    test('Badge image should have valid src URL format', () => {
      const badgesSection = document.querySelector('.badges');
      const badgeImg = badgesSection.querySelector('img');

      const imgSrc = badgeImg.getAttribute('src');

      // Verify it's a valid URL format
      expect(imgSrc).toMatch(/^https?:\/\//);

      // Verify it's an SVG badge (CircleCI badges are SVGs)
      expect(imgSrc).toContain('.svg');
    });

    test('Badge image should have correct CircleCI SVG endpoint', () => {
      const badgesSection = document.querySelector('.badges');
      const badgeImg = badgesSection.querySelector('img');

      const imgSrc = badgeImg.getAttribute('src');

      // CircleCI badge format: https://circleci.com/gh/{owner}/{repo}.svg?style=shield
      expect(imgSrc).toBe('https://circleci.com/gh/yetone/mirdb.svg?style=shield');
    });

    test('Badge should be in a visible section of the page', () => {
      const badgesSection = document.querySelector('.badges');

      // Badge section should exist
      expect(badgesSection).not.toBeNull();

      // Badges should be in the hero/header area (within first major section)
      const heroSection = document.querySelector('.hero, #hero');
      expect(heroSection).not.toBeNull();

      // Check that badges section is within or after hero
      const badgesInHero = heroSection.contains(badgesSection);
      expect(badgesInHero).toBe(true);
    });

    test('Badges section should have proper styling class', () => {
      const badgesSection = document.querySelector('.badges');

      // Verify badges container exists with proper class
      expect(badgesSection.classList.contains('badges')).toBe(true);
    });
  });

  describe('Additional Badge Validations', () => {
    test('Badge container should use flexbox for proper layout', () => {
      // This test verifies the CSS structure defined for badges
      // The .badges class should have display:flex and gap for proper spacing
      const styleContent = document.querySelector('style');
      expect(styleContent).not.toBeNull();

      const styleText = styleContent.textContent;
      expect(styleText).toContain('.badges');
      expect(styleText).toContain('display: flex');
    });

    test('Badge should be accessible with descriptive alt text', () => {
      const badgesSection = document.querySelector('.badges');
      const badgeImg = badgesSection.querySelector('img');

      const altText = badgeImg.getAttribute('alt');

      // Alt text should describe what the badge represents
      expect(altText.length).toBeGreaterThan(5);
      expect(altText.toLowerCase()).toMatch(/build|status|circleci/);
    });
  });
});
