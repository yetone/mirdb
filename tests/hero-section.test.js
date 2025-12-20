/**
 * Hero Section Display Tests
 *
 * These tests verify the hero section displays product name, tagline,
 * and call-to-action buttons correctly on the MirDB landing page.
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Hero Section Display', () => {
  beforeAll(() => {
    // Load the landing page HTML
    const htmlPath = path.join(__dirname, '..', 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document.documentElement.innerHTML = htmlContent;
  });

  // Test Case 1: Page contains 'MirDB' product name in prominent heading (h1)
  test('should display MirDB product name in h1 heading', () => {
    const h1Element = document.querySelector('h1');
    expect(h1Element).not.toBeNull();
    expect(h1Element.textContent).toContain('MirDB');
  });

  // Test Case 2: Tagline mentions 'persistent', 'key-value', and 'Memcached' concepts
  test('should display tagline with persistent, key-value, and Memcached concepts', () => {
    const heroSection = document.querySelector('.hero, [class*="hero"], header, section:first-of-type');
    expect(heroSection).not.toBeNull();

    const heroText = heroSection.textContent.toLowerCase();
    expect(heroText).toContain('persistent');
    expect(heroText).toContain('key-value');
    expect(heroText).toContain('memcached');
  });

  // Test Case 3: Primary CTA button exists with text like 'Get Started' or 'View on GitHub'
  test('should have primary CTA button with appropriate text', () => {
    const allLinks = document.querySelectorAll('a');

    let foundPrimaryCTA = false;
    const ctaTexts = ['get started', 'view on github', 'github', 'start'];

    // Check buttons/links for CTA text
    allLinks.forEach(link => {
      const text = link.textContent.toLowerCase().trim();
      ctaTexts.forEach(ctaText => {
        if (text.includes(ctaText)) {
          foundPrimaryCTA = true;
        }
      });
    });

    expect(foundPrimaryCTA).toBe(true);
  });

  // Test Case 4: GitHub CTA links to valid GitHub repository URL
  test('should have GitHub CTA link pointing to valid GitHub repository URL', () => {
    const allLinks = document.querySelectorAll('a');
    let foundGitHubLink = false;
    let gitHubUrl = null;

    allLinks.forEach(link => {
      const href = link.getAttribute('href');
      if (href && href.includes('github.com')) {
        foundGitHubLink = true;
        gitHubUrl = href;
      }
    });

    expect(foundGitHubLink).toBe(true);
    expect(gitHubUrl).toMatch(/^https:\/\/github\.com\/[\w-]+\/[\w-]+/);
  });
});
