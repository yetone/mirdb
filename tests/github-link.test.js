/**
 * GitHub Repository Link Tests
 *
 * These tests verify that GitHub repository links are prominently displayed
 * and functional on the MirDB landing page (REQ-6).
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('GitHub Repository Link', () => {
  let document;

  beforeAll(() => {
    // Load the landing page HTML
    const htmlPath = path.join(__dirname, '..', 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    global.document.documentElement.innerHTML = htmlContent;
    document = global.document;
  });

  // Test Case 1: Page contains at least one link to github.com
  test('should contain at least one link to github.com', () => {
    const allLinks = document.querySelectorAll('a');
    let githubLinkCount = 0;

    allLinks.forEach(link => {
      const href = link.getAttribute('href');
      if (href && href.includes('github.com')) {
        githubLinkCount++;
      }
    });

    expect(githubLinkCount).toBeGreaterThanOrEqual(1);
  });

  // Test Case 2: Hero section CTA includes GitHub link
  test('should have GitHub link in hero section CTA', () => {
    const heroSection = document.querySelector('.hero');
    expect(heroSection).not.toBeNull();

    const heroLinks = heroSection.querySelectorAll('a');
    let foundGitHubCTA = false;

    heroLinks.forEach(link => {
      const href = link.getAttribute('href');
      if (href && href.includes('github.com')) {
        foundGitHubCTA = true;
      }
    });

    expect(foundGitHubCTA).toBe(true);
  });

  // Test Case 3: Footer contains GitHub repository link
  test('should have GitHub link in footer', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();

    const footerLinks = footer.querySelectorAll('a');
    let foundFooterGitHubLink = false;

    footerLinks.forEach(link => {
      const href = link.getAttribute('href');
      if (href && href.includes('github.com')) {
        foundFooterGitHubLink = true;
      }
    });

    expect(foundFooterGitHubLink).toBe(true);
  });

  // Test Case 4: GitHub links use valid URL format (https://github.com/...)
  test('should have GitHub links with valid URL format', () => {
    const allLinks = document.querySelectorAll('a');
    const githubUrls = [];

    allLinks.forEach(link => {
      const href = link.getAttribute('href');
      if (href && href.includes('github.com')) {
        githubUrls.push(href);
      }
    });

    expect(githubUrls.length).toBeGreaterThan(0);

    // Verify each GitHub URL matches the expected format
    const validUrlPattern = /^https:\/\/github\.com\/[\w-]+\/[\w-]+(\/.*)?$/;
    githubUrls.forEach(url => {
      expect(url).toMatch(validUrlPattern);
    });
  });
});
