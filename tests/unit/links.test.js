/**
 * Link Validation Unit Tests
 * Owner: Scenario 5 - Footer Content and Links (also used by Scenario 10)
 *
 * Test cases for footer link validation
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Footer Content and Links', () => {
  let document;
  let footer;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    footer = document.querySelector('footer');
  });

  describe('Test Case 1: Footer Element', () => {
    test('Footer semantic element exists', () => {
      expect(footer).not.toBeNull();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });
  });

  describe('Test Case 2: GitHub Repository Link', () => {
    test('Link with href="https://github.com/yetone/mirdb" exists', () => {
      const githubLink = footer.querySelector('a[href="https://github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });
  });

  describe('Test Case 3: GitHub Link Opens in New Tab', () => {
    test('GitHub link has target="_blank" attribute', () => {
      const githubLink = footer.querySelector('a[href="https://github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    test('GitHub link has rel="noopener noreferrer" for security', () => {
      const githubLink = footer.querySelector('a[href="https://github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
      const rel = githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });

  describe('Test Case 4: MIT License Text', () => {
    test('Text containing "MIT" appears in footer', () => {
      const footerText = footer.textContent;
      expect(footerText).toContain('MIT');
    });
  });

  describe('Test Case 5: Author Attribution', () => {
    test('Author name or attribution text appears in footer', () => {
      const footerText = footer.textContent;
      expect(footerText.toLowerCase()).toMatch(/yetone|created by|author/i);
    });

    test('Author link exists and points to author profile', () => {
      const authorLink = footer.querySelector('a[href*="github.com/yetone"]');
      expect(authorLink).not.toBeNull();
    });
  });

  describe('Test Case 6: CircleCI Badge', () => {
    test('Image element with CircleCI badge exists (src contains "circleci")', () => {
      const badges = footer.querySelectorAll('img');
      let circleciFound = false;
      badges.forEach(badge => {
        const src = badge.getAttribute('src');
        if (src && src.toLowerCase().includes('circleci')) {
          circleciFound = true;
        }
      });
      expect(circleciFound).toBe(true);
    });

    test('CircleCI badge has proper alt text for accessibility', () => {
      const badge = footer.querySelector('img[src*="circleci"]');
      expect(badge).not.toBeNull();
      expect(badge.getAttribute('alt')).toBeTruthy();
    });
  });

  describe('Test Case 7: Badge is Clickable', () => {
    test('CircleCI badge is wrapped in a link to CI results page', () => {
      const badgeLink = footer.querySelector('a[href*="circleci.com"]');
      expect(badgeLink).not.toBeNull();

      const badge = badgeLink.querySelector('img');
      expect(badge).not.toBeNull();
      expect(badge.getAttribute('src')).toContain('circleci');
    });

    test('CircleCI link opens in new tab with security attributes', () => {
      const badgeLink = footer.querySelector('a[href*="circleci.com"]');
      expect(badgeLink).not.toBeNull();
      expect(badgeLink.getAttribute('target')).toBe('_blank');
      const rel = badgeLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });
});
