/**
 * HTML Structure Unit Tests
 * Owner: Scenario 1 - Hero Section Display (also used by Scenario 2)
 *
 * Test cases for hero section HTML structure
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Hero Section Display', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('Test Case 2: Logo Image Element', () => {
    test('Logo image element exists with correct src attribute pointing to assets/logo.gif', () => {
      const logo = document.querySelector('img[src="assets/logo.gif"]');
      expect(logo).not.toBeNull();
      expect(logo.getAttribute('src')).toBe('assets/logo.gif');
    });

    test('Logo has alt attribute for accessibility', () => {
      const logo = document.querySelector('img[src="assets/logo.gif"]');
      expect(logo).not.toBeNull();
      expect(logo.getAttribute('alt')).toBeTruthy();
    });
  });

  describe('Test Case 3: H1 Title Element', () => {
    test('H1 element exists with text content "MirDB"', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toBe('MirDB');
    });
  });

  describe('Test Case 4: Tagline Text', () => {
    test('Element containing "A Persistent Key-Value Store with Memcached Protocol" exists', () => {
      const tagline = document.querySelector('.tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent).toBe('A Persistent Key-Value Store with Memcached Protocol');
    });
  });

  describe('Test Case 5: View on GitHub Button', () => {
    test('Button/link with text "View on GitHub" and href containing "github.com/yetone/mirdb" exists', () => {
      const links = document.querySelectorAll('a');
      let githubLink = null;

      links.forEach(link => {
        if (link.textContent.trim() === 'View on GitHub') {
          githubLink = link;
        }
      });

      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('href')).toContain('github.com/yetone/mirdb');
    });

    test('GitHub link opens in new tab with security attributes', () => {
      const githubLink = document.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
      expect(githubLink.getAttribute('rel')).toContain('noopener');
    });
  });

  describe('Test Case 6: Get Started Button', () => {
    test('Button/link with text "Get Started" exists and links to quick start section', () => {
      const links = document.querySelectorAll('a');
      let getStartedLink = null;

      links.forEach(link => {
        if (link.textContent.trim() === 'Get Started') {
          getStartedLink = link;
        }
      });

      expect(getStartedLink).not.toBeNull();
      expect(getStartedLink.getAttribute('href')).toBe('#quickstart');
    });
  });

  describe('HTML5 Structure', () => {
    test('Document has proper lang attribute', () => {
      const html = document.querySelector('html');
      expect(html.getAttribute('lang')).toBe('en');
    });

    test('Document has viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    test('Header element exists', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    test('Main element exists', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    test('Footer element exists', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });
  });
});
