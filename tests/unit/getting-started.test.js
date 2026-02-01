/**
 * Getting Started Section Tests
 * Owner: Scenario 4 - Getting Started Section
 *
 * Tests:
 * - Getting started section exists
 * - Installation instructions present
 * - Documentation link present
 * - Quick start example present
 */
const { loadHTML, querySection, containsText } = require('../helpers/dom-utils');

describe('Getting Started Section', () => {
  beforeEach(() => {
    loadHTML('index.html');
  });

  describe('Section Structure', () => {
    test('getting started section exists with installation instructions', () => {
      const section = querySection('getting-started');

      expect(section).not.toBeNull();
      expect(section.tagName.toLowerCase()).toBe('section');

      // Should have installation instructions content
      expect(containsText(section, 'cargo install mirdb')).toBe(true);
    });

    test('section has proper heading', () => {
      const section = querySection('getting-started');
      const heading = section.querySelector('h2');

      expect(heading).not.toBeNull();
      expect(heading.textContent).toContain('Getting Started');
    });
  });

  describe('Documentation Links', () => {
    test('link to documentation or README exists', () => {
      const section = querySection('getting-started');
      const links = section.querySelectorAll('a');

      // Should have at least one link (documentation link)
      expect(links.length).toBeGreaterThan(0);

      // Check for documentation-related link
      const docLink = Array.from(links).find(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        return href.includes('github.com') ||
               href.includes('docs') ||
               href.includes('readme') ||
               href.includes('#') ||
               text.includes('documentation') ||
               text.includes('docs') ||
               text.includes('readme') ||
               text.includes('learn more') ||
               text.includes('full guide');
      });

      expect(docLink).toBeDefined();
    });

    test('documentation link has proper attributes for external links', () => {
      const section = querySection('getting-started');
      const externalLinks = Array.from(section.querySelectorAll('a[href^="http"]'));

      externalLinks.forEach(link => {
        expect(link.getAttribute('target')).toBe('_blank');
        expect(link.getAttribute('rel')).toContain('noopener');
      });
    });
  });

  describe('Quick Start Example', () => {
    test('quick start code example is present showing basic usage', () => {
      const section = querySection('getting-started');

      // Should have code blocks
      const codeBlocks = section.querySelectorAll('code, pre');
      expect(codeBlocks.length).toBeGreaterThan(0);

      // Should show installation command
      expect(containsText(section, 'cargo install')).toBe(true);

      // Should show server start command
      expect(containsText(section, 'mirdb-server')).toBe(true);
    });

    test('code example demonstrates basic Memcached client usage', () => {
      const section = querySection('getting-started');

      // Should have usage example with Memcached command
      expect(containsText(section, 'set') || containsText(section, 'get')).toBe(true);
    });

    test('code blocks have syntax highlighting language class', () => {
      const section = querySection('getting-started');
      const codeElements = section.querySelectorAll('code');

      const hasLanguageClass = Array.from(codeElements).some(code => {
        return code.classList.contains('language-bash') ||
               code.classList.contains('language-shell') ||
               code.className.includes('language-');
      });

      expect(hasLanguageClass).toBe(true);
    });
  });

  describe('Installation Instructions', () => {
    test('shows cargo installation method', () => {
      const section = querySection('getting-started');
      expect(containsText(section, 'cargo install mirdb')).toBe(true);
    });

    test('shows server startup command', () => {
      const section = querySection('getting-started');
      expect(containsText(section, 'mirdb-server')).toBe(true);
      expect(containsText(section, '--port')).toBe(true);
    });
  });
});
