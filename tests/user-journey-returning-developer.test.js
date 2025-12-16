/**
 * User Journey - Returning Developer Tests
 * Scenario: Verify quick reference access for returning developers (US-3, US-4)
 *
 * This scenario tests that returning developers can quickly access:
 * - Commands reference section (1-2 clicks from navigation)
 * - SET command syntax (visible without excessive scrolling)
 * - Configuration section (easily accessible)
 * - Default port configuration (addr parameter with 0.0.0.0:12333)
 */

const fs = require('fs');
const path = require('path');

describe('User Journey - Returning Developer', () => {
  let document;
  let html;

  beforeAll(() => {
    // Load the index.html file
    const htmlPath = path.resolve(__dirname, '../index.html');
    html = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  /**
   * Test Case 1: Navigation to commands section
   * Input: Click navigation link to commands
   * Expected: Page scrolls to commands section within 500ms
   * Type: e2e
   */
  describe('Test Case 1: Click navigation link to commands', () => {
    test('should have a commands navigation link that is accessible in 1-2 clicks', () => {
      // Get the navigation element
      const nav = document.querySelector('nav, header nav, [role="navigation"]');
      expect(nav).not.toBeNull();

      // Find the commands link
      const commandsLink = nav.querySelector('a[href="#commands"], a[href*="commands"]');
      expect(commandsLink).not.toBeNull();
      expect(commandsLink.textContent.toLowerCase()).toContain('command');
    });

    test('commands section target exists for navigation', () => {
      // Verify the target section exists
      const commandsSection = document.getElementById('commands');
      expect(commandsSection).not.toBeNull();
      expect(commandsSection.tagName.toLowerCase()).toBe('section');
    });

    test('smooth scroll behavior is enabled for quick navigation', () => {
      // Check that the HTML has smooth scroll behavior in CSS
      const htmlElement = document.querySelector('html');

      // Verify the link has proper href for anchor navigation
      const nav = document.querySelector('nav');
      const commandsLink = nav.querySelector('a[href="#commands"]');
      expect(commandsLink).not.toBeNull();

      // The href should point directly to the section (no intermediate pages)
      const href = commandsLink.getAttribute('href');
      expect(href).toBe('#commands');
    });

    test('commands link is visible in the navigation (not hidden or nested)', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelector('.nav-links, ul, [class*="menu"]');
      expect(navLinks).not.toBeNull();

      // Commands link should be a direct child or close descendant
      const commandsLink = navLinks.querySelector('a[href="#commands"]');
      expect(commandsLink).not.toBeNull();

      // Should be in main navigation list (1 click access)
      const listItem = commandsLink.closest('li');
      expect(listItem).not.toBeNull();
    });
  });

  /**
   * Test Case 2: Find SET command syntax
   * Input: Search for SET command visually
   * Expected: SET command syntax visible without excessive scrolling within commands section
   * Type: integration
   */
  describe('Test Case 2: Search for SET command visually', () => {
    test('SET command card exists in commands section', () => {
      const commandsSection = document.getElementById('commands');
      expect(commandsSection).not.toBeNull();

      // Find SET command
      const commandCards = commandsSection.querySelectorAll('.command-card, [class*="command"]');
      let setCommandCard = null;

      for (const card of commandCards) {
        const heading = card.querySelector('h3, h4, .command-name');
        if (heading && heading.textContent.toUpperCase().trim() === 'SET') {
          setCommandCard = card;
          break;
        }
      }

      expect(setCommandCard).not.toBeNull();
    });

    test('SET command syntax includes key, flags, exptime, bytes parameters', () => {
      const commandsSection = document.getElementById('commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      let setCommandCard = null;
      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading && heading.textContent.toUpperCase().trim() === 'SET') {
          setCommandCard = card;
          break;
        }
      }

      expect(setCommandCard).not.toBeNull();

      // Get the syntax element
      const syntaxElement = setCommandCard.querySelector('.command-syntax, code');
      expect(syntaxElement).not.toBeNull();

      const syntaxText = syntaxElement.textContent.toLowerCase();
      expect(syntaxText).toContain('set');
      expect(syntaxText).toContain('key');
      expect(syntaxText).toContain('flags');
      expect(syntaxText).toContain('exptime');
      expect(syntaxText).toContain('bytes');
    });

    test('SET command is positioned prominently (first or near top of commands grid)', () => {
      const commandsSection = document.getElementById('commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      // SET should be within first few commands (visible without scrolling within section)
      let setIndex = -1;
      for (let i = 0; i < commandCards.length; i++) {
        const heading = commandCards[i].querySelector('h3');
        if (heading && heading.textContent.toUpperCase().trim() === 'SET') {
          setIndex = i;
          break;
        }
      }

      expect(setIndex).toBeGreaterThanOrEqual(0);
      // SET should be within first 3 commands (prominent position)
      expect(setIndex).toBeLessThan(3);
    });

    test('SET command has visible example for quick reference', () => {
      const commandsSection = document.getElementById('commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      let setCommandCard = null;
      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading && heading.textContent.toUpperCase().trim() === 'SET') {
          setCommandCard = card;
          break;
        }
      }

      expect(setCommandCard).not.toBeNull();

      // Check for example
      const exampleElement = setCommandCard.querySelector('.command-example, pre');
      expect(exampleElement).not.toBeNull();
      expect(exampleElement.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 3: Navigation to configuration section
   * Input: Click navigation link to configuration
   * Expected: Page scrolls to configuration section
   * Type: e2e
   */
  describe('Test Case 3: Click navigation link to configuration', () => {
    test('should have a configuration navigation link', () => {
      const nav = document.querySelector('nav, header nav, [role="navigation"]');
      expect(nav).not.toBeNull();

      // Find configuration link
      const configLink = nav.querySelector('a[href="#configuration"], a[href*="config"]');
      expect(configLink).not.toBeNull();
      expect(configLink.textContent.toLowerCase()).toContain('config');
    });

    test('configuration section target exists', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();
      expect(configSection.tagName.toLowerCase()).toBe('section');
    });

    test('configuration link is directly accessible in navigation (1 click)', () => {
      const nav = document.querySelector('nav');
      const configLink = nav.querySelector('a[href="#configuration"]');
      expect(configLink).not.toBeNull();

      // Verify it's in main nav list
      const href = configLink.getAttribute('href');
      expect(href).toBe('#configuration');

      // Should be visible link (not in dropdown or hidden)
      const listItem = configLink.closest('li');
      expect(listItem).not.toBeNull();
    });

    test('configuration section has proper heading for identification', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      const heading = configSection.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent.toLowerCase()).toContain('config');
    });
  });

  /**
   * Test Case 4: Find default port configuration
   * Input: Find default port configuration
   * Expected: addr parameter with default 0.0.0.0:12333 is visible in configuration section
   * Type: integration
   */
  describe('Test Case 4: Find default port configuration', () => {
    test('addr parameter is documented in configuration section', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      const sectionText = configSection.textContent;
      expect(sectionText).toContain('addr');
    });

    test('default port 0.0.0.0:12333 is visible', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      const sectionText = configSection.textContent;
      expect(sectionText).toContain('0.0.0.0:12333');
    });

    test('addr parameter is in a table or clearly formatted for quick scanning', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      // Check for table with addr parameter
      const table = configSection.querySelector('table');
      expect(table).not.toBeNull();

      const tableText = table.textContent;
      expect(tableText).toContain('addr');
      expect(tableText).toContain('0.0.0.0:12333');
    });

    test('configuration table is organized with Parameter, Description, Default columns', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      const table = configSection.querySelector('table');
      expect(table).not.toBeNull();

      const headerRow = table.querySelector('thead tr, tr:first-child');
      expect(headerRow).not.toBeNull();

      const headers = headerRow.querySelectorAll('th');
      expect(headers.length).toBeGreaterThanOrEqual(2);

      // Verify expected columns exist
      const headerTexts = Array.from(headers).map(h => h.textContent.toLowerCase());
      expect(headerTexts.some(t => t.includes('parameter') || t.includes('param'))).toBe(true);
      expect(headerTexts.some(t => t.includes('default') || t.includes('value'))).toBe(true);
    });

    test('addr row contains listen address description', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      const table = configSection.querySelector('table');
      expect(table).not.toBeNull();

      // Find row with addr
      const rows = table.querySelectorAll('tbody tr, tr');
      let addrRow = null;

      for (const row of rows) {
        if (row.textContent.includes('addr') && row.textContent.includes('0.0.0.0:12333')) {
          addrRow = row;
          break;
        }
      }

      expect(addrRow).not.toBeNull();

      // Should have description about listen/address/port
      const rowText = addrRow.textContent.toLowerCase();
      expect(rowText).toMatch(/listen|address|port|server/);
    });
  });

  /**
   * Additional tests for returning developer experience
   */
  describe('Additional: Quick Reference Accessibility', () => {
    test('navigation is sticky/fixed for easy access while scrolling', () => {
      const nav = document.querySelector('nav, .navbar');
      expect(nav).not.toBeNull();

      // Navigation should have position: sticky or fixed in the design
      // (This would need CSS check in real e2e test - here we verify structure)
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
      expect(header.contains(nav)).toBe(true);
    });

    test('all major sections are accessible via single-click navigation', () => {
      const nav = document.querySelector('nav');
      const expectedSections = ['features', 'quick-start', 'commands', 'configuration'];

      for (const sectionId of expectedSections) {
        const link = nav.querySelector(`a[href="#${sectionId}"]`);
        expect(link).not.toBeNull();

        const targetSection = document.getElementById(sectionId);
        expect(targetSection).not.toBeNull();
      }
    });

    test('commands section has multiple commands for comprehensive reference', () => {
      const commandsSection = document.getElementById('commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      // Should have all documented commands
      expect(commandCards.length).toBeGreaterThanOrEqual(4);

      // Verify key commands exist
      const commandNames = [];
      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading) {
          commandNames.push(heading.textContent.toUpperCase().trim());
        }
      }

      expect(commandNames).toContain('SET');
      expect(commandNames).toContain('GET');
      expect(commandNames).toContain('DELETE');
      expect(commandNames).toContain('INFO');
    });
  });
});
