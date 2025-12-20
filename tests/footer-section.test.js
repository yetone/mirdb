/**
 * Footer Section Tests
 *
 * These tests verify that the footer section contains required links and information
 * for the MirDB landing page.
 *
 * Test Cases:
 * 1. Page has footer element
 * 2. Footer contains GitHub repository link
 * 3. Footer displays license information
 * 4. Footer includes copyright or MirDB attribution
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Footer Section', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the landing page HTML
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    global.document.documentElement.innerHTML = htmlContent;
    document = global.document;
  });

  /**
   * Test Case 1: Check for footer element
   * Expected: Page has footer element
   */
  test('TC1: Page has footer element', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();
    expect(footer.tagName.toLowerCase()).toBe('footer');
  });

  /**
   * Test Case 2: Verify GitHub link in footer
   * Expected: Footer contains GitHub repository link
   */
  test('TC2: Footer contains GitHub repository link', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();

    const footerLinks = footer.querySelectorAll('a');
    let hasGitHubLink = false;
    let gitHubUrl = null;

    footerLinks.forEach(link => {
      const href = link.getAttribute('href');
      if (href && href.includes('github.com')) {
        hasGitHubLink = true;
        gitHubUrl = href;
      }
    });

    expect(hasGitHubLink).toBe(true);
    // Verify it's a valid GitHub URL format
    expect(gitHubUrl).toMatch(/^https:\/\/github\.com\//);
  });

  /**
   * Test Case 3: Check for license information
   * Expected: Footer displays license information
   */
  test('TC3: Footer displays license information', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();

    const footerText = footer.textContent.toLowerCase();

    // Check for common license terms
    const hasLicenseInfo =
      footerText.includes('license') ||
      footerText.includes('mit') ||
      footerText.includes('apache') ||
      footerText.includes('gpl') ||
      footerText.includes('bsd') ||
      footerText.includes('open source');

    expect(hasLicenseInfo).toBe(true);
  });

  /**
   * Test Case 4: Verify copyright or project attribution
   * Expected: Footer includes copyright or MirDB attribution
   */
  test('TC4: Footer includes copyright or MirDB attribution', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();

    const footerText = footer.textContent.toLowerCase();

    // Check for copyright symbol, year, or project name attribution
    const hasAttribution =
      footerText.includes('mirdb') ||
      footerText.includes('copyright') ||
      footerText.includes('\u00a9') || // copyright symbol
      footerText.includes('(c)') ||
      /\d{4}/.test(footerText); // year pattern

    expect(hasAttribution).toBe(true);
  });

  /**
   * Additional Test: Footer has proper structure with container
   */
  test('Footer has proper structure with container', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();

    const container = footer.querySelector('.container');
    expect(container).not.toBeNull();
  });

  /**
   * Additional Test: Footer links are accessible with href attributes
   */
  test('Footer links have valid href attributes', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();

    const footerLinks = footer.querySelectorAll('a');
    expect(footerLinks.length).toBeGreaterThan(0);

    footerLinks.forEach(link => {
      const href = link.getAttribute('href');
      expect(href).not.toBeNull();
      expect(href).not.toBe('');
      // Links should be valid URLs (starting with http/https or #)
      expect(href).toMatch(/^(https?:\/\/|#)/);
    });
  });

  /**
   * Additional Test: Footer contains Issues link for community engagement
   */
  test('Footer contains Issues or community link', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();

    const footerLinks = footer.querySelectorAll('a');
    let hasCommunityLink = false;

    footerLinks.forEach(link => {
      const href = link.getAttribute('href') || '';
      const text = link.textContent.toLowerCase();

      if (
        href.includes('issues') ||
        href.includes('discussions') ||
        text.includes('issue') ||
        text.includes('community') ||
        text.includes('support')
      ) {
        hasCommunityLink = true;
      }
    });

    expect(hasCommunityLink).toBe(true);
  });

  /**
   * Additional Test: Footer is styled appropriately (has background color)
   */
  test('Footer has appropriate styling defined in CSS', () => {
    // Check that footer styles exist in the HTML/CSS
    const hasFooterStyles =
      htmlContent.includes('footer {') ||
      htmlContent.includes('footer{');

    expect(hasFooterStyles).toBe(true);
  });

  /**
   * Additional Test: Footer text is readable (paragraph elements present)
   */
  test('Footer contains readable text content', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();

    const paragraphs = footer.querySelectorAll('p');
    expect(paragraphs.length).toBeGreaterThan(0);

    // Check that paragraphs have actual content
    paragraphs.forEach(p => {
      expect(p.textContent.trim().length).toBeGreaterThan(0);
    });
  });
});
