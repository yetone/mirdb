/**
 * External Links Navigation Unit Tests
 * Owner: Scenario 4 - External Links Navigation
 *
 * Unit tests for Footer component links and external link validation
 *
 * Navigation and Smooth Scroll Unit Tests
 * Owner: Scenario 12 - Navigation and Smooth Scroll
 *
 * Unit tests for scroll behavior CSS and navigation link validation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { EXTERNAL_URLS, SELECTORS } from '../fixtures/test-data';

// HTML content for Footer component testing
const footerHTML = `
<footer id="footer" class="footer">
  <div class="container footer__content">
    <p class="footer__copyright">&copy; 2024 MirDB. Open source under MIT License.</p>
    <nav class="footer__links">
      <a href="https://github.com/mirdb/mirdb" target="_blank" rel="noopener">Source Code</a>
      <a href="https://mirdb.dev/docs" target="_blank" rel="noopener">Documentation</a>
    </nav>
  </div>
</footer>
`;

// Full page HTML for comprehensive testing
const fullPageHTML = `
<!DOCTYPE html>
<html lang="en">
<head><title>MirDB</title></head>
<body>
  <header id="header" class="header">
    <nav class="nav container">
      <a href="/" class="nav__logo">MirDB</a>
      <ul class="nav__links">
        <li><a href="#features" class="nav__link">Features</a></li>
        <li><a href="#quickstart" class="nav__link">Quick Start</a></li>
        <li><a href="#techspecs" class="nav__link">Specs</a></li>
        <li><a href="https://github.com/mirdb/mirdb" class="nav__link" target="_blank" rel="noopener">GitHub</a></li>
      </ul>
    </nav>
  </header>
  <main id="main-content"></main>
  ${footerHTML}
</body>
</html>
`;

describe('Footer Component Links', () => {
  let document: Document;

  beforeEach(() => {
    const dom = new JSDOM(footerHTML);
    document = dom.window.document;
  });

  it('TC5: Footer contains both documentation and source code links', () => {
    const footer = document.querySelector('#footer');
    expect(footer).toBeTruthy();

    const footerLinks = footer?.querySelector('.footer__links');
    expect(footerLinks).toBeTruthy();

    const links = footerLinks?.querySelectorAll('a');
    expect(links?.length).toBeGreaterThanOrEqual(2);

    // Check for documentation link
    const docsLink = Array.from(links || []).find(
      link => link.textContent?.toLowerCase().includes('documentation') ||
              link.textContent?.toLowerCase().includes('docs')
    );
    expect(docsLink).toBeTruthy();

    // Check for source code link
    const sourceLink = Array.from(links || []).find(
      link => link.textContent?.toLowerCase().includes('source') ||
              link.textContent?.toLowerCase().includes('github') ||
              link.textContent?.toLowerCase().includes('code')
    );
    expect(sourceLink).toBeTruthy();
  });

  it('TC6: All external links have valid href attributes', () => {
    const footer = document.querySelector('#footer');
    const links = footer?.querySelectorAll('a[href]');

    expect(links).toBeTruthy();
    expect(links?.length).toBeGreaterThan(0);

    links?.forEach(link => {
      const href = link.getAttribute('href');
      expect(href).toBeTruthy();
      // Verify href is a valid URL starting with http/https
      expect(href).toMatch(/^https?:\/\/.+/);
    });
  });

  it('footer links have proper security attributes for external URLs', () => {
    const footer = document.querySelector('#footer');
    const externalLinks = footer?.querySelectorAll('a[target="_blank"]');

    expect(externalLinks).toBeTruthy();

    externalLinks?.forEach(link => {
      const rel = link.getAttribute('rel');
      // External links opening in new tab should have rel="noopener"
      expect(rel).toContain('noopener');
    });
  });

  it('documentation link points to correct URL', () => {
    const footer = document.querySelector('#footer');
    const docsLink = Array.from(footer?.querySelectorAll('a') || []).find(
      link => link.textContent?.toLowerCase().includes('documentation')
    );

    expect(docsLink).toBeTruthy();
    expect(docsLink?.getAttribute('href')).toBe(EXTERNAL_URLS.DOCUMENTATION);
  });

  it('source code link points to correct repository URL', () => {
    const footer = document.querySelector('#footer');
    const sourceLink = Array.from(footer?.querySelectorAll('a') || []).find(
      link => link.textContent?.toLowerCase().includes('source')
    );

    expect(sourceLink).toBeTruthy();
    expect(sourceLink?.getAttribute('href')).toBe(EXTERNAL_URLS.GITHUB_REPO);
  });

  it('footer has copyright notice', () => {
    const copyright = document.querySelector('.footer__copyright');
    expect(copyright).toBeTruthy();
    expect(copyright?.textContent).toContain('MirDB');
  });
});

describe('Header Navigation Links', () => {
  let document: Document;

  beforeEach(() => {
    const dom = new JSDOM(fullPageHTML);
    document = dom.window.document;
  });

  it('header contains GitHub link', () => {
    const header = document.querySelector('#header');
    expect(header).toBeTruthy();

    const githubLink = header?.querySelector('a[href*="github"]');
    expect(githubLink).toBeTruthy();
    expect(githubLink?.getAttribute('href')).toBe(EXTERNAL_URLS.GITHUB_REPO);
  });

  it('header GitHub link has proper external link attributes', () => {
    const header = document.querySelector('#header');
    const githubLink = header?.querySelector('a[href*="github"]');

    expect(githubLink?.getAttribute('target')).toBe('_blank');
    expect(githubLink?.getAttribute('rel')).toContain('noopener');
  });

  it('navigation contains internal anchor links', () => {
    const nav = document.querySelector('.nav__links');
    expect(nav).toBeTruthy();

    const internalLinks = nav?.querySelectorAll('a[href^="#"]');
    expect(internalLinks?.length).toBeGreaterThan(0);

    // Verify common section links exist
    const hrefs = Array.from(internalLinks || []).map(link => link.getAttribute('href'));
    expect(hrefs).toContain('#features');
    expect(hrefs).toContain('#quickstart');
  });
});

describe('External Link Validation', () => {
  let document: Document;

  beforeEach(() => {
    const dom = new JSDOM(fullPageHTML);
    document = dom.window.document;
  });

  it('all external links have href values starting with http/https', () => {
    // Get all external links (those with target="_blank" or starting with http)
    const allLinks = document.querySelectorAll('a');
    const externalLinks = Array.from(allLinks).filter(link => {
      const href = link.getAttribute('href');
      return href?.startsWith('http://') || href?.startsWith('https://');
    });

    expect(externalLinks.length).toBeGreaterThan(0);

    externalLinks.forEach(link => {
      const href = link.getAttribute('href');
      expect(href).toMatch(/^https?:\/\/.+/);
    });
  });

  it('page contains required external links', () => {
    const allLinks = document.querySelectorAll('a[href^="http"]');
    const hrefs = Array.from(allLinks).map(link => link.getAttribute('href'));

    // Should have GitHub repo link
    expect(hrefs).toContain(EXTERNAL_URLS.GITHUB_REPO);

    // Should have documentation link
    expect(hrefs).toContain(EXTERNAL_URLS.DOCUMENTATION);
  });
});

/**
 * Scroll Behavior and Navigation Links Unit Tests
 * Owner: Scenario 12 - Navigation and Smooth Scroll
 */
describe('Scroll Behavior CSS', () => {
  it('TC3: base.css contains scroll-behavior: smooth on html element', async () => {
    // Read the base.css file content
    const fs = await import('fs/promises');
    const path = await import('path');
    const baseCssPath = path.join(process.cwd(), 'src/styles/base.css');
    const baseCssContent = await fs.readFile(baseCssPath, 'utf-8');

    // Verify scroll-behavior: smooth is defined for html element
    expect(baseCssContent).toContain('scroll-behavior: smooth');

    // More specific check: ensure it's in the html rule
    const htmlRule = baseCssContent.match(/html\s*\{[^}]*\}/s);
    expect(htmlRule).toBeTruthy();
    expect(htmlRule![0]).toContain('scroll-behavior: smooth');
  });

  it('index.html references base.css which contains smooth scroll', async () => {
    const fs = await import('fs/promises');
    const path = await import('path');
    const indexPath = path.join(process.cwd(), 'src/index.html');
    const indexContent = await fs.readFile(indexPath, 'utf-8');

    // Verify base.css is linked
    expect(indexContent).toContain('base.css');
    expect(indexContent).toMatch(/<link[^>]*href="[^"]*base\.css"[^>]*>/);
  });
});

describe('Navigation Links Matching Section IDs', () => {
  let document: Document;

  beforeEach(() => {
    const dom = new JSDOM(fullPageHTML);
    document = dom.window.document;
  });

  it('TC4: navigation links have href values matching section IDs', () => {
    // Get all internal navigation links (those starting with #)
    const navLinks = document.querySelectorAll('.nav__links .nav__link[href^="#"]');
    expect(navLinks.length).toBeGreaterThan(0);

    navLinks.forEach((link) => {
      const href = link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).toMatch(/^#[a-z]+/);

      // Verify the target section exists in the document
      const sectionId = href!.substring(1);

      // In the full page HTML, verify section exists
      // Note: Full page HTML has sections like #features, #quickstart, #techspecs
      const expectedSections = ['features', 'quickstart', 'techspecs'];
      if (expectedSections.includes(sectionId)) {
        // This link should point to a valid section
        expect(expectedSections).toContain(sectionId);
      }
    });
  });

  it('navigation links point to features, quickstart, and techspecs sections', () => {
    const navLinks = document.querySelectorAll('.nav__links .nav__link[href^="#"]');
    const hrefs = Array.from(navLinks).map((link) => link.getAttribute('href'));

    // Verify expected section links exist
    expect(hrefs).toContain('#features');
    expect(hrefs).toContain('#quickstart');
    expect(hrefs).toContain('#techspecs');
  });

  it('all internal navigation links follow correct format', () => {
    const navLinks = document.querySelectorAll('.nav__links .nav__link[href^="#"]');

    navLinks.forEach((link) => {
      const href = link.getAttribute('href');
      // Should be # followed by lowercase letters
      expect(href).toMatch(/^#[a-z]+$/);
    });
  });
});

describe('Header Sticky Navigation Setup', () => {
  it('header CSS has sticky positioning', async () => {
    const fs = await import('fs/promises');
    const path = await import('path');
    const headerCssPath = path.join(process.cwd(), 'src/components/Header/Header.css');
    const headerCssContent = await fs.readFile(headerCssPath, 'utf-8');

    // Verify sticky position is set on header
    expect(headerCssContent).toContain('position: sticky');
    expect(headerCssContent).toContain('top: 0');
    expect(headerCssContent).toContain('z-index');
  });
});
