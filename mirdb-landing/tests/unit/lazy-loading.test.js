/**
 * Lazy Loading Unit Tests
 * Owner: Scenario 18 - Asset Loading and Optimization
 *
 * Tests:
 * - Images have loading="lazy" attribute
 * - Below-fold images use lazy loading
 * - Lazy loading module exports correct functions
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('Lazy Loading Attributes', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  test('TC3: Below-fold images have loading="lazy" attribute', () => {
    // Architecture diagram should have lazy loading
    // Match img tag with architecture-diagram class that has loading="lazy" attribute (in any order)
    const archImgMatch = htmlContent.match(/<img[^>]*class="architecture-diagram"[^>]*/);
    expect(archImgMatch).not.toBeNull();
    expect(archImgMatch[0]).toMatch(/loading="lazy"/);
  });

  test('Architecture section image uses lazy loading', () => {
    // The architecture.svg should have loading="lazy"
    const architectureImgMatch = htmlContent.match(/<img[^>]*src="assets\/images\/architecture\.svg"[^>]*/);
    expect(architectureImgMatch).not.toBeNull();
    expect(architectureImgMatch[0]).toMatch(/loading="lazy"/);
  });

  test('Images below the fold have width and height attributes for layout stability', () => {
    // Check architecture diagram has dimensions
    const archImgMatch = htmlContent.match(/<img[^>]*class="architecture-diagram"[^>]*/);
    expect(archImgMatch).not.toBeNull();
    expect(archImgMatch[0]).toMatch(/width=/);
    expect(archImgMatch[0]).toMatch(/height=/);
  });

  test('All img elements have alt attributes for accessibility', () => {
    // Find all img tags
    const imgTags = htmlContent.match(/<img[^>]*>/g) || [];

    imgTags.forEach((imgTag) => {
      // Each img should have an alt attribute
      expect(imgTag).toMatch(/alt=/);
    });
  });

  test('External status badge image has appropriate attributes', () => {
    // CircleCI badge
    const badgeMatch = htmlContent.match(/<img[^>]*class="status-badge"[^>]*/);
    if (badgeMatch) {
      expect(badgeMatch[0]).toMatch(/alt=/);
      expect(badgeMatch[0]).toMatch(/width=/);
      expect(badgeMatch[0]).toMatch(/height=/);
    }
  });
});

describe('Lazy Load Module Structure', () => {
  let lazyLoadModule;

  beforeAll(() => {
    // Read the module file to check exports
    const modulePath = path.join(__dirname, '../../js/modules/lazy-load.js');
    lazyLoadModule = fs.readFileSync(modulePath, 'utf8');
  });

  test('Module exports init function', () => {
    expect(lazyLoadModule).toMatch(/export\s+(const|function)\s+init/);
  });

  test('Module exports observeElement function', () => {
    expect(lazyLoadModule).toMatch(/export\s+(const|function)\s+observeElement/);
  });

  test('Module uses IntersectionObserver', () => {
    expect(lazyLoadModule).toMatch(/IntersectionObserver/);
  });

  test('Module handles data-src attribute for lazy loading', () => {
    expect(lazyLoadModule).toMatch(/data-src|dataset\.src/);
  });

  test('Module provides fallback for browsers without IntersectionObserver', () => {
    expect(lazyLoadModule).toMatch(/('IntersectionObserver'\s+in\s+window|IntersectionObserver.*window)/);
  });
});
