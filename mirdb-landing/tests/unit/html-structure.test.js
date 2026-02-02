/**
 * HTML Structure Unit Tests
 * Owner: First Builder (shared across scenarios)
 *
 * Tests:
 * - Valid HTML5 document structure
 * - Semantic element usage
 * - Heading hierarchy
 * - Required sections present
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('HTML Structure', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  test('Document has valid HTML5 doctype', () => {
    expect(htmlContent).toMatch(/^<!DOCTYPE html>/i);
  });

  test('HTML element has lang attribute', () => {
    expect(htmlContent).toMatch(/<html[^>]+lang="en"/);
  });

  test('Document has charset meta tag', () => {
    expect(htmlContent).toMatch(/<meta[^>]+charset="UTF-8"/i);
  });

  test('Document has viewport meta tag', () => {
    expect(htmlContent).toMatch(/<meta[^>]+name="viewport"/);
    expect(htmlContent).toMatch(/width=device-width/);
  });

  test('Document has title element', () => {
    // Title contains MirDB
    expect(htmlContent).toMatch(/<title>.*MirDB.*<\/title>/);
  });

  test('Document has main element with id', () => {
    expect(htmlContent).toMatch(/<main[^>]+id="main-content"/);
  });

  test('Hero section has proper semantic structure', () => {
    // Hero section exists with id
    expect(htmlContent).toMatch(/<section[^>]+id="hero"/);

    // Hero section has aria-labelledby
    expect(htmlContent).toMatch(/<section[^>]+id="hero"[^>]+aria-labelledby="hero-heading"/);
  });

  test('TC4: Hero section contains proper semantic HTML with h1 heading', () => {
    // Check for h1 element in hero section
    expect(htmlContent).toMatch(/<h1[^>]+id="hero-heading"/);
    expect(htmlContent).toMatch(/<h1[^>]+class="hero__title"/);

    // Verify h1 contains MirDB
    expect(htmlContent).toMatch(/<h1[^>]*>MirDB<\/h1>/);
  });

  test('Document includes required stylesheet', () => {
    expect(htmlContent).toMatch(/<link[^>]+rel="stylesheet"[^>]+href="css\/styles\.css"/);
  });

  test('Document includes main JavaScript file', () => {
    expect(htmlContent).toMatch(/<script[^>]+type="module"[^>]+src="js\/main\.js"/);
  });

  test('Skip navigation link is present for accessibility', () => {
    expect(htmlContent).toMatch(/<a[^>]+href="#main-content"[^>]+class="visually-hidden"[^>]*>Skip to main content<\/a>/);
  });

  test('All required sections exist', () => {
    const requiredSections = ['hero', 'features', 'code-examples', 'architecture', 'status', 'specifications'];

    requiredSections.forEach(sectionId => {
      expect(htmlContent).toMatch(new RegExp(`<section[^>]+id="${sectionId}"`));
    });
  });

  test('Hero section contains logo image with alt text', () => {
    // Check for img with hero__logo class and alt text (attributes can be in any order)
    expect(htmlContent).toMatch(/class="hero__logo"/);
    expect(htmlContent).toMatch(/alt="MirDB Logo"/);
  });

  test('Hero section contains tagline with key phrases', () => {
    expect(htmlContent).toMatch(/Persistent Key-Value Store/);
    expect(htmlContent).toMatch(/Memcached Protocol/);
  });

  // Footer tests (Scenario 17)
  test('Footer element exists with semantic footer tag', () => {
    expect(htmlContent).toMatch(/<footer[^>]*>/);
  });

  test('Footer has proper class structure', () => {
    expect(htmlContent).toMatch(/<footer[^>]+class="[^"]*site-footer[^"]*"/);
  });
});
