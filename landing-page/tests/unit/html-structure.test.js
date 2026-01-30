/**
 * HTML Structure Unit Tests
 * Tests for validating HTML structure of the landing page
 */
const fs = require('fs');
const path = require('path');

describe('HTML Structure Tests', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  describe('Hero Section HTML Structure', () => {
    test('TC2: Contains h1 with product name MirDB', () => {
      // Check for h1 element with MirDB
      expect(htmlContent).toMatch(/<h1[^>]*class="hero-title"[^>]*>MirDB<\/h1>/);
    });

    test('Contains hero section with correct id', () => {
      expect(htmlContent).toMatch(/<section[^>]*id="hero"[^>]*class="hero"/);
    });

    test('Contains subtitle with tagline about persistent key-value store', () => {
      expect(htmlContent).toMatch(/Persistent Key-Value Store/);
      expect(htmlContent).toMatch(/Memcached Protocol/);
    });

    test('Contains two CTA buttons', () => {
      // Check for Get Started button
      expect(htmlContent).toMatch(/<a[^>]*href="#getting-started"[^>]*class="btn btn-primary"[^>]*>Get Started<\/a>/);
      // Check for View on GitHub button
      expect(htmlContent).toMatch(/<a[^>]*href="https:\/\/github\.com\/yetone\/mirdb"[^>]*class="btn btn-secondary"[^>]*[^>]*>View on GitHub<\/a>/);
    });

    test('Contains hero logo image', () => {
      expect(htmlContent).toMatch(/<img[^>]*src="assets\/images\/logo\.gif"[^>]*alt="MirDB Logo"[^>]*class="hero-logo"/);
    });

    test('GitHub button has correct target and rel attributes', () => {
      expect(htmlContent).toMatch(/target="_blank"/);
      expect(htmlContent).toMatch(/rel="noopener noreferrer"/);
    });

    test('Hero section contains container div', () => {
      // Hero section should have a container for proper layout
      expect(htmlContent).toMatch(/<section[^>]*id="hero"[^>]*>[\s\S]*<div class="container">/);
    });

    test('Hero content wrapper exists', () => {
      expect(htmlContent).toMatch(/<div class="hero-content">/);
    });

    test('Hero CTA wrapper exists', () => {
      expect(htmlContent).toMatch(/<div class="hero-cta">/);
    });
  });

  describe('General HTML Structure', () => {
    test('Has proper DOCTYPE declaration', () => {
      expect(htmlContent).toMatch(/^<!DOCTYPE html>/i);
    });

    test('Has html lang attribute', () => {
      expect(htmlContent).toMatch(/<html[^>]*lang="en"[^>]*>/);
    });

    test('Has meta viewport for responsive design', () => {
      expect(htmlContent).toMatch(/<meta[^>]*name="viewport"[^>]*content="width=device-width, initial-scale=1\.0"[^>]*>/);
    });

    test('Has meta charset UTF-8', () => {
      expect(htmlContent).toMatch(/<meta[^>]*charset="UTF-8"[^>]*>/);
    });

    test('Has title with MirDB', () => {
      expect(htmlContent).toMatch(/<title>MirDB[^<]*<\/title>/);
    });

    test('Links to main stylesheet', () => {
      expect(htmlContent).toMatch(/<link[^>]*rel="stylesheet"[^>]*href="css\/styles\.css"[^>]*>/);
    });

    test('Links to main JavaScript file', () => {
      expect(htmlContent).toMatch(/<script[^>]*src="js\/main\.js"[^>]*><\/script>/);
    });
  });

  describe('SEO Meta Tags', () => {
    test('Has meta description', () => {
      expect(htmlContent).toMatch(/<meta[^>]*name="description"[^>]*content="[^"]*MirDB[^"]*"[^>]*>/);
    });

    test('Has Open Graph title', () => {
      expect(htmlContent).toMatch(/<meta[^>]*property="og:title"[^>]*content="[^"]*MirDB[^"]*"[^>]*>/);
    });

    test('Has Open Graph description', () => {
      expect(htmlContent).toMatch(/<meta[^>]*property="og:description"[^>]*>/);
    });

    test('Has Twitter card meta tag', () => {
      expect(htmlContent).toMatch(/<meta[^>]*name="twitter:card"[^>]*>/);
    });
  });

  describe('Semantic HTML', () => {
    test('Uses semantic section elements', () => {
      expect(htmlContent).toMatch(/<section[^>]*id="hero"/);
      expect(htmlContent).toMatch(/<section[^>]*id="features"/);
      expect(htmlContent).toMatch(/<section[^>]*id="getting-started"/);
    });

    test('Has nav element for navigation', () => {
      expect(htmlContent).toMatch(/<nav[^>]*class="nav"/);
    });

    test('Has footer element', () => {
      expect(htmlContent).toMatch(/<footer[^>]*class="footer"/);
    });
  });
});
