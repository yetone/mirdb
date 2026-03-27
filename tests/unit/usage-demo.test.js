/**
 * Usage Demonstration Unit Tests
 * Owner: Scenario 4 - Usage Demonstration
 *
 * Tests:
 * - Usage GIF presence and src attribute
 * - Usage GIF alt text for accessibility
 * - Demo container structure
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { JSDOM } from 'jsdom';
import { resolve } from 'path';

describe('Usage Demonstration', () => {
  let document;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('TC1: Usage demonstration image', () => {
    it('should have an image element in the demo section', () => {
      const demoSection = document.querySelector('#demo');
      expect(demoSection).not.toBeNull();

      const img = demoSection.querySelector('img');
      expect(img).not.toBeNull();
    });

    it('should have src pointing to usage.gif', () => {
      const demoSection = document.querySelector('#demo');
      const img = demoSection.querySelector('img');
      expect(img.getAttribute('src')).toContain('usage.gif');
    });
  });

  describe('TC2: Usage GIF accessibility', () => {
    it('should have descriptive alt text for the usage GIF', () => {
      const demoSection = document.querySelector('#demo');
      const img = demoSection.querySelector('img');

      const altText = img.getAttribute('alt');
      expect(altText).not.toBeNull();
      expect(altText).not.toBe('');
      expect(altText.length).toBeGreaterThan(10);
    });

    it('should mention usage demonstration in alt text', () => {
      const demoSection = document.querySelector('#demo');
      const img = demoSection.querySelector('img');

      const altText = img.getAttribute('alt').toLowerCase();
      expect(altText).toContain('usage');
    });

    it('should mention get/set operations in alt text', () => {
      const demoSection = document.querySelector('#demo');
      const img = demoSection.querySelector('img');

      const altText = img.getAttribute('alt').toLowerCase();
      expect(altText).toMatch(/get|set/);
    });
  });

  describe('TC3: Demo container styling', () => {
    it('should have a demo section with id="demo"', () => {
      const demoSection = document.querySelector('#demo');
      expect(demoSection).not.toBeNull();
      expect(demoSection.tagName.toLowerCase()).toBe('section');
    });

    it('should have centered container', () => {
      const demoSection = document.querySelector('#demo');
      const container = demoSection.querySelector('.text-center');
      expect(container).not.toBeNull();
    });

    it('should have a styled image container with shadow and rounded corners', () => {
      const demoSection = document.querySelector('#demo');
      const imgContainer = demoSection.querySelector('.rounded-lg.shadow-xl');
      expect(imgContainer).not.toBeNull();
    });

    it('should have the image with responsive styling', () => {
      const demoSection = document.querySelector('#demo');
      const img = demoSection.querySelector('img');

      const classList = img.className;
      expect(classList).toContain('max-w-full');
      expect(classList).toContain('h-auto');
    });
  });

  describe('Demo section heading', () => {
    it('should have a heading for the demo section', () => {
      const demoSection = document.querySelector('#demo');
      const heading = demoSection.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent.toLowerCase()).toMatch(/action|demo|usage/);
    });
  });
});
