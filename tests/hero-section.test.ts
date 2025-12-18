import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Hero Section Display', () => {
  let document: Document;
  let heroSection: Element | null;

  beforeEach(() => {
    // Load the actual HTML file
    const htmlPath = path.resolve(__dirname, '../website/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    heroSection = document.querySelector('[data-testid="hero-section"], .hero, #hero, section.hero');
  });

  describe('TC1: Product name "MirDB" is visible in hero section', () => {
    it('should have a hero section visible', () => {
      expect(heroSection).not.toBeNull();
    });

    it('should display "MirDB" as a heading in the hero section', () => {
      const headings = heroSection?.querySelectorAll('h1, h2, h3');
      const mirdbHeading = Array.from(headings || []).find(h =>
        h.textContent?.match(/MirDB/i)
      );
      expect(mirdbHeading).not.toBeUndefined();
      expect(mirdbHeading?.tagName).toBe('H1'); // Should be prominent heading
    });

    it('should have MirDB as the primary title', () => {
      const h1 = heroSection?.querySelector('h1');
      expect(h1?.textContent).toContain('MirDB');
    });
  });

  describe('TC2: Tagline mentions "persistent key-value store" and "memcached compatibility"', () => {
    it('should have a tagline element', () => {
      const tagline = heroSection?.querySelector('.tagline, p');
      expect(tagline).not.toBeNull();
    });

    it('should mention "persistent key-value store"', () => {
      const heroText = heroSection?.textContent?.toLowerCase() || '';
      expect(heroText).toMatch(/persistent.*key-value.*store/i);
    });

    it('should mention "memcached compatibility"', () => {
      const heroText = heroSection?.textContent?.toLowerCase() || '';
      expect(heroText).toMatch(/memcached.*compatib/i);
    });
  });

  describe('TC3: "Get Started" button is present and clickable', () => {
    it('should have a "Get Started" button/link', () => {
      const allLinks = heroSection?.querySelectorAll('a, button');
      const getStarted = Array.from(allLinks || []).find(el =>
        el.textContent?.match(/get started/i)
      );
      expect(getStarted).not.toBeUndefined();
    });

    it('should style "Get Started" as a primary CTA', () => {
      const allLinks = heroSection?.querySelectorAll('a, button');
      const getStarted = Array.from(allLinks || []).find(el =>
        el.textContent?.match(/get started/i)
      );
      const classList = getStarted?.className || '';
      // Should have primary styling class
      expect(classList).toMatch(/primary|cta|btn-primary/);
    });

    it('should have a valid href attribute', () => {
      const allLinks = heroSection?.querySelectorAll('a');
      const getStarted = Array.from(allLinks || []).find(el =>
        el.textContent?.match(/get started/i)
      ) as HTMLAnchorElement | undefined;
      expect(getStarted?.href).toBeTruthy();
    });
  });

  describe('TC4: "View on GitHub" button is present and links to external repository', () => {
    it('should have a "View on GitHub" button/link', () => {
      const allLinks = heroSection?.querySelectorAll('a');
      const githubLink = Array.from(allLinks || []).find(el =>
        el.textContent?.match(/github/i)
      );
      expect(githubLink).not.toBeUndefined();
    });

    it('should link to a GitHub repository', () => {
      const allLinks = heroSection?.querySelectorAll('a');
      const githubLink = Array.from(allLinks || []).find(el =>
        el.textContent?.match(/github/i)
      ) as HTMLAnchorElement | undefined;
      expect(githubLink?.href).toMatch(/github\.com/i);
    });

    it('should open in a new tab (external link)', () => {
      const allLinks = heroSection?.querySelectorAll('a');
      const githubLink = Array.from(allLinks || []).find(el =>
        el.textContent?.match(/github/i)
      ) as HTMLAnchorElement | undefined;
      expect(githubLink?.target).toBe('_blank');
    });

    it('should have rel="noopener noreferrer" for security', () => {
      const allLinks = heroSection?.querySelectorAll('a');
      const githubLink = Array.from(allLinks || []).find(el =>
        el.textContent?.match(/github/i)
      ) as HTMLAnchorElement | undefined;
      expect(githubLink?.rel).toContain('noopener');
    });
  });
});
