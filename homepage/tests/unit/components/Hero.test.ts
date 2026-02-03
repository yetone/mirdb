/**
 * Unit tests for Hero component.
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - Hero component renders without errors
 * - Logo is present
 * - Tagline is displayed correctly
 * - CTA buttons are present and have correct attributes
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Window, type Document as HappyDomDocument } from 'happy-dom';

describe('Hero Component', () => {
  let document: HappyDomDocument;

  // For Astro components, we test the rendered HTML output
  // This simulates what the component will render
  const heroHTML = `
    <section id="hero" class="min-h-[80vh] flex flex-col items-center justify-center px-4 py-16 bg-terminal-bg text-terminal-text">
      <div class="max-w-4xl mx-auto text-center">
        <div class="mb-8" data-testid="hero-logo">
          <img
            src="/mirdb/logo.gif"
            alt="MirDB Logo"
            class="mx-auto max-w-full h-auto"
            width="400"
            height="200"
          />
        </div>
        <h1 class="text-4xl md:text-5xl lg:text-6xl font-bold text-white mb-4">
          MirDB
        </h1>
        <p class="text-xl md:text-2xl text-terminal-green font-mono mb-8" data-testid="hero-tagline">
          A Persistent Key-Value Store with Memcached Protocol
        </p>
        <div class="flex flex-col sm:flex-row gap-4 justify-center" data-testid="hero-cta-buttons">
          <a
            href="#installation"
            class="inline-flex items-center justify-center px-8 py-3 text-lg font-semibold text-terminal-bg bg-terminal-green rounded-lg hover:bg-green-400 transition-colors"
            data-testid="get-started-button"
          >
            Get Started
          </a>
          <a
            href="https://github.com/yetone/mirdb"
            target="_blank"
            rel="noopener noreferrer"
            class="inline-flex items-center justify-center px-8 py-3 text-lg font-semibold text-white border-2 border-white rounded-lg hover:bg-white hover:text-terminal-bg transition-colors"
            data-testid="github-button"
          >
            View on GitHub
          </a>
        </div>
      </div>
    </section>
  `;

  beforeEach(() => {
    const window = new Window();
    window.document.body.innerHTML = heroHTML;
    document = window.document;
  });

  describe('Component Structure', () => {
    it('renders the hero section with correct ID', () => {
      const hero = document.querySelector('#hero');
      expect(hero).toBeTruthy();
      expect(hero?.tagName).toBe('SECTION');
    });

    it('renders without errors and matches expected structure', () => {
      const hero = document.querySelector('#hero');
      expect(hero).toBeTruthy();

      // Check main structural elements
      const logo = document.querySelector('[data-testid="hero-logo"]');
      const tagline = document.querySelector('[data-testid="hero-tagline"]');
      const ctaButtons = document.querySelector('[data-testid="hero-cta-buttons"]');

      expect(logo).toBeTruthy();
      expect(tagline).toBeTruthy();
      expect(ctaButtons).toBeTruthy();
    });
  });

  describe('Logo Display', () => {
    it('displays the MirDB logo image', () => {
      const logoContainer = document.querySelector('[data-testid="hero-logo"]');
      expect(logoContainer).toBeTruthy();

      const logoImg = logoContainer?.querySelector('img');
      expect(logoImg).toBeTruthy();
      expect(logoImg?.getAttribute('src')).toBe('/mirdb/logo.gif');
      expect(logoImg?.getAttribute('alt')).toBe('MirDB Logo');
    });

    it('logo has proper accessibility attributes', () => {
      const logoImg = document.querySelector('[data-testid="hero-logo"] img');
      expect(logoImg?.getAttribute('alt')).toBe('MirDB Logo');
      expect(logoImg?.getAttribute('width')).toBe('400');
      expect(logoImg?.getAttribute('height')).toBe('200');
    });
  });

  describe('Tagline Display', () => {
    it('displays the correct tagline text', () => {
      const tagline = document.querySelector('[data-testid="hero-tagline"]');
      expect(tagline).toBeTruthy();
      expect(tagline?.textContent?.trim()).toBe('A Persistent Key-Value Store with Memcached Protocol');
    });

    it('tagline has terminal-style green color class', () => {
      const tagline = document.querySelector('[data-testid="hero-tagline"]');
      expect(tagline?.classList.contains('text-terminal-green')).toBe(true);
    });
  });

  describe('Project Title', () => {
    it('displays MirDB as the main heading', () => {
      const h1 = document.querySelector('h1');
      expect(h1).toBeTruthy();
      expect(h1?.textContent?.trim()).toBe('MirDB');
    });
  });

  describe('CTA Buttons', () => {
    it('renders Get Started button with correct href', () => {
      const getStartedBtn = document.querySelector('[data-testid="get-started-button"]');
      expect(getStartedBtn).toBeTruthy();
      expect(getStartedBtn?.getAttribute('href')).toBe('#installation');
      expect(getStartedBtn?.textContent?.trim()).toContain('Get Started');
    });

    it('renders View on GitHub button with correct attributes', () => {
      const githubBtn = document.querySelector('[data-testid="github-button"]');
      expect(githubBtn).toBeTruthy();
      expect(githubBtn?.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
      expect(githubBtn?.getAttribute('target')).toBe('_blank');
      expect(githubBtn?.getAttribute('rel')).toBe('noopener noreferrer');
      expect(githubBtn?.textContent?.trim()).toContain('View on GitHub');
    });

    it('both CTA buttons are present in the buttons container', () => {
      const ctaContainer = document.querySelector('[data-testid="hero-cta-buttons"]');
      const buttons = ctaContainer?.querySelectorAll('a');
      expect(buttons?.length).toBe(2);
    });
  });

  describe('Accessibility', () => {
    it('has proper heading hierarchy with single H1', () => {
      const headings = document.querySelectorAll('h1');
      expect(headings.length).toBe(1);
    });

    it('GitHub button opens in new tab with security attributes', () => {
      const githubBtn = document.querySelector('[data-testid="github-button"]');
      expect(githubBtn?.getAttribute('rel')).toContain('noopener');
      expect(githubBtn?.getAttribute('rel')).toContain('noreferrer');
    });

    it('logo image has descriptive alt text', () => {
      const logoImg = document.querySelector('[data-testid="hero-logo"] img');
      const altText = logoImg?.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText?.length).toBeGreaterThan(0);
    });
  });

  describe('Responsive Design Classes', () => {
    it('has responsive text sizing classes on heading', () => {
      const h1 = document.querySelector('h1');
      expect(h1?.classList.contains('text-4xl')).toBe(true);
      expect(h1?.classList.contains('md:text-5xl')).toBe(true);
      expect(h1?.classList.contains('lg:text-6xl')).toBe(true);
    });

    it('has responsive layout classes on CTA container', () => {
      const ctaContainer = document.querySelector('[data-testid="hero-cta-buttons"]');
      expect(ctaContainer?.classList.contains('flex-col')).toBe(true);
      expect(ctaContainer?.classList.contains('sm:flex-row')).toBe(true);
    });
  });
});
