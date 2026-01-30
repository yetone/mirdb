/**
 * Getting Started Section Integration Tests.
 * Owner: Scenario 5 - Getting Started Section
 *
 * Tests:
 * - Documentation link is present and functional
 * - Link has correct attributes for external navigation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { renderGettingStarted } from '../../src/components/GettingStarted';

describe('Getting Started Integration - Documentation Link', () => {
  let section: HTMLElement;

  beforeEach(() => {
    section = renderGettingStarted();
    document.body.appendChild(section);
  });

  afterEach(() => {
    document.body.innerHTML = '';
  });

  describe('Documentation Link Presence', () => {
    it('should have a documentation link', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]') as HTMLAnchorElement;
      expect(docLink).toBeTruthy();
    });

    it('should have correct href pointing to GitHub documentation', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]') as HTMLAnchorElement;
      expect(docLink.href).toBe('https://github.com/pjzhong/mirdb#readme');
    });

    it('should open in a new tab', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]') as HTMLAnchorElement;
      expect(docLink.target).toBe('_blank');
    });

    it('should have security attributes for external link', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]') as HTMLAnchorElement;
      expect(docLink.rel).toContain('noopener');
      expect(docLink.rel).toContain('noreferrer');
    });
  });

  describe('Link Functionality', () => {
    it('should be clickable', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]') as HTMLAnchorElement;
      expect(docLink.tagName).toBe('A');
    });

    it('should have meaningful link text', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]');
      const linkText = docLink?.textContent?.trim();
      expect(linkText).toContain('Documentation');
    });

    it('should have visible icon', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]');
      const icon = docLink?.querySelector('svg');
      expect(icon).toBeTruthy();
    });
  });

  describe('Link Accessibility', () => {
    it('should be focusable', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]') as HTMLAnchorElement;
      docLink.focus();
      expect(document.activeElement).toBe(docLink);
    });

    it('should have focus styles via CSS class', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]') as HTMLAnchorElement;
      expect(docLink.classList.contains('doc-link')).toBe(true);
    });
  });

  describe('Link URL Validation', () => {
    it('should have valid URL format', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]') as HTMLAnchorElement;
      const url = new URL(docLink.href);
      expect(url.protocol).toBe('https:');
      expect(url.hostname).toBe('github.com');
    });

    it('should point to mirdb repository', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]') as HTMLAnchorElement;
      expect(docLink.href).toContain('mirdb');
    });

    it('should point to readme section', () => {
      const docLink = section.querySelector('[data-testid="doc-link"]') as HTMLAnchorElement;
      expect(docLink.href).toContain('#readme');
    });
  });
});
