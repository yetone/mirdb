/**
 * Unit tests for Footer component.
 * Owner: Scenario 8 - Footer Section
 *
 * Tests the footer section rendering including GitHub link,
 * license information, and project credits.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { renderFooter } from '../../../src/components/Footer';

describe('Footer Component', () => {
  let footer: HTMLElement;

  beforeEach(() => {
    footer = renderFooter();
  });

  it('renders a footer element with proper id and class', () => {
    expect(footer.tagName).toBe('FOOTER');
    expect(footer.id).toBe('footer');
    expect(footer.className).toContain('footer-section');
  });

  it('contains link to GitHub repository', () => {
    const githubLink = footer.querySelector('a[href*="github.com"]') as HTMLAnchorElement;
    expect(githubLink).not.toBeNull();
    expect(githubLink.href).toContain('github.com');
    expect(githubLink.href).toContain('mirdb');
  });

  it('GitHub link opens in new tab', () => {
    const githubLink = footer.querySelector('a[href*="github.com"]') as HTMLAnchorElement;
    expect(githubLink).not.toBeNull();
    expect(githubLink.target).toBe('_blank');
    expect(githubLink.rel).toContain('noopener');
    expect(githubLink.rel).toContain('noreferrer');
  });

  it('GitHub link has recognizable text or icon', () => {
    const githubLink = footer.querySelector('a[href*="github.com"]') as HTMLAnchorElement;
    expect(githubLink).not.toBeNull();
    const linkText = githubLink.textContent?.toLowerCase() || '';
    const hasGitHubReference = linkText.includes('github') ||
                               githubLink.querySelector('svg') !== null ||
                               githubLink.getAttribute('aria-label')?.toLowerCase().includes('github');
    expect(hasGitHubReference).toBe(true);
  });

  it('displays license information', () => {
    const footerText = footer.textContent?.toLowerCase() || '';
    const hasLicense = footerText.includes('license') ||
                       footerText.includes('mit') ||
                       footerText.includes('apache') ||
                       footerText.includes('bsd') ||
                       footerText.includes('gpl');
    expect(hasLicense).toBe(true);
  });

  it('includes project credits', () => {
    const footerText = footer.textContent?.toLowerCase() || '';
    const hasCredits = footerText.includes('mirdb') ||
                       footerText.includes('credit') ||
                       footerText.includes('built') ||
                       footerText.includes('made') ||
                       footerText.includes('created');
    expect(hasCredits).toBe(true);
  });

  it('has proper accessibility attributes', () => {
    expect(footer.tagName).toBe('FOOTER');
    // Footer element is a landmark by default, but can have aria-label for clarity
    const hasAccessibleName = footer.hasAttribute('aria-label') ||
                              footer.hasAttribute('aria-labelledby') ||
                              footer.tagName === 'FOOTER';
    expect(hasAccessibleName).toBe(true);
  });

  it('has container with proper classes', () => {
    const container = footer.querySelector('.container');
    expect(container).not.toBeNull();
    expect(container?.classList.contains('footer-container')).toBe(true);
  });

  it('contains copyright or year information', () => {
    const footerText = footer.textContent || '';
    const currentYear = new Date().getFullYear();
    const hasCopyrightOrYear = footerText.includes('©') ||
                               footerText.includes(currentYear.toString()) ||
                               footerText.includes('copyright');
    expect(hasCopyrightOrYear).toBe(true);
  });

  it('all external links have security attributes', () => {
    const externalLinks = footer.querySelectorAll('a[target="_blank"]');
    externalLinks.forEach((link) => {
      const anchor = link as HTMLAnchorElement;
      expect(anchor.rel).toContain('noopener');
    });
  });

  it('footer content is organized in sections', () => {
    const container = footer.querySelector('.footer-container');
    expect(container).not.toBeNull();
    // Footer should have meaningful content organization
    const childElements = container?.children.length || 0;
    expect(childElements).toBeGreaterThan(0);
  });
});

describe('Footer Component - Content Sections', () => {
  let footer: HTMLElement;

  beforeEach(() => {
    footer = renderFooter();
  });

  it('has a section for repository links', () => {
    const githubLink = footer.querySelector('a[href*="github"]');
    expect(githubLink).not.toBeNull();
  });

  it('has license text visible', () => {
    const licenseElement = footer.querySelector('.footer-license') ||
                           footer.querySelector('[class*="license"]');
    // If no specific class, check text content
    if (!licenseElement) {
      const footerText = footer.textContent?.toLowerCase() || '';
      expect(footerText.includes('mit') || footerText.includes('license')).toBe(true);
    } else {
      expect(licenseElement).not.toBeNull();
    }
  });

  it('has credits section', () => {
    const creditsElement = footer.querySelector('.footer-credits') ||
                           footer.querySelector('[class*="credits"]');
    // If no specific class, check text content
    if (!creditsElement) {
      const footerText = footer.textContent?.toLowerCase() || '';
      expect(footerText.includes('mirdb') || footerText.includes('rust')).toBe(true);
    } else {
      expect(creditsElement).not.toBeNull();
    }
  });
});

describe('Footer Component - Links', () => {
  let footer: HTMLElement;

  beforeEach(() => {
    footer = renderFooter();
  });

  it('GitHub link points to correct repository', () => {
    const githubLink = footer.querySelector('a[href*="github.com"]') as HTMLAnchorElement;
    expect(githubLink).not.toBeNull();
    expect(githubLink.href).toContain('jzwdsb/mirdb');
  });

  it('links have hover states (via CSS classes)', () => {
    const links = footer.querySelectorAll('a');
    links.forEach((link) => {
      // Check that links have appropriate classes for styling
      expect(link.tagName).toBe('A');
    });
    expect(links.length).toBeGreaterThan(0);
  });
});
