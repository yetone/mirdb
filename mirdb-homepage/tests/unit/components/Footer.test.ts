/**
 * Footer Component Unit Tests.
 * Owner: Scenario 6 - Footer and Project Badges
 *
 * Tests:
 * - Footer renders with badges
 * - CI, version, license badges present
 * - GitHub link has correct security attributes
 * - Author attribution is present
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { renderFooter, BADGES } from '../../../src/components/Footer';

describe('Footer Component', () => {
  let footer: HTMLElement;

  beforeEach(() => {
    footer = renderFooter();
  });

  // Test Case 1: Footer renders with badges, links, and attribution
  describe('Footer Rendering', () => {
    it('should render footer element with semantic footer tag', () => {
      expect(footer).not.toBeNull();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    it('should have proper id attribute', () => {
      expect(footer.getAttribute('id')).toBe('footer');
    });

    it('should render all badges', () => {
      const badges = footer.querySelectorAll('[data-testid="badge"]');
      expect(badges.length).toBe(3); // CI, version, license
    });

    it('should render GitHub repository link', () => {
      const githubLink = footer.querySelector('[data-testid="github-link"]');
      expect(githubLink).not.toBeNull();
    });

    it('should render author attribution', () => {
      const attribution = footer.querySelector('[data-testid="author-attribution"]');
      expect(attribution).not.toBeNull();
    });
  });

  // Test Case 2: CircleCI badge is present and links to CI dashboard
  describe('CI Status Badge', () => {
    it('should render CircleCI badge', () => {
      const ciBadge = footer.querySelector('[data-testid="badge-ci"]');
      expect(ciBadge).not.toBeNull();
    });

    it('should have CircleCI badge image with proper alt text', () => {
      const ciBadge = footer.querySelector('[data-testid="badge-ci"]');
      const img = ciBadge?.querySelector('img');
      expect(img).not.toBeNull();
      expect(img?.getAttribute('alt')).toContain('CI');
    });

    it('should link to CircleCI dashboard', () => {
      const ciBadge = footer.querySelector('[data-testid="badge-ci"]');
      const link = ciBadge?.closest('a') || ciBadge?.querySelector('a');
      expect(link).not.toBeNull();
      expect(link?.getAttribute('href')).toContain('circleci');
    });
  });

  // Test Case 3: Crates.io version badge is present and links to crate page
  describe('Version Badge', () => {
    it('should render Crates.io version badge', () => {
      const versionBadge = footer.querySelector('[data-testid="badge-version"]');
      expect(versionBadge).not.toBeNull();
    });

    it('should have version badge image with proper alt text', () => {
      const versionBadge = footer.querySelector('[data-testid="badge-version"]');
      const img = versionBadge?.querySelector('img');
      expect(img).not.toBeNull();
      expect(img?.getAttribute('alt')?.toLowerCase()).toContain('version');
    });

    it('should link to Crates.io crate page', () => {
      const versionBadge = footer.querySelector('[data-testid="badge-version"]');
      const link = versionBadge?.closest('a') || versionBadge?.querySelector('a');
      expect(link).not.toBeNull();
      expect(link?.getAttribute('href')).toContain('crates.io');
    });
  });

  // Test Case 4: License badge displays current license and links to license file
  describe('License Badge', () => {
    it('should render license badge', () => {
      const licenseBadge = footer.querySelector('[data-testid="badge-license"]');
      expect(licenseBadge).not.toBeNull();
    });

    it('should have license badge image with proper alt text', () => {
      const licenseBadge = footer.querySelector('[data-testid="badge-license"]');
      const img = licenseBadge?.querySelector('img');
      expect(img).not.toBeNull();
      expect(img?.getAttribute('alt')?.toLowerCase()).toContain('license');
    });

    it('should link to license file', () => {
      const licenseBadge = footer.querySelector('[data-testid="badge-license"]');
      const link = licenseBadge?.closest('a') || licenseBadge?.querySelector('a');
      expect(link).not.toBeNull();
      const href = link?.getAttribute('href');
      expect(href?.toLowerCase()).toMatch(/license/i);
    });
  });

  // Test Case 5: GitHub repository link opens with rel='noopener noreferrer'
  describe('GitHub Repository Link Security', () => {
    it('should have GitHub repository link with rel noopener noreferrer', () => {
      const githubLink = footer.querySelector('[data-testid="github-link"]') as HTMLAnchorElement;
      expect(githubLink).not.toBeNull();
      expect(githubLink?.getAttribute('rel')).toContain('noopener');
      expect(githubLink?.getAttribute('rel')).toContain('noreferrer');
    });

    it('should have GitHub repository link with target _blank', () => {
      const githubLink = footer.querySelector('[data-testid="github-link"]') as HTMLAnchorElement;
      expect(githubLink?.getAttribute('target')).toBe('_blank');
    });

    it('should link to GitHub repository', () => {
      const githubLink = footer.querySelector('[data-testid="github-link"]') as HTMLAnchorElement;
      expect(githubLink?.getAttribute('href')).toContain('github.com');
    });
  });

  // Test Case 6: Footer includes author/maintainer attribution
  describe('Author Attribution', () => {
    it('should include author attribution text', () => {
      const attribution = footer.querySelector('[data-testid="author-attribution"]');
      expect(attribution).not.toBeNull();
      expect(attribution?.textContent?.trim().length).toBeGreaterThan(0);
    });

    it('should mention maintainer or author', () => {
      const attribution = footer.querySelector('[data-testid="author-attribution"]');
      expect(attribution?.textContent?.toLowerCase()).toMatch(/maintained|created|built|author|by/i);
    });
  });
});

// Test BADGES export
describe('BADGES Export', () => {
  it('should export BADGES array with 3 badges', () => {
    expect(BADGES).toBeDefined();
    expect(BADGES.length).toBe(3);
  });

  it('should have CI badge in BADGES array', () => {
    const ciBadge = BADGES.find((b) => b.type === 'ci');
    expect(ciBadge).toBeDefined();
    expect(ciBadge?.url).toBeTruthy();
    expect(ciBadge?.link).toBeTruthy();
    expect(ciBadge?.alt).toBeTruthy();
  });

  it('should have version badge in BADGES array', () => {
    const versionBadge = BADGES.find((b) => b.type === 'version');
    expect(versionBadge).toBeDefined();
    expect(versionBadge?.url).toBeTruthy();
    expect(versionBadge?.link).toBeTruthy();
    expect(versionBadge?.alt).toBeTruthy();
  });

  it('should have license badge in BADGES array', () => {
    const licenseBadge = BADGES.find((b) => b.type === 'license');
    expect(licenseBadge).toBeDefined();
    expect(licenseBadge?.url).toBeTruthy();
    expect(licenseBadge?.link).toBeTruthy();
    expect(licenseBadge?.alt).toBeTruthy();
  });
});

// Test external link security attributes
describe('External Link Security', () => {
  let footer: HTMLElement;

  beforeEach(() => {
    footer = renderFooter();
  });

  it('should have all external links with proper security attributes', () => {
    const externalLinks = footer.querySelectorAll('a[target="_blank"]');
    externalLinks.forEach((link) => {
      const rel = link.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });

  it('should have badge links open in new tab with security attributes', () => {
    const badgeLinks = footer.querySelectorAll('[data-testid^="badge-"] a, a:has([data-testid^="badge-"])');
    // All badge parent links should have security attributes
    badgeLinks.forEach((link) => {
      if (link.getAttribute('target') === '_blank') {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      }
    });
  });
});
