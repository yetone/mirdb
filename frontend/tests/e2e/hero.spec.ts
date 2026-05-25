/**
 * E2E tests for Hero Section.
 * Covers REQ-1 (logo/tagline), REQ-10 (GitHub/docs links).
 */

import { test, expect } from '@playwright/test';

test.describe('Homepage Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page title contains MirDB', async ({ page }) => {
    await expect(page).toHaveTitle(/MirDB/);
  });

  test('hero section is visible without scrolling', async ({ page }) => {
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    const isInViewport = await heroSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return (
        rect.top >= 0 &&
        rect.left >= 0 &&
        rect.bottom <= window.innerHeight &&
        rect.right <= window.innerWidth
      );
    });
    expect(isInViewport).toBe(true);
  });

  test('logo renders correctly', async ({ page }) => {
    const logoImg = page.getByTestId('hero-logo-img');
    await expect(logoImg).toBeVisible();
    await expect(logoImg).toHaveAttribute('src', '/mirdb-logo.svg');
    await expect(logoImg).toHaveAttribute('alt', 'MirDB Logo');
  });

  test('tagline text matches expected value', async ({ page }) => {
    const tagline = page.getByTestId('hero-tagline');
    await expect(tagline).toHaveText(
      'Fast, persistent key-value store with Memcached protocol'
    );
  });

  test('CTA button is present and visible', async ({ page }) => {
    const ctaButton = page.getByTestId('hero-cta-button');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toHaveText('Get Started Now');
  });

  test('external links have correct hrefs', async ({ page }) => {
    const githubLink = page.getByTestId('hero-link-github');
    const docsLink = page.getByTestId('hero-link-docs');
    const communityLink = page.getByTestId('hero-link-community');

    await expect(githubLink).toHaveAttribute(
      'href',
      'https://github.com/mirdb/mirdb'
    );
    await expect(docsLink).toHaveAttribute(
      'href',
      'https://mirdb.io/docs'
    );
    await expect(communityLink).toHaveAttribute(
      'href',
      'https://mirdb.io/community'
    );
  });

  test('all external links open in new tab and have rel attribute', async ({ page }) => {
    const links = [
      page.getByTestId('hero-link-github'),
      page.getByTestId('hero-link-docs'),
      page.getByTestId('hero-link-community'),
    ];

    for (const link of links) {
      await expect(link).toHaveAttribute('target', '_blank');
      await expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    }
  });

  test('hero section has proper ARIA attributes', async ({ page }) => {
    const heroSection = page.getByTestId('hero-section');
    const linksNav = page.getByTestId('hero-links');

    await expect(heroSection).toHaveAttribute('aria-label', 'Hero');
    await expect(linksNav).toHaveAttribute('aria-label', 'External resources');
  });
});
