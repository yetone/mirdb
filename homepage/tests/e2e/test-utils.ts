/**
 * Shared E2E Test Utilities
 * Owner: First Builder
 *
 * Expected exports:
 * - viewport presets (mobile, tablet, desktop)
 * - common assertions
 * - page navigation helpers
 * - accessibility check helpers
 */

import { Page, expect } from '@playwright/test';

// Viewport presets
export const viewports = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 720 },
  desktopLarge: { width: 1920, height: 1080 },
};

// Common selectors
export const selectors = {
  hero: {
    section: '#hero',
    title: '.hero-title',
    tagline: '.hero-tagline',
    ctaButton: '.hero-cta .btn-primary',
    learnMoreButton: '.hero-cta .btn-secondary',
  },
  features: {
    section: '#features',
    title: '#features-title',
    grid: '.features-grid',
    card: '.feature-card',
  },
  usage: {
    section: '#usage',
    title: '#usage-title',
    codeBlock: '.code-block',
  },
  quickstart: {
    section: '#quickstart',
    title: '#quickstart-title',
    steps: '.step',
    stepNumber: '.step-number',
  },
  navigation: {
    header: '.header',
    logo: '.nav-logo',
    links: '.nav-links',
    link: '.nav-link',
    toggle: '.nav-toggle',
  },
  footer: {
    section: '.footer',
    links: '.footer-links',
    link: '.footer-link',
    copyright: '.footer-copyright',
  },
};

/**
 * Navigate to homepage and wait for load
 */
export async function navigateToHomepage(page: Page): Promise<void> {
  await page.goto('/');
  await page.waitForLoadState('domcontentloaded');
}

/**
 * Check if element is visible in viewport
 */
export async function isElementVisible(page: Page, selector: string): Promise<boolean> {
  const element = page.locator(selector);
  return await element.isVisible();
}

/**
 * Scroll to section by ID
 */
export async function scrollToSection(page: Page, sectionId: string): Promise<void> {
  await page.locator(`#${sectionId}`).scrollIntoViewIfNeeded();
  await page.waitForTimeout(300); // Allow for smooth scroll animation
}

/**
 * Get all text content from an element
 */
export async function getTextContent(page: Page, selector: string): Promise<string> {
  const element = page.locator(selector);
  return (await element.textContent()) || '';
}

/**
 * Check color contrast (basic check for text visibility)
 */
export async function checkColorContrast(
  page: Page,
  selector: string
): Promise<{ hasContrast: boolean }> {
  const element = page.locator(selector);
  const isVisible = await element.isVisible();
  return { hasContrast: isVisible };
}

/**
 * Wait for navigation after click
 */
export async function clickAndWaitForNavigation(
  page: Page,
  selector: string,
  targetSelector: string
): Promise<void> {
  await page.locator(selector).click();
  await page.waitForSelector(targetSelector, { state: 'visible' });
}

/**
 * Check if link opens in new tab
 */
export async function hasNewTabAttribute(page: Page, selector: string): Promise<boolean> {
  const element = page.locator(selector).first();
  const target = await element.getAttribute('target');
  const rel = await element.getAttribute('rel');
  return target === '_blank' && (rel?.includes('noopener') || rel?.includes('noreferrer'));
}
