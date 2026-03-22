/**
 * Shared Test Utilities
 * Owner: First test builder
 *
 * Exports:
 * - VIEWPORTS: { mobile, tablet, desktop } dimension objects
 * - waitForLoad(): Wait for page to fully load
 * - scrollToSection(selector): Scroll element into view
 * - getComputedStyle(element, property): Get CSS property value
 * - countWords(text): Count words in a string
 */

import { Page } from '@playwright/test';

export const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 720 },
};

export async function waitForLoad(page: Page): Promise<void> {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForLoadState('networkidle');
}

export async function scrollToSection(page: Page, selector: string): Promise<void> {
  const element = page.locator(selector);
  await element.scrollIntoViewIfNeeded();
}

export async function getComputedStyle(
  page: Page,
  selector: string,
  property: string
): Promise<string> {
  return page.evaluate(
    ([sel, prop]) => {
      const el = document.querySelector(sel);
      if (!el) return '';
      return window.getComputedStyle(el).getPropertyValue(prop);
    },
    [selector, property]
  );
}

export function countWords(text: string): number {
  return text.trim().split(/\s+/).filter(word => word.length > 0).length;
}
