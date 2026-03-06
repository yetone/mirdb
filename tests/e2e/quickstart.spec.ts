/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 4 - Quick Start Code Example
 *
 * Test coverage:
 * - Quick-start section presence
 * - Code block element
 * - Memcached command examples
 * - Syntax highlighting/styling
 */

import { test, expect } from '@playwright/test';
import { waitForPageLoad, navigateToSection } from './test-utils';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: Quick-start section exists with correct id', async ({ page }) => {
    // Check that section with id='quick-start' exists in the DOM
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify it's a section element
    const tagName = await quickStartSection.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('section');

    // Navigate to the section and verify it's in view
    await navigateToSection(page, 'quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  test('TC2: Code block element exists within quick-start section', async ({ page }) => {
    // Navigate to quick-start section
    await navigateToSection(page, 'quick-start');

    // Check that a pre element exists within the quick-start section
    const codeBlockPre = page.locator('#quick-start pre.code-block');
    await expect(codeBlockPre).toBeVisible();

    // Check that a code element exists within the pre element
    const codeElement = page.locator('#quick-start pre.code-block code');
    await expect(codeElement).toBeVisible();

    // Verify the code block has the expected test ID
    const codeBlockWithTestId = page.locator('[data-testid="code-block"]');
    await expect(codeBlockWithTestId).toBeVisible();
  });

  test('TC3: Code block contains Memcached commands', async ({ page }) => {
    await navigateToSection(page, 'quick-start');

    const codeBlock = page.locator('#quick-start .code-block code');
    const codeText = await codeBlock.textContent();

    // Verify code contains SET command
    expect(codeText).toContain('set');

    // Verify code contains GET command
    expect(codeText).toContain('get');

    // Verify code contains connection example
    expect(codeText).toContain('telnet');
    expect(codeText).toContain('localhost');
    expect(codeText).toContain('11211');

    // Verify code shows typical Memcached responses
    expect(codeText).toContain('STORED');
    expect(codeText).toContain('VALUE');
    expect(codeText).toContain('END');
  });

  test('TC4: Code block has syntax highlighting or styling', async ({ page }) => {
    await navigateToSection(page, 'quick-start');

    // Check that code block has CSS classes for styling
    const codeBlock = page.locator('#quick-start pre.code-block');
    const hasCodeBlockClass = await codeBlock.evaluate(el => el.classList.contains('code-block'));
    expect(hasCodeBlockClass).toBe(true);

    // Check that the code element has highlighting class
    const codeElement = page.locator('#quick-start pre.code-block code');
    const hasHighlightClass = await codeElement.evaluate(el => el.classList.contains('code-highlight'));
    expect(hasHighlightClass).toBe(true);

    // Verify syntax highlighting spans exist for keywords
    const keywordSpans = page.locator('#quick-start .code-keyword');
    const keywordCount = await keywordSpans.count();
    expect(keywordCount).toBeGreaterThan(0);

    // Verify the code block has appropriate styling (background color, font-family)
    const computedStyle = await codeBlock.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        fontFamily: styles.fontFamily
      };
    });

    // Should have a dark background (not transparent or white)
    expect(computedStyle.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(computedStyle.backgroundColor).not.toBe('rgb(255, 255, 255)');

    // Should have monospace font
    expect(computedStyle.fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/);
  });

  test('Copy button exists and is functional', async ({ page }) => {
    await navigateToSection(page, 'quick-start');

    // Check that copy button exists
    const copyButton = page.locator('[data-testid="copy-code-btn"]');
    await expect(copyButton).toBeVisible();

    // Verify button has appropriate aria-label
    const ariaLabel = await copyButton.getAttribute('aria-label');
    expect(ariaLabel).toContain('Copy');

    // Verify button is clickable
    await expect(copyButton).toBeEnabled();
  });

  test('Code block wrapper has proper structure', async ({ page }) => {
    await navigateToSection(page, 'quick-start');

    // Check code block wrapper exists
    const wrapper = page.locator('#quick-start .code-block-wrapper');
    await expect(wrapper).toBeVisible();

    // Check header with language indicator exists
    const header = page.locator('#quick-start .code-block-header');
    await expect(header).toBeVisible();

    // Check language label exists
    const langLabel = page.locator('#quick-start .code-block-lang');
    await expect(langLabel).toBeVisible();
    const langText = await langLabel.textContent();
    expect(langText?.toLowerCase()).toContain('shell');
  });
});
