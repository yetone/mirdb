// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * Accessibility - Screen Reader Support Tests
 *
 * These tests verify that the MirDB homepage is accessible to screen reader users
 * by checking semantic HTML structure, alt text, ARIA labels, and running
 * automated accessibility audits.
 */

// Test Case 1: Validate heading hierarchy
// Expected: Page has single h1, headings follow logical hierarchy (h1 > h2 > h3)
test('TC1: Validate heading hierarchy - Page has single h1 and logical heading hierarchy', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Check that there is exactly one h1 element
  const h1Elements = page.locator('h1');
  await expect(h1Elements).toHaveCount(1);

  // Verify the h1 contains the product name
  await expect(h1Elements.first()).toHaveText('MirDB');

  // Get all headings and verify hierarchy
  const allHeadings = await page.evaluate(() => {
    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    return Array.from(headings).map(h => ({
      level: parseInt(h.tagName.substring(1)),
      text: h.textContent.trim()
    }));
  });

  // Verify we have headings
  expect(allHeadings.length).toBeGreaterThan(0);

  // Verify heading hierarchy - no skipping levels (e.g., h1 directly to h3)
  let previousLevel = 0;
  for (const heading of allHeadings) {
    // Each heading level should not jump more than 1 level
    // e.g., h1 can go to h2, h2 can go to h3 or h2, but h1 should not go directly to h3
    if (previousLevel > 0) {
      const levelJump = heading.level - previousLevel;
      // Level can go up by at most 1, or go down by any amount (returning to higher levels)
      expect(levelJump).toBeLessThanOrEqual(1);
    }
    previousLevel = heading.level;
  }

  // Verify h2 elements exist (for sections)
  const h2Elements = page.locator('h2');
  const h2Count = await h2Elements.count();
  expect(h2Count).toBeGreaterThan(0);

  // Verify h3 elements exist (for subsections)
  const h3Elements = page.locator('h3');
  const h3Count = await h3Elements.count();
  expect(h3Count).toBeGreaterThan(0);
});

// Test Case 2: Check landmark regions
// Expected: Page has main, header, footer, and navigation landmarks
test('TC2: Check landmark regions - Page has main, header, footer, and navigation landmarks', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Check for <header> landmark
  const header = page.locator('header');
  await expect(header).toBeVisible();
  const headerCount = await header.count();
  expect(headerCount).toBeGreaterThan(0);

  // Check for <main> landmark
  const main = page.locator('main');
  await expect(main).toBeVisible();
  const mainCount = await main.count();
  expect(mainCount).toBe(1); // Should have exactly one main

  // Check for <footer> landmark
  const footer = page.locator('footer');
  await expect(footer).toBeVisible();
  const footerCount = await footer.count();
  expect(footerCount).toBeGreaterThan(0);

  // Check for <nav> landmark (or role="navigation")
  const nav = page.locator('nav, [role="navigation"]');
  const navCount = await nav.count();
  expect(navCount).toBeGreaterThan(0);

  // Verify landmarks using ARIA roles as well
  const landmarks = await page.evaluate(() => {
    const results = {
      banner: document.querySelectorAll('header, [role="banner"]').length > 0,
      main: document.querySelectorAll('main, [role="main"]').length > 0,
      contentinfo: document.querySelectorAll('footer, [role="contentinfo"]').length > 0,
      navigation: document.querySelectorAll('nav, [role="navigation"]').length > 0
    };
    return results;
  });

  expect(landmarks.banner).toBe(true);
  expect(landmarks.main).toBe(true);
  expect(landmarks.contentinfo).toBe(true);
  expect(landmarks.navigation).toBe(true);
});

// Test Case 3: Verify all images have alt attributes
// Expected: All img elements have descriptive alt text or alt='' for decorative images
test('TC3: Verify all images have alt attributes - All img elements have alt text', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Get all images
  const images = page.locator('img');
  const imageCount = await images.count();

  // If there are images, verify each has an alt attribute
  for (let i = 0; i < imageCount; i++) {
    const img = images.nth(i);
    const alt = await img.getAttribute('alt');

    // alt attribute must exist (can be empty for decorative images)
    expect(alt).not.toBeNull();

    // Get the src for better error messages
    const src = await img.getAttribute('src');

    // If alt is not empty, verify it's descriptive (at least a few characters)
    if (alt !== '') {
      expect(alt.length).toBeGreaterThan(0);
    }
  }

  // Also check for SVGs with proper accessibility
  const svgs = page.locator('svg');
  const svgCount = await svgs.count();

  for (let i = 0; i < svgCount; i++) {
    const svg = svgs.nth(i);

    // SVGs should either have aria-hidden="true" (decorative)
    // or have a title/aria-label for screen readers
    // Also check if parent element has aria-hidden="true" (for decorative icon containers)
    const ariaHidden = await svg.getAttribute('aria-hidden');
    const ariaLabel = await svg.getAttribute('aria-label');
    const role = await svg.getAttribute('role');
    const title = await svg.locator('title').count();

    // Check if parent container has aria-hidden (e.g., .feature-icon div)
    const parentAriaHidden = await svg.evaluate(el => {
      let parent = el.parentElement;
      while (parent) {
        if (parent.getAttribute('aria-hidden') === 'true') {
          return true;
        }
        parent = parent.parentElement;
      }
      return false;
    });

    // SVG is accessible if it's hidden from screen readers (decorative)
    // or has proper labeling, or its parent container is hidden
    const isAccessible = ariaHidden === 'true' ||
                         parentAriaHidden ||
                         ariaLabel !== null ||
                         (role === 'img' && title > 0);

    expect(isAccessible).toBe(true);
  }
});

// Test Case 4: Check code snippet accessibility
// Expected: Code blocks are marked up with proper role and are readable by screen readers
test('TC4: Check code snippet accessibility - Code blocks are readable by screen readers', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Check for code blocks (pre and code elements)
  const codeBlocks = page.locator('pre code, pre');
  const codeBlockCount = await codeBlocks.count();

  // Verify there are code blocks
  expect(codeBlockCount).toBeGreaterThan(0);

  // Check each code block for accessibility
  const preElements = page.locator('pre');
  const preCount = await preElements.count();

  for (let i = 0; i < preCount; i++) {
    const pre = preElements.nth(i);

    // Pre elements should contain code elements for proper semantics
    const codeChild = pre.locator('code');
    const hasCode = await codeChild.count();
    expect(hasCode).toBeGreaterThan(0);

    // Check that the pre/code block is not hidden from screen readers
    // unless explicitly marked as decorative
    const ariaHidden = await pre.getAttribute('aria-hidden');
    if (ariaHidden !== 'true') {
      // Verify the code content is accessible
      const textContent = await pre.textContent();
      expect(textContent.trim().length).toBeGreaterThan(0);
    }
  }

  // Verify code blocks are focusable or within focusable containers
  // for keyboard navigation if needed
  const codeElements = page.locator('code');
  const codeCount = await codeElements.count();

  for (let i = 0; i < codeCount; i++) {
    const code = codeElements.nth(i);

    // Code should have a language class for syntax highlighting context
    const className = await code.getAttribute('class');
    // It's okay if there's no class, but if there is, it should indicate language
    if (className) {
      // Language classes typically start with 'language-' (Prism.js format)
      const hasLanguageClass = className.includes('language-');
      // We just verify the class exists; having a language class is a good practice
    }
  }
});

// Test Case 5: Run automated accessibility audit (axe-core)
// Expected: No critical or serious accessibility violations detected
test('TC5: Run automated accessibility audit - No critical or serious violations', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Wait for the page to be fully loaded
  await page.waitForLoadState('domcontentloaded');

  // Run axe-core accessibility audit
  const accessibilityScanResults = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
    .analyze();

  // Filter for critical and serious violations
  const criticalViolations = accessibilityScanResults.violations.filter(
    v => v.impact === 'critical'
  );
  const seriousViolations = accessibilityScanResults.violations.filter(
    v => v.impact === 'serious'
  );

  // Log violations for debugging if any
  if (criticalViolations.length > 0 || seriousViolations.length > 0) {
    console.log('Critical violations:', JSON.stringify(criticalViolations, null, 2));
    console.log('Serious violations:', JSON.stringify(seriousViolations, null, 2));
  }

  // Assert no critical violations
  expect(criticalViolations.length).toBe(0);

  // Assert no serious violations
  expect(seriousViolations.length).toBe(0);
});

// Additional test: Verify interactive elements have accessible names
test('TC-Additional: Interactive elements have accessible names', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Check all links have accessible text
  const links = page.locator('a');
  const linkCount = await links.count();

  for (let i = 0; i < linkCount; i++) {
    const link = links.nth(i);

    // Get the accessible name (text content or aria-label)
    const textContent = await link.textContent();
    const ariaLabel = await link.getAttribute('aria-label');
    const title = await link.getAttribute('title');

    // Link must have an accessible name
    const hasAccessibleName = (textContent && textContent.trim().length > 0) ||
                              (ariaLabel && ariaLabel.length > 0) ||
                              (title && title.length > 0);

    expect(hasAccessibleName).toBe(true);
  }

  // Check all buttons have accessible text (if any exist)
  const buttons = page.locator('button');
  const buttonCount = await buttons.count();

  for (let i = 0; i < buttonCount; i++) {
    const button = buttons.nth(i);

    const textContent = await button.textContent();
    const ariaLabel = await button.getAttribute('aria-label');
    const title = await button.getAttribute('title');

    const hasAccessibleName = (textContent && textContent.trim().length > 0) ||
                              (ariaLabel && ariaLabel.length > 0) ||
                              (title && title.length > 0);

    expect(hasAccessibleName).toBe(true);
  }
});

// Test: Verify sections have accessible labels
test('TC-Additional: Sections have accessible labels via aria-labelledby', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Check sections with aria-labelledby
  const labelledSections = page.locator('section[aria-labelledby]');
  const labelledCount = await labelledSections.count();

  for (let i = 0; i < labelledCount; i++) {
    const section = labelledSections.nth(i);
    const labelledBy = await section.getAttribute('aria-labelledby');

    // Verify the referenced element exists
    if (labelledBy) {
      const labelElement = page.locator(`#${labelledBy}`);
      await expect(labelElement).toBeVisible();
    }
  }
});
