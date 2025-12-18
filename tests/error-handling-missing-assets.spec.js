// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

/**
 * Error Handling - Missing Assets Tests for MirDB Homepage
 * Tests graceful degradation when CSS fails, JavaScript is disabled,
 * and images fail to load.
 *
 * Scenario: Verify page handles missing or failed asset loads gracefully
 */

// Helper to get file URL
const getFileUrl = () => {
  return 'file://' + path.resolve(__dirname, '..', 'index.html');
};

test.describe('Error Handling - CSS Failure Graceful Degradation (E2E)', () => {
  /**
   * Test Case 1: Load page with CSS disabled/failed
   * Input: Load page with CSS disabled/failed
   * Expected: Content remains readable due to semantic HTML
   */
  test('TC1: content remains readable when CSS fails to load', async ({ page }) => {
    // Block CSS files from loading to simulate CSS failure
    await page.route('**/*.css', route => route.abort());
    await page.route('**/prism*.css', route => route.abort());

    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Verify main heading is visible and readable
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toHaveText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Verify description is visible
    const description = page.locator('.description');
    await expect(description).toBeVisible();

    // Verify navigation links are present and accessible
    const navLinks = page.locator('.nav-links a');
    const navCount = await navLinks.count();
    expect(navCount).toBeGreaterThanOrEqual(3);

    // Verify all nav links are visible
    for (let i = 0; i < navCount; i++) {
      await expect(navLinks.nth(i)).toBeVisible();
    }

    // Verify feature section headings are visible
    const featuresH2 = page.locator('#features h2');
    await expect(featuresH2).toBeVisible();
    await expect(featuresH2).toHaveText('Key Features');

    // Verify feature cards content is readable
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(3);

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const cardH3 = card.locator('h3');
      const cardP = card.locator('p');
      await expect(cardH3).toBeVisible();
      await expect(cardP).toBeVisible();
    }

    // Verify Getting Started section is visible
    const gettingStartedH2 = page.locator('#getting-started h2');
    await expect(gettingStartedH2).toBeVisible();
    await expect(gettingStartedH2).toHaveText('Getting Started');

    // Verify code sections are visible
    const codeSections = page.locator('.code-section');
    const codeCount = await codeSections.count();
    expect(codeCount).toBeGreaterThanOrEqual(4);

    // Verify footer content is visible
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify CTA buttons are visible and clickable
    const ctaButtons = page.locator('.cta-buttons a');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBe(2);

    for (let i = 0; i < buttonCount; i++) {
      await expect(ctaButtons.nth(i)).toBeVisible();
    }
  });

  test('TC1b: semantic HTML structure allows reading without CSS', async ({ page }) => {
    // Block all stylesheets
    await page.route('**/*.css', route => route.abort());

    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Verify semantic elements exist and have content
    const semanticStructure = await page.evaluate(() => {
      return {
        header: document.querySelector('header') !== null,
        nav: document.querySelector('nav') !== null,
        main: document.querySelector('main') !== null,
        footer: document.querySelector('footer') !== null,
        sections: document.querySelectorAll('section').length,
        h1: document.querySelector('h1')?.textContent?.trim(),
        h2Count: document.querySelectorAll('h2').length,
        h3Count: document.querySelectorAll('h3').length,
        links: document.querySelectorAll('a[href]').length,
        lists: document.querySelectorAll('ul, ol').length
      };
    });

    // Verify all semantic structural elements exist
    expect(semanticStructure.header).toBe(true);
    expect(semanticStructure.nav).toBe(true);
    expect(semanticStructure.main).toBe(true);
    expect(semanticStructure.footer).toBe(true);
    expect(semanticStructure.sections).toBeGreaterThanOrEqual(4);
    expect(semanticStructure.h1).toBe('MirDB');
    expect(semanticStructure.h2Count).toBeGreaterThanOrEqual(4);
    expect(semanticStructure.h3Count).toBeGreaterThanOrEqual(3);
    expect(semanticStructure.links).toBeGreaterThan(5);
    expect(semanticStructure.lists).toBeGreaterThan(0);
  });

  test('TC1c: text content is readable without CSS styling', async ({ page }) => {
    // Block CSS to test unstyled content
    await page.route('**/*.css', route => route.abort());

    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Check that text content has reasonable default rendering
    const textAnalysis = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = window.getComputedStyle(body);

      // Get all visible text elements
      const textElements = document.querySelectorAll('h1, h2, h3, p, li, a, code');
      const readableElements = [];

      for (const el of textElements) {
        const rect = el.getBoundingClientRect();
        const style = window.getComputedStyle(el);
        const text = el.textContent?.trim();

        if (text && text.length > 0) {
          readableElements.push({
            tag: el.tagName.toLowerCase(),
            hasText: text.length > 0,
            isInViewport: rect.width > 0 && rect.height > 0,
            fontSize: parseFloat(style.fontSize),
            display: style.display
          });
        }
      }

      return {
        totalTextElements: readableElements.length,
        visibleElements: readableElements.filter(e => e.isInViewport).length,
        hiddenElements: readableElements.filter(e => !e.isInViewport).length
      };
    });

    // Most text elements should be visible even without CSS
    expect(textAnalysis.totalTextElements).toBeGreaterThan(20);
    expect(textAnalysis.visibleElements).toBeGreaterThan(10);
  });
});

test.describe('Error Handling - JavaScript Disabled Fallback (E2E)', () => {
  /**
   * Test Case 2: Load page with JavaScript disabled
   * Input: Load page with JavaScript disabled
   * Expected: Core content and navigation works without JavaScript
   */
  test('TC2: core content and navigation works without JavaScript', async ({ browser }) => {
    // Create context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false
    });
    const page = await context.newPage();

    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Verify main heading is visible
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toHaveText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();

    // Verify description is visible
    const description = page.locator('.description');
    await expect(description).toBeVisible();

    // Verify hero section CTA buttons are visible and have hrefs
    const ctaButtons = page.locator('.cta-buttons a');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBe(2);

    // Check Get Started button
    const getStartedBtn = page.locator('.cta-buttons .btn-primary');
    await expect(getStartedBtn).toBeVisible();
    const getStartedHref = await getStartedBtn.getAttribute('href');
    expect(getStartedHref).toBe('#getting-started');

    // Check GitHub button
    const githubBtn = page.locator('.cta-buttons .btn-secondary');
    await expect(githubBtn).toBeVisible();
    const githubHref = await githubBtn.getAttribute('href');
    expect(githubHref).toContain('github.com');

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are visible
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(3);

    // Verify architecture section is visible
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify getting started section is visible
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify supported commands section is visible
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify footer is visible
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    await context.close();
  });

  test('TC2b: navigation anchor links work without JavaScript', async ({ browser }) => {
    // Create context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false
    });
    const page = await context.newPage();

    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Verify navigation links exist and have correct hrefs
    const navLinks = await page.evaluate(() => {
      const links = document.querySelectorAll('.nav-links a');
      return Array.from(links).map(link => ({
        text: link.textContent?.trim(),
        href: link.getAttribute('href'),
        isAnchor: link.getAttribute('href')?.startsWith('#') || false
      }));
    });

    // Check that we have navigation links
    expect(navLinks.length).toBeGreaterThanOrEqual(3);

    // Verify internal navigation links point to valid sections
    const internalLinks = navLinks.filter(l => l.isAnchor);
    expect(internalLinks.length).toBeGreaterThanOrEqual(2);

    // Verify each internal link targets an existing section
    for (const link of internalLinks) {
      const targetId = link.href?.substring(1); // Remove # prefix
      if (targetId) {
        const targetSection = page.locator(`#${targetId}`);
        await expect(targetSection).toBeAttached();
      }
    }

    await context.close();
  });

  test('TC2c: code examples are visible without JavaScript syntax highlighting', async ({ browser }) => {
    // Create context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false
    });
    const page = await context.newPage();

    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Verify code blocks exist and contain code
    const codeBlocks = page.locator('pre code');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThanOrEqual(4);

    // Verify each code block has readable content
    for (let i = 0; i < codeCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();
      const text = await codeBlock.textContent();
      expect(text?.length).toBeGreaterThan(5);
    }

    await context.close();
  });

  test('TC2d: all sections accessible without JavaScript', async ({ browser }) => {
    // Create context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false
    });
    const page = await context.newPage();

    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Check all main sections are present and visible
    const sections = ['#features', '#architecture', '#getting-started', '#commands'];

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      await expect(section).toBeAttached();

      // Check section has a heading
      const heading = section.locator('h2');
      await expect(heading).toBeVisible();
    }

    await context.close();
  });
});

test.describe('Error Handling - Image Alt Text Fallbacks (Unit)', () => {
  /**
   * Test Case 3: Check for broken image fallbacks
   * Input: Check for broken image fallbacks
   * Expected: Images have alt text that displays if images fail to load
   */
  test('TC3: all images have alt text for fallback', async ({ page }) => {
    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Get all img elements
    const images = await page.evaluate(() => {
      const imgElements = document.querySelectorAll('img');
      return Array.from(imgElements).map(img => ({
        src: img.getAttribute('src') || '',
        alt: img.getAttribute('alt'),
        hasAlt: img.hasAttribute('alt'),
        altLength: img.getAttribute('alt')?.length || 0
      }));
    });

    // If there are images, verify they all have alt attributes
    for (const img of images) {
      expect(img.hasAlt, `Image ${img.src} should have an alt attribute`).toBe(true);
    }
  });

  test('TC3b: SVG images have accessible names or are properly hidden', async ({ page }) => {
    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Check all SVG elements for accessibility
    const svgAnalysis = await page.evaluate(() => {
      const svgElements = document.querySelectorAll('svg');
      return Array.from(svgElements).map(svg => {
        const parent = svg.parentElement;
        const parentAriaHidden = parent?.getAttribute('aria-hidden') === 'true';
        const svgAriaHidden = svg.getAttribute('aria-hidden') === 'true';
        const hasTitle = svg.querySelector('title') !== null;
        const hasDesc = svg.querySelector('desc') !== null;
        const ariaLabel = svg.getAttribute('aria-label');
        const ariaLabelledBy = svg.getAttribute('aria-labelledby');
        const role = svg.getAttribute('role');

        return {
          isDecorative: parentAriaHidden || svgAriaHidden,
          hasAccessibleName: Boolean(hasTitle || hasDesc || ariaLabel || ariaLabelledBy),
          hasRole: Boolean(role),
          role: role,
          parentClass: parent?.className || ''
        };
      });
    });

    // Each SVG should either be marked as decorative or have an accessible name
    for (const svg of svgAnalysis) {
      const isAccessible = svg.isDecorative || svg.hasAccessibleName || svg.hasRole;
      expect(isAccessible,
        `SVG in ${svg.parentClass} should be marked decorative or have accessible name`
      ).toBe(true);
    }
  });

  test('TC3c: architecture diagram SVG has descriptive title and desc', async ({ page }) => {
    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Check the architecture diagram specifically
    const diagramAccessibility = await page.evaluate(() => {
      const diagram = document.querySelector('.architecture-diagram');
      if (!diagram) return null;

      return {
        hasTitle: diagram.querySelector('title') !== null,
        hasDesc: diagram.querySelector('desc') !== null,
        titleText: diagram.querySelector('title')?.textContent?.trim() || '',
        descText: diagram.querySelector('desc')?.textContent?.trim() || '',
        hasRole: diagram.getAttribute('role') === 'img',
        ariaLabelledBy: diagram.getAttribute('aria-labelledby')
      };
    });

    // Architecture diagram should exist and be accessible
    expect(diagramAccessibility).not.toBeNull();
    expect(diagramAccessibility.hasTitle).toBe(true);
    expect(diagramAccessibility.hasDesc).toBe(true);
    expect(diagramAccessibility.titleText.length).toBeGreaterThan(10);
    expect(diagramAccessibility.descText.length).toBeGreaterThan(50);
    expect(diagramAccessibility.hasRole).toBe(true);
  });

  test('TC3d: icon SVGs in feature cards are properly hidden from screen readers', async ({ page }) => {
    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Check feature card icons specifically
    const iconAccessibility = await page.evaluate(() => {
      const iconContainers = document.querySelectorAll('.feature-icon');
      return Array.from(iconContainers).map(container => ({
        hasAriaHidden: container.getAttribute('aria-hidden') === 'true',
        containsSvg: container.querySelector('svg') !== null,
        svgAriaHidden: container.querySelector('svg')?.getAttribute('aria-hidden')
      }));
    });

    // All icon containers should have aria-hidden="true"
    expect(iconAccessibility.length).toBe(3);
    for (const icon of iconAccessibility) {
      expect(icon.hasAriaHidden,
        'Feature icon container should have aria-hidden="true"'
      ).toBe(true);
      expect(icon.containsSvg).toBe(true);
    }
  });

  test('TC3e: page content remains usable if external images fail', async ({ page }) => {
    // Block all external image requests
    await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => route.abort());

    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Verify page structure is intact
    const pageStructure = await page.evaluate(() => {
      return {
        headerExists: document.querySelector('header') !== null,
        mainExists: document.querySelector('main') !== null,
        footerExists: document.querySelector('footer') !== null,
        h1Text: document.querySelector('h1')?.textContent?.trim(),
        featureCardsCount: document.querySelectorAll('.feature-card').length
      };
    });

    expect(pageStructure.headerExists).toBe(true);
    expect(pageStructure.mainExists).toBe(true);
    expect(pageStructure.footerExists).toBe(true);
    expect(pageStructure.h1Text).toBe('MirDB');
    expect(pageStructure.featureCardsCount).toBe(3);
  });
});

test.describe('Error Handling - Combined Asset Failures', () => {
  test('page remains functional with both CSS and JS disabled', async ({ browser }) => {
    // Create context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false
    });
    const page = await context.newPage();

    // Also block CSS
    await page.route('**/*.css', route => route.abort());

    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');

    // Verify core content is still accessible
    const coreContent = await page.evaluate(() => {
      return {
        h1: document.querySelector('h1')?.textContent?.trim(),
        tagline: document.querySelector('.tagline')?.textContent?.trim(),
        navLinksCount: document.querySelectorAll('.nav-links a').length,
        featureCardsCount: document.querySelectorAll('.feature-card').length,
        codeBlocksCount: document.querySelectorAll('pre code').length,
        footerExists: document.querySelector('footer') !== null
      };
    });

    expect(coreContent.h1).toBe('MirDB');
    expect(coreContent.tagline).toContain('Persistent Key-Value Store');
    expect(coreContent.navLinksCount).toBeGreaterThanOrEqual(3);
    expect(coreContent.featureCardsCount).toBe(3);
    expect(coreContent.codeBlocksCount).toBeGreaterThanOrEqual(4);
    expect(coreContent.footerExists).toBe(true);

    await context.close();
  });
});
