/**
 * E2E Tests for Developer Evaluation User Journey
 * Scenario: User Journey - Developer Evaluation Flow
 * Validates the complete user journey from landing to accessing documentation,
 * simulating Developer Dan's evaluation workflow.
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('User Journey - Developer Evaluation Flow', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Complete Developer Dan user journey from landing to docs click
   * Expected: User can complete entire journey without confusion or dead ends
   */
  test.describe('Test Case 1: Complete User Journey', () => {
    test('should allow complete journey from landing to documentation access', async ({ page }) => {
      // Step 1: Land on homepage - verify page loads correctly
      await expect(page).toHaveTitle(/MirDB/);

      // Step 2: Understand value proposition - verify hero section is visible
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toHaveText('MirDB');

      const tagline = page.locator('.hero-tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent');

      // Step 3: Evaluate features - scroll to and verify features section exists
      const featuresSection = page.locator('#features, .features, [data-section="features"]');
      if (await featuresSection.count() > 0) {
        await featuresSection.scrollIntoViewIfNeeded();
        await expect(featuresSection).toBeVisible();
      }

      // Step 4: Review quick-start - verify quick start section
      const quickStartSection = page.locator('#quick-start, .quick-start, [data-section="quick-start"]');
      await quickStartSection.scrollIntoViewIfNeeded();
      await expect(quickStartSection).toBeVisible();

      // Verify code examples exist
      const codeBlocks = page.locator('.code-block, pre code');
      await expect(codeBlocks.first()).toBeVisible();

      // Step 5: Access documentation - verify CTA buttons are clickable
      const getStartedBtn = page.locator('.btn-primary, a[href="#quick-start"]').first();
      await expect(getStartedBtn).toBeVisible();

      const githubBtn = page.locator('a[href*="github.com"]').first();
      await expect(githubBtn).toBeVisible();
      await expect(githubBtn).toHaveAttribute('href', /github\.com/);
    });

    test('should have no broken internal navigation links', async ({ page }) => {
      // Get all internal anchor links
      const internalLinks = page.locator('a[href^="#"]');
      const count = await internalLinks.count();

      for (let i = 0; i < count; i++) {
        const link = internalLinks.nth(i);
        const href = await link.getAttribute('href');

        // Skip empty hash links
        if (href === '#') continue;

        const targetId = href.replace('#', '');
        const target = page.locator(`#${targetId}`);

        // Verify target exists
        const targetExists = await target.count() > 0;
        expect(targetExists, `Target section ${href} should exist`).toBe(true);
      }
    });

    test('should allow smooth scrolling through all sections', async ({ page }) => {
      // Scroll through each major section
      const sections = [
        '.hero',
        '#quick-start, .quick-start',
        '#configuration, .configuration'
      ];

      for (const selector of sections) {
        const section = page.locator(selector).first();
        if (await section.count() > 0) {
          await section.scrollIntoViewIfNeeded();
          await expect(section).toBeInViewport({ ratio: 0.1 });
        }
      }
    });
  });

  /**
   * Test Case 2: Measure time to understand value proposition
   * Expected: Value proposition is clear within 3 seconds of page load
   */
  test.describe('Test Case 2: Value Proposition Clarity', () => {
    test('should display value proposition within 3 seconds', async ({ page }) => {
      const startTime = Date.now();

      // Wait for critical content to be visible
      await page.locator('h1').waitFor({ state: 'visible' });
      await page.locator('.hero-tagline').waitFor({ state: 'visible' });
      await page.locator('.hero-description').waitFor({ state: 'visible' });

      const loadTime = Date.now() - startTime;
      expect(loadTime).toBeLessThan(3000);
    });

    test('should have hero content visible above fold without scrolling', async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });

      const h1 = page.locator('h1');
      const tagline = page.locator('.hero-tagline');
      const description = page.locator('.hero-description');
      const ctaButtons = page.locator('.hero-cta');

      // All critical elements should be visible in viewport
      await expect(h1).toBeInViewport();
      await expect(tagline).toBeInViewport();
      await expect(description).toBeInViewport();
      await expect(ctaButtons).toBeInViewport();
    });

    test('should clearly communicate key differentiators', async ({ page }) => {
      const heroContent = await page.locator('.hero').textContent();
      const lowerContent = heroContent.toLowerCase();

      // Must mention persistent storage
      expect(lowerContent).toContain('persistent');

      // Must mention memcached compatibility
      expect(lowerContent).toContain('memcached');
    });

    test('should have readable value proposition text', async ({ page }) => {
      const tagline = page.locator('.hero-tagline');
      const taglineText = await tagline.textContent();

      // Tagline should be concise but informative
      expect(taglineText.length).toBeGreaterThan(20);
      expect(taglineText.length).toBeLessThan(100);

      const description = page.locator('.hero-description');
      const descText = await description.textContent();

      // Description should provide more detail
      expect(descText.length).toBeGreaterThan(50);
    });
  });

  /**
   * Test Case 3: Verify logical content flow from hero to footer
   * Expected: Content sections flow logically: Hero -> Features -> Quick Start -> Docs CTA
   */
  test.describe('Test Case 3: Logical Content Flow', () => {
    test('should have sections in correct logical order', async ({ page }) => {
      // Get positions of key sections
      const hero = page.locator('.hero');
      const quickStart = page.locator('#quick-start, .quick-start').first();
      const footer = page.locator('footer');

      const heroBox = await hero.boundingBox();
      const quickStartBox = await quickStart.boundingBox();
      const footerBox = await footer.boundingBox();

      // Verify sections appear in correct order (top to bottom)
      expect(heroBox.y).toBeLessThan(quickStartBox.y);
      expect(quickStartBox.y).toBeLessThan(footerBox.y);
    });

    test('should have all required sections present', async ({ page }) => {
      // Hero section
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Quick Start section
      const quickStart = page.locator('#quick-start, .quick-start').first();
      await expect(quickStart).toBeVisible();

      // Footer
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('should have navigation that matches content sections', async ({ page }) => {
      // Check if navigation exists
      const nav = page.locator('nav');
      if (await nav.count() > 0) {
        // Get navigation links
        const navLinks = nav.locator('a[href^="#"]');
        const navCount = await navLinks.count();

        // Each nav link should point to an existing section
        for (let i = 0; i < navCount; i++) {
          const href = await navLinks.nth(i).getAttribute('href');
          if (href && href !== '#') {
            const targetId = href.replace('#', '');
            const target = page.locator(`#${targetId}`);
            const exists = await target.count() > 0;
            expect(exists, `Section ${href} linked in nav should exist`).toBe(true);
          }
        }
      }
    });

    test('should maintain content flow on mobile viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      // Verify sections still follow same order on mobile
      const hero = page.locator('.hero');
      const quickStart = page.locator('#quick-start, .quick-start').first();

      const heroBox = await hero.boundingBox();
      const quickStartBox = await quickStart.boundingBox();

      expect(heroBox.y).toBeLessThan(quickStartBox.y);
    });
  });

  /**
   * Test Case 4: Check for clear next-step CTAs throughout page
   * Expected: Each section has clear call-to-action or navigation hint
   */
  test.describe('Test Case 4: Clear CTAs Throughout Page', () => {
    test('should have prominent CTAs in hero section', async ({ page }) => {
      // Primary CTA (Get Started)
      const primaryCTA = page.locator('.btn-primary').first();
      await expect(primaryCTA).toBeVisible();

      // Verify it has clear action text
      const primaryText = await primaryCTA.textContent();
      expect(primaryText.toLowerCase()).toMatch(/get started|start|begin|try/);

      // Secondary CTA (GitHub)
      const secondaryCTA = page.locator('.btn-secondary').first();
      await expect(secondaryCTA).toBeVisible();

      const secondaryText = await secondaryCTA.textContent();
      expect(secondaryText.toLowerCase()).toMatch(/github|source|code|view/);
    });

    test('should have clear documentation access in footer', async ({ page }) => {
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();

      // Footer should have documentation link
      const docsLink = footer.locator('a[href*="github"], a[href*="docs"], a:has-text("Documentation")').first();
      await expect(docsLink).toBeVisible();
    });

    test('should have clickable CTA buttons with proper attributes', async ({ page }) => {
      // Check all CTA buttons
      const ctaButtons = page.locator('.btn, button[type="button"], a.btn');
      const count = await ctaButtons.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < Math.min(count, 5); i++) {
        const btn = ctaButtons.nth(i);

        // Should be visible
        await expect(btn).toBeVisible();

        // Should have href or onclick
        const href = await btn.getAttribute('href');
        const onclick = await btn.getAttribute('onclick');
        const hasAction = href || onclick;

        expect(hasAction, 'CTA should have href or onclick').toBeTruthy();
      }
    });

    test('should have external links open in new tab', async ({ page }) => {
      const externalLinks = page.locator('a[href^="http"]');
      const count = await externalLinks.count();

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const target = await link.getAttribute('target');
        const rel = await link.getAttribute('rel');

        // External links should open in new tab
        expect(target).toBe('_blank');

        // Should have security attributes
        expect(rel).toContain('noopener');
      }
    });

    test('should have section headings that act as navigation anchors', async ({ page }) => {
      // Check that major sections have IDs for anchor navigation
      const sections = ['quick-start', 'configuration'];

      for (const sectionId of sections) {
        const section = page.locator(`#${sectionId}, [id="${sectionId}"]`);
        if (await section.count() > 0) {
          const id = await section.getAttribute('id');
          expect(id).toBe(sectionId);
        }
      }
    });
  });

  /**
   * Additional tests for complete user journey experience
   */
  test.describe('User Journey Experience Quality', () => {
    test('should provide clear visual hierarchy', async ({ page }) => {
      // H1 should be largest heading
      const h1 = page.locator('h1').first();
      const h2 = page.locator('h2').first();

      if (await h2.count() > 0) {
        const h1FontSize = await h1.evaluate(el => parseFloat(window.getComputedStyle(el).fontSize));
        const h2FontSize = await h2.evaluate(el => parseFloat(window.getComputedStyle(el).fontSize));

        expect(h1FontSize).toBeGreaterThan(h2FontSize);
      }
    });

    test('should maintain readability on different screen sizes', async ({ page }) => {
      const viewports = [
        { width: 1920, height: 1080, name: 'desktop' },
        { width: 768, height: 1024, name: 'tablet' },
        { width: 375, height: 667, name: 'mobile' }
      ];

      for (const vp of viewports) {
        await page.setViewportSize({ width: vp.width, height: vp.height });

        // Check no horizontal overflow
        const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
        expect(scrollWidth, `No horizontal scroll on ${vp.name}`).toBeLessThanOrEqual(vp.width);

        // Main content should remain visible
        const h1 = page.locator('h1');
        await expect(h1).toBeVisible();
      }
    });

    test('should load without JavaScript errors', async ({ page }) => {
      const errors = [];
      page.on('pageerror', err => errors.push(err.message));

      await page.goto(indexPath);
      await page.waitForLoadState('domcontentloaded');

      // No critical errors should occur
      const criticalErrors = errors.filter(err =>
        !err.includes('favicon') && !err.includes('404')
      );
      expect(criticalErrors).toHaveLength(0);
    });
  });
});
