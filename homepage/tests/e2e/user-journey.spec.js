/**
 * User Journey E2E Tests
 * Owner: Scenario 20 - User Journey - Developer Evaluation
 *
 * Validates the primary user journey for developers evaluating MirDB:
 * 1. Landing on homepage and understanding value proposition
 * 2. Navigating through key sections (Features, Quick Start, Architecture)
 * 3. Finding GitHub link to access source code
 * 4. Completing full journey efficiently
 */

const { test, expect } = require('@playwright/test');

// Test configuration
const JOURNEY_TIMEOUT = 120000; // 2 minutes max for full journey

test.describe('User Journey - Developer Evaluation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Complete User Journey', () => {
    test('TC1: Complete full user journey in under 2 minutes', async ({ page }) => {
      test.setTimeout(JOURNEY_TIMEOUT);
      const startTime = Date.now();

      // Step 1: Land on homepage - verify hero is visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('.hero__title')).toContainText('MirDB');

      // Step 2: Understand value proposition - read hero content
      const heroDescription = page.locator('.hero__description');
      await expect(heroDescription).toBeVisible();
      await expect(heroDescription).toContainText('persistence');

      // Step 3: Explore features - scroll to features section
      await page.locator('a[href="#features"]').first().click();
      await expect(page.locator('#features')).toBeInViewport();
      await expect(page.locator('.features-grid')).toBeVisible();

      // Step 4: Try Quick Start - navigate to quick start section
      await page.locator('a[href="#quick-start"]').first().click();
      await expect(page.locator('#quick-start')).toBeInViewport();
      await expect(page.locator('.quick-start__steps')).toBeVisible();

      // Step 5: Deep dive architecture - read architecture section
      await page.locator('a[href="#architecture"]').first().click();
      await expect(page.locator('#architecture')).toBeInViewport();
      await expect(page.locator('.architecture-components')).toBeVisible();

      // Step 6: Access source code - find and click GitHub link
      const githubLink = page.locator('a[href*="github.com"]').first();
      await expect(githubLink).toBeVisible();
      const githubHref = await githubLink.getAttribute('href');
      expect(githubHref).toContain('github.com');

      const endTime = Date.now();
      const journeyDuration = endTime - startTime;

      // Journey should complete in under 2 minutes (120000ms)
      expect(journeyDuration).toBeLessThan(JOURNEY_TIMEOUT);
    });

    test('TC1b: User can navigate without getting lost or confused', async ({ page }) => {
      // Verify navigation is always visible (sticky header)
      await page.evaluate(() => window.scrollTo(0, 500));
      await expect(page.locator('.header')).toBeVisible();

      // Verify all navigation links are present and functional
      const navLinks = page.locator('.nav__list .nav__link');
      await expect(navLinks).toHaveCount(6); // Features, Quick Start, Architecture, API, Config, Contributing

      // Verify each major section is reachable from nav
      const sections = ['features', 'quick-start', 'architecture', 'api-reference', 'configuration', 'contributing'];
      for (const section of sections) {
        const link = page.locator(`a[href="#${section}"]`).first();
        await expect(link).toBeVisible();
      }
    });
  });

  test.describe('Hero CTA to Quick Start', () => {
    test('TC2: Find Quick Start from hero CTA with single click', async ({ page }) => {
      // Verify hero CTA button is visible and properly labeled
      const heroCta = page.locator('.hero__cta');
      await expect(heroCta).toBeVisible();
      await expect(heroCta).toContainText('Quick Start');

      // Verify CTA links to Quick Start section
      const ctaHref = await heroCta.getAttribute('href');
      expect(ctaHref).toBe('#quick-start');

      // Click CTA and verify Quick Start section is in viewport
      await heroCta.click();
      await expect(page.locator('#quick-start')).toBeInViewport();

      // Verify Quick Start content is visible after single click
      await expect(page.locator('.quick-start__steps')).toBeVisible();
      await expect(page.locator('.quick-start__step').first()).toBeVisible();
    });

    test('TC2b: Hero CTA is prominently styled and accessible', async ({ page }) => {
      const heroCta = page.locator('.hero__cta');

      // Verify CTA has proper accessible name
      await expect(heroCta).toBeVisible();

      // Verify CTA is keyboard focusable
      await heroCta.focus();
      await expect(heroCta).toBeFocused();
    });
  });

  test.describe('GitHub Link Accessibility', () => {
    test('TC3: Find GitHub link within 2 clicks from hero', async ({ page }) => {
      // From hero section - GitHub should be accessible within 2 clicks
      // Option 1: Direct link in header/footer (0 clicks to see, 1 click to access)
      const directGithubLink = page.locator('a[href*="github.com/yetone/mirdb"]').first();

      if (await directGithubLink.isVisible()) {
        // Direct link visible - only 1 click needed
        const href = await directGithubLink.getAttribute('href');
        expect(href).toContain('github.com');
      } else {
        // Navigate to Contributing section (1 click) then find GitHub link (1 more click)
        await page.locator('a[href="#contributing"]').first().click();
        await expect(page.locator('#contributing')).toBeInViewport();

        const contributingGithubLink = page.locator('#contributing a[href*="github.com"]').first();
        await expect(contributingGithubLink).toBeVisible();
      }
    });

    test('TC3b: Find GitHub link from Features section within 2 clicks', async ({ page }) => {
      // Navigate to Features section (1 click)
      await page.locator('a[href="#features"]').first().click();
      await expect(page.locator('#features')).toBeInViewport();

      // GitHub link should be visible in header or footer (0 additional navigation)
      // Or reachable via Contributing nav link (1 more click)
      const visibleGithubLink = page.locator('a[href*="github.com"]');
      const count = await visibleGithubLink.count();
      expect(count).toBeGreaterThan(0);
    });

    test('TC3c: Find GitHub link from Architecture section within 2 clicks', async ({ page }) => {
      // Navigate to Architecture section (1 click)
      await page.locator('a[href="#architecture"]').first().click();
      await expect(page.locator('#architecture')).toBeInViewport();

      // Navigate to footer or Contributing for GitHub link (can scroll or click nav)
      // Option: Scroll to footer where GitHub link exists
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

      const footerGithubLink = page.locator('.footer a[href*="github.com"]').first();
      await expect(footerGithubLink).toBeVisible();
    });

    test('TC3d: GitHub link present in footer from any scroll position', async ({ page }) => {
      // Verify footer contains GitHub link
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

      const footerGithubLink = page.locator('.footer__github-link');
      await expect(footerGithubLink).toBeVisible();

      const href = await footerGithubLink.getAttribute('href');
      expect(href).toContain('github.com');
      expect(href).toContain('mirdb');
    });
  });

  test.describe('Hero Section Value Proposition', () => {
    test('TC4: Hero section communicates persistent key-value store', async ({ page }) => {
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      // Check title mentions MirDB
      const title = page.locator('.hero__title');
      await expect(title).toContainText('MirDB');

      // Check tagline or description mentions key-value store
      const tagline = page.locator('.hero__tagline');
      await expect(tagline).toBeVisible();
      const taglineText = await tagline.textContent();
      expect(taglineText.toLowerCase()).toMatch(/key-value/i);

      // Check description mentions persistence
      const description = page.locator('.hero__description');
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText.toLowerCase()).toMatch(/persist/i);
    });

    test('TC4b: Hero section communicates Memcached protocol compatibility', async ({ page }) => {
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      // Check that Memcached is mentioned in hero content
      const heroContent = page.locator('#hero');
      const heroText = await heroContent.textContent();

      expect(heroText.toLowerCase()).toMatch(/memcached/i);
    });

    test('TC4c: Hero section is visually prominent and clear', async ({ page }) => {
      // Verify hero section is the first major content area
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      // Verify key elements are present
      await expect(page.locator('.hero__logo')).toBeVisible();
      await expect(page.locator('.hero__title')).toBeVisible();
      await expect(page.locator('.hero__tagline')).toBeVisible();
      await expect(page.locator('.hero__description')).toBeVisible();
      await expect(page.locator('.hero__cta')).toBeVisible();
    });
  });

  test.describe('Navigation to Major Sections', () => {
    test('TC5: Navigate to all major sections from navigation', async ({ page }) => {
      const majorSections = [
        { href: '#features', id: 'features', name: 'Features' },
        { href: '#quick-start', id: 'quick-start', name: 'Quick Start' },
        { href: '#architecture', id: 'architecture', name: 'Architecture' },
        { href: '#api-reference', id: 'api-reference', name: 'API Reference' },
        { href: '#configuration', id: 'configuration', name: 'Configuration' },
        { href: '#contributing', id: 'contributing', name: 'Contributing' }
      ];

      for (const section of majorSections) {
        // Find nav link for this section
        const navLink = page.locator(`.nav__link[href="${section.href}"]`);
        await expect(navLink).toBeVisible();

        // Click nav link
        await navLink.click();

        // Verify section is in viewport
        const sectionElement = page.locator(`#${section.id}`);
        await expect(sectionElement).toBeInViewport();

        // Verify section has content
        await expect(sectionElement).toBeVisible();
      }
    });

    test('TC5b: Navigation links are clearly labeled', async ({ page }) => {
      const expectedLinks = [
        'Features',
        'Quick Start',
        'Architecture',
        'API Reference',
        'Configuration',
        'Contributing'
      ];

      for (const linkText of expectedLinks) {
        const link = page.locator(`.nav__link:has-text("${linkText}")`);
        await expect(link).toBeVisible();
      }
    });

    test('TC5c: Navigation remains accessible during scroll', async ({ page }) => {
      // Scroll to middle of page
      await page.evaluate(() => window.scrollTo(0, 1000));

      // Verify header with navigation is still visible (sticky header)
      const header = page.locator('.header');
      await expect(header).toBeVisible();

      // Verify navigation links are still accessible
      const navList = page.locator('.nav__list');
      await expect(navList).toBeVisible();
    });

    test('TC5d: Each section has proper heading and content', async ({ page }) => {
      const sectionInfo = [
        { id: 'features', titleId: 'features-title' },
        { id: 'quick-start', titleId: 'quick-start-title' },
        { id: 'architecture', titleId: 'architecture-title' },
        { id: 'api-reference', titleId: 'api-reference-title' },
        { id: 'configuration', titleId: 'configuration-title' },
        { id: 'contributing', titleId: 'contributing-title' }
      ];

      for (const section of sectionInfo) {
        const sectionElement = page.locator(`#${section.id}`);
        await expect(sectionElement).toBeVisible();

        // Verify section has a title
        const title = page.locator(`#${section.titleId}`);
        await expect(title).toBeVisible();

        // Verify title is an h2 (proper heading hierarchy)
        const tagName = await title.evaluate(el => el.tagName.toLowerCase());
        expect(tagName).toBe('h2');
      }
    });
  });

  test.describe('Journey Efficiency', () => {
    test('Journey from landing to understanding capabilities is efficient', async ({ page }) => {
      // User should understand what MirDB does from hero alone
      const heroContent = await page.locator('#hero').textContent();

      // Key information present in hero
      expect(heroContent).toMatch(/MirDB/i);
      expect(heroContent).toMatch(/key-value/i);
      expect(heroContent).toMatch(/memcached/i);
      expect(heroContent).toMatch(/persist/i);
    });

    test('Key features are discoverable in Features section', async ({ page }) => {
      await page.locator('a[href="#features"]').first().click();
      await expect(page.locator('#features')).toBeInViewport();

      // Verify feature cards are present
      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();
      expect(count).toBeGreaterThanOrEqual(4);

      // Verify key features are mentioned
      const featuresText = await page.locator('#features').textContent();
      expect(featuresText).toMatch(/memcached protocol/i);
      expect(featuresText).toMatch(/persistence/i);
      expect(featuresText).toMatch(/lsm tree/i);
      expect(featuresText).toMatch(/compaction/i);
    });

    test('Quick Start provides actionable installation steps', async ({ page }) => {
      await page.locator('a[href="#quick-start"]').first().click();
      await expect(page.locator('#quick-start')).toBeInViewport();

      // Verify step-by-step instructions exist
      const steps = page.locator('.quick-start__step');
      const stepCount = await steps.count();
      expect(stepCount).toBeGreaterThanOrEqual(3);

      // Verify code blocks are present for commands
      const codeBlocks = page.locator('#quick-start .code-block');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThanOrEqual(1);
    });
  });
});
