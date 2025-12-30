// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: No JavaScript Core Content
 * Scenario: Verify that core content is accessible without JavaScript
 * NFR-6: Page must function without JavaScript for core content
 */

test.describe('No JavaScript Core Content', () => {
  // Use a context with JavaScript disabled for all tests in this suite
  test.use({ javaScriptEnabled: false });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Load page with JavaScript disabled
   * Input: Load page with JavaScript disabled
   * Expected: Hero section content is visible including headline and value proposition
   */
  test('TC1: Hero section content is visible with JavaScript disabled', async ({ page }) => {
    // Verify the page loaded successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section exists and is visible
    const heroSection = page.locator('#hero.hero-section');
    await expect(heroSection).toBeVisible();

    // Verify the main headline (h1) is visible and has content
    const headline = page.locator('#hero h1');
    await expect(headline).toBeVisible();
    const headlineText = await headline.textContent();
    expect(headlineText).toBeTruthy();
    expect(headlineText.length).toBeGreaterThan(10);

    // Verify headline mentions persistent or Memcached (value proposition)
    const lowerHeadline = headlineText.toLowerCase();
    const hasValueProp = lowerHeadline.includes('persistent') || lowerHeadline.includes('memcached');
    expect(hasValueProp).toBeTruthy();

    // Verify the value proposition description is visible
    const heroDescription = page.locator('#hero .hero-description');
    await expect(heroDescription).toBeVisible();
    const descriptionText = await heroDescription.textContent();
    expect(descriptionText).toBeTruthy();
    expect(descriptionText.length).toBeGreaterThan(50);

    // Verify CTA buttons are visible
    const getStartedBtn = page.locator('#hero .btn-primary');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toContainText('Get Started');

    const githubBtn = page.locator('#hero .btn-secondary');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toContainText('GitHub');

    // Verify CTA buttons have valid href attributes (functional without JS)
    await expect(getStartedBtn).toHaveAttribute('href', '#getting-started');
    const githubHref = await githubBtn.getAttribute('href');
    expect(githubHref).toContain('github.com');
  });

  /**
   * Test Case 2: Check Features section with JS disabled
   * Input: Check Features section with JS disabled
   * Expected: All feature cards/items are visible and readable
   */
  test('TC2: Features section is visible and readable with JavaScript disabled', async ({ page }) => {
    // Verify Features section exists and is visible
    const featuresSection = page.locator('#features.features-section');
    await expect(featuresSection).toBeVisible();

    // Verify section heading is visible
    const featuresHeading = page.locator('#features h2');
    await expect(featuresHeading).toBeVisible();
    await expect(featuresHeading).toHaveText('Features');

    // Verify features grid/container is visible
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify multiple feature cards exist and are visible
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4); // At least 4 key features expected

    // Verify each feature card has visible content
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();

      // Verify each card has a heading
      const cardHeading = card.locator('h3');
      await expect(cardHeading).toBeVisible();
      const headingText = await cardHeading.textContent();
      expect(headingText).toBeTruthy();
      expect(headingText.length).toBeGreaterThan(3);

      // Verify each card has a description
      const cardDescription = card.locator('p');
      await expect(cardDescription).toBeVisible();
      const descriptionText = await cardDescription.textContent();
      expect(descriptionText).toBeTruthy();
      expect(descriptionText.length).toBeGreaterThan(20);
    }

    // Verify specific key features are present
    const memcachedCard = page.locator('.feature-card', { hasText: 'Memcached Protocol' });
    await expect(memcachedCard).toBeVisible();

    const persistentCard = page.locator('.feature-card', { hasText: 'Persistent Storage' });
    await expect(persistentCard).toBeVisible();

    const lsmCard = page.locator('.feature-card', { hasText: 'LSM Tree' });
    await expect(lsmCard).toBeVisible();

    const asyncCard = page.locator('.feature-card', { hasText: 'Async Performance' });
    await expect(asyncCard).toBeVisible();
  });

  /**
   * Test Case 3: Check Getting Started section with JS disabled
   * Input: Check Getting Started section with JS disabled
   * Expected: Code examples and instructions are visible
   */
  test('TC3: Getting Started section is visible with code examples with JavaScript disabled', async ({ page }) => {
    // Verify Getting Started section exists and is visible
    const gettingStartedSection = page.locator('#getting-started.getting-started-section');
    await expect(gettingStartedSection).toBeVisible();

    // Verify section heading is visible
    const sectionHeading = page.locator('#getting-started h2');
    await expect(sectionHeading).toBeVisible();
    await expect(sectionHeading).toHaveText('Getting Started');

    // Verify steps are visible
    const steps = page.locator('#getting-started .step');
    const stepCount = await steps.count();
    expect(stepCount).toBeGreaterThanOrEqual(2); // At least build and run steps

    // Verify each step has visible heading and content
    for (let i = 0; i < stepCount; i++) {
      const step = steps.nth(i);
      await expect(step).toBeVisible();

      // Verify step heading
      const stepHeading = step.locator('h3');
      await expect(stepHeading).toBeVisible();
    }

    // Verify code blocks are visible and contain code
    const codeBlocks = page.locator('#getting-started pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check that code blocks have actual content
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();
      const codeText = await codeBlock.textContent();
      expect(codeText).toBeTruthy();
      expect(codeText.trim().length).toBeGreaterThan(5);
    }

    // Verify specific code examples are present
    let foundBuildCommand = false;
    let foundServerCommand = false;
    let foundClientExample = false;

    for (let i = 0; i < codeBlockCount; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText.includes('cargo build')) {
        foundBuildCommand = true;
      }
      if (codeText.includes('mirdb-server')) {
        foundServerCommand = true;
      }
      if (codeText.includes('telnet') || (codeText.includes('set') && codeText.includes('get'))) {
        foundClientExample = true;
      }
    }

    expect(foundBuildCommand).toBeTruthy();
    expect(foundServerCommand).toBeTruthy();
    expect(foundClientExample).toBeTruthy();
  });

  /**
   * Test Case 4: Check navigation with JS disabled
   * Input: Check navigation with JS disabled
   * Expected: Navigation links are visible and functional (may be expanded view)
   */
  test('TC4: Navigation links are visible and functional with JavaScript disabled', async ({ page }) => {
    // Verify header is visible
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Verify navigation element is visible
    const nav = page.locator('nav.main-nav');
    await expect(nav).toBeVisible();

    // Verify logo and brand name are visible
    const logoText = page.locator('.logo-text');
    await expect(logoText).toBeVisible();
    await expect(logoText).toHaveText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();

    // Verify navigation list is visible
    const navList = page.locator('.nav-list');
    await expect(navList).toBeVisible();

    // Verify all navigation links are visible
    const navLinks = page.locator('.nav-list .nav-link');
    const linkCount = await navLinks.count();
    expect(linkCount).toBe(5); // Features, How It Works, Getting Started, Documentation, GitHub

    // Verify each nav link is visible and has valid href
    const expectedLinks = [
      { text: 'Features', href: '#features' },
      { text: 'How It Works', href: '#how-it-works' },
      { text: 'Getting Started', href: '#getting-started' },
      { text: 'Documentation', href: '#documentation' },
      { text: 'GitHub', hrefContains: 'github.com' }
    ];

    for (const expected of expectedLinks) {
      const link = page.locator('.nav-link', { hasText: expected.text });
      await expect(link).toBeVisible();

      if (expected.href) {
        await expect(link).toHaveAttribute('href', expected.href);
      } else if (expected.hrefContains) {
        const href = await link.getAttribute('href');
        expect(href).toContain(expected.hrefContains);
      }
    }

    // Verify anchor links work (navigate to section)
    // Test Features link - should have valid href that points to existing section
    const featuresLink = page.locator('.nav-link', { hasText: 'Features' });
    const featuresHref = await featuresLink.getAttribute('href');
    expect(featuresHref).toBe('#features');

    // Verify the target section exists
    const featuresSection = page.locator(featuresHref);
    await expect(featuresSection).toBeVisible();

    // Test Getting Started link
    const gettingStartedLink = page.locator('.nav-link', { hasText: 'Getting Started' });
    const gettingStartedHref = await gettingStartedLink.getAttribute('href');
    expect(gettingStartedHref).toBe('#getting-started');

    // Verify the target section exists
    const gettingStartedSection = page.locator(gettingStartedHref);
    await expect(gettingStartedSection).toBeVisible();

    // Test Documentation link
    const documentationLink = page.locator('.nav-link', { hasText: 'Documentation' });
    const documentationHref = await documentationLink.getAttribute('href');
    expect(documentationHref).toBe('#documentation');

    // Verify the target section exists
    const documentationSection = page.locator(documentationHref);
    await expect(documentationSection).toBeVisible();

    // GitHub link should open externally (has valid external URL)
    const githubLink = page.locator('.nav-link', { hasText: 'GitHub' });
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  /**
   * Additional test: Verify footer is visible without JavaScript
   */
  test('Footer is visible and functional with JavaScript disabled', async ({ page }) => {
    // Verify footer is visible
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify footer contains copyright information
    const footerText = await footer.textContent();
    expect(footerText).toContain('MirDB');

    // Verify footer navigation links are visible
    const footerNav = page.locator('.footer-nav');
    await expect(footerNav).toBeVisible();

    // Verify footer links have valid hrefs
    const footerLinks = page.locator('.footer-nav a');
    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount).toBeGreaterThan(0);

    // Each link should have a valid href
    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeVisible();
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
    }
  });

  /**
   * Additional test: Verify documentation section is visible without JavaScript
   */
  test('Documentation section is visible with JavaScript disabled', async ({ page }) => {
    // Verify Documentation section exists and is visible
    const documentationSection = page.locator('#documentation.documentation-section');
    await expect(documentationSection).toBeVisible();

    // Verify section heading
    const sectionHeading = page.locator('#documentation h2');
    await expect(sectionHeading).toBeVisible();
    await expect(sectionHeading).toHaveText('Documentation');

    // Verify supported commands table is visible
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    // Verify table has header and rows
    const tableHeaders = page.locator('.commands-table th');
    await expect(tableHeaders.first()).toBeVisible();

    const tableRows = page.locator('.commands-table tbody tr');
    const rowCount = await tableRows.count();
    expect(rowCount).toBeGreaterThan(0);

    // Verify configuration code block is visible
    const configCodeBlock = page.locator('#documentation pre code');
    await expect(configCodeBlock).toBeVisible();
    const configText = await configCodeBlock.textContent();
    expect(configText).toContain('addr');
    expect(configText).toContain('work_dir');
  });

  /**
   * Additional test: Verify all content is in static HTML (no hidden/JS-dependent elements)
   */
  test('All main content sections exist in static HTML', async ({ page }) => {
    // Verify main element exists
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Verify all expected sections exist within main
    const sections = ['hero', 'features', 'getting-started', 'documentation'];

    for (const sectionId of sections) {
      const section = page.locator(`#${sectionId}`);
      await expect(section).toBeVisible();

      // Each section should have an h2 (except hero which has h1)
      if (sectionId === 'hero') {
        const h1 = section.locator('h1');
        await expect(h1).toBeVisible();
      } else {
        const h2 = section.locator('h2');
        await expect(h2).toBeVisible();
      }
    }

    // Verify no elements are hidden by default requiring JS to show
    // Check that no content has display:none or visibility:hidden
    const allVisibleContent = await page.evaluate(() => {
      const elementsToCheck = document.querySelectorAll('main h1, main h2, main h3, main p, main pre, main table');
      let allVisible = true;

      elementsToCheck.forEach(el => {
        const styles = window.getComputedStyle(el);
        if (styles.display === 'none' || styles.visibility === 'hidden') {
          allVisible = false;
        }
      });

      return allVisible;
    });

    expect(allVisibleContent).toBeTruthy();
  });
});
