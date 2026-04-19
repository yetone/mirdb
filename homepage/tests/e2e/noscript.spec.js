/**
 * Progressive Enhancement E2E Tests
 * Owner: Scenario 12 - Progressive Enhancement - No JavaScript Functionality
 *
 * Tests for NFR-6: Homepage must function without JavaScript for basic content access
 *
 * Tests:
 * - Content visibility without JS
 * - Navigation functionality without JS
 * - Code examples accessibility without JS
 * - External links functionality without JS
 * - No JS-only content hiding
 */

import { test as base, expect } from '@playwright/test';

// Base URL for the local server
const BASE_URL = 'http://localhost:3000';

// Create a custom test fixture with JavaScript disabled
const test = base.extend({
  // Override the page fixture to use a context with JavaScript disabled
  page: async ({ browser }, use) => {
    const context = await browser.newContext({
      javaScriptEnabled: false,
      baseURL: BASE_URL
    });
    const page = await context.newPage();
    await use(page);
    await context.close();
  }
});

test.describe('Progressive Enhancement - No JavaScript Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Hero section, features, and content are all visible without JavaScript', async ({ page }) => {
    // Verify Hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify hero title
    const heroTitle = heroSection.locator('.hero__title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MiRDB');

    // Verify hero tagline
    const heroTagline = heroSection.locator('.hero__tagline');
    await expect(heroTagline).toBeVisible();
    const taglineText = await heroTagline.textContent();
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('key-value');

    // Verify CTA buttons are visible
    const ctaSection = heroSection.locator('.hero__cta');
    await expect(ctaSection).toBeVisible();

    // Verify Features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are visible
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Verify each feature card content is readable
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();
      await expect(card.locator('.feature-card__title')).toBeVisible();
      await expect(card.locator('.feature-card__description')).toBeVisible();
    }

    // Verify Quick Start section is visible
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify Configuration section is visible
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify header is visible
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Verify footer is visible
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();
  });

  test('TC2: All navigation links work and navigate correctly without JavaScript', async ({ page }) => {
    // Check header navigation links
    const headerNav = page.locator('header .header__nav');
    await expect(headerNav).toBeVisible();

    // Verify Features link exists and has correct href
    const featuresLink = headerNav.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveAttribute('href', '#features');

    // Click Features link and verify navigation
    await featuresLink.click();
    await expect(page).toHaveURL(/#features/);

    // Verify the features section is in viewport after clicking
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Navigate back to top
    await page.goto(BASE_URL);
    await page.waitForLoadState('domcontentloaded');

    // Verify Docs link (points to quickstart)
    const docsLink = headerNav.locator('a[href="#quickstart"]');
    await expect(docsLink).toBeVisible();

    // Click Docs link and verify navigation
    await docsLink.click();
    await expect(page).toHaveURL(/#quickstart/);

    // Verify quickstart section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Verify hero CTA "Get Started" button works
    await page.goto(BASE_URL);
    await page.waitForLoadState('domcontentloaded');

    const getStartedBtn = page.locator('#hero a[href="#quickstart"]');
    await expect(getStartedBtn).toBeVisible();
    await getStartedBtn.click();
    await expect(page).toHaveURL(/#quickstart/);
  });

  test('TC3: Code examples are visible and readable without JavaScript (copy button may not work)', async ({ page }) => {
    // Navigate to code example section
    const codeExampleSection = page.locator('#code-example');
    await expect(codeExampleSection).toBeVisible();

    // Verify code blocks are visible
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThanOrEqual(1);

    // Verify each code block content is readable
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Verify code content is visible
      const codeContent = codeBlock.locator('.code-block__content');
      await expect(codeContent).toBeVisible();

      // Verify code text is not empty
      const codeText = await codeContent.textContent();
      expect(codeText.trim().length).toBeGreaterThan(0);

      // Verify code header/language label is visible
      const codeHeader = codeBlock.locator('.code-block__header');
      await expect(codeHeader).toBeVisible();
    }

    // Verify Quick Start code examples are also visible
    const quickstartCommands = page.locator('.quickstart__command');
    const quickstartCommandCount = await quickstartCommands.count();
    expect(quickstartCommandCount).toBeGreaterThanOrEqual(1);

    // Verify each command is readable
    for (let i = 0; i < quickstartCommandCount; i++) {
      const command = quickstartCommands.nth(i);
      await expect(command).toBeVisible();
      const commandText = await command.textContent();
      expect(commandText.trim().length).toBeGreaterThan(0);
    }

    // Note: Copy buttons are present but may not function without JS - that's acceptable
    const copyButtons = page.locator('.code-block__copy');
    const copyButtonCount = await copyButtons.count();
    // Copy buttons should exist in DOM even without JS
    expect(copyButtonCount).toBeGreaterThanOrEqual(1);
  });

  test('TC4: External links (GitHub) open correctly without JavaScript', async ({ page }) => {
    // Check GitHub link in header
    const headerGitHubLink = page.locator('header a.header__github');
    await expect(headerGitHubLink).toBeVisible();
    await expect(headerGitHubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(headerGitHubLink).toHaveAttribute('target', '_blank');
    await expect(headerGitHubLink).toHaveAttribute('rel', /noopener/);

    // Check GitHub link in hero section
    const heroGitHubLink = page.locator('#hero a.btn-primary[href*="github"]');
    await expect(heroGitHubLink).toBeVisible();
    await expect(heroGitHubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(heroGitHubLink).toHaveAttribute('target', '_blank');
    await expect(heroGitHubLink).toHaveAttribute('rel', /noopener/);

    // Check GitHub link in footer
    const footerGitHubLink = page.locator('footer a[href*="github.com/yetone/mirdb"]').first();
    await expect(footerGitHubLink).toBeVisible();
    await expect(footerGitHubLink).toHaveAttribute('target', '_blank');
    await expect(footerGitHubLink).toHaveAttribute('rel', /noopener/);

    // Check License link in footer (also external)
    const licenseLink = page.locator('footer a[href*="LICENSE"]');
    await expect(licenseLink).toBeVisible();
    await expect(licenseLink).toHaveAttribute('target', '_blank');
    await expect(licenseLink).toHaveAttribute('rel', /noopener/);

    // Check GitHub Releases link in quickstart
    const releasesLink = page.locator('a[href*="github.com/yetone/mirdb/releases"]');
    await expect(releasesLink).toBeVisible();
    await expect(releasesLink).toHaveAttribute('target', '_blank');
    await expect(releasesLink).toHaveAttribute('rel', /noopener/);
  });

  test('TC5: No critical content is hidden behind JS-only interactions', async ({ page }) => {
    // Verify main content sections are not hidden by CSS that depends on JS
    const mainSections = [
      '#hero',
      '#features',
      '#quickstart',
      '#code-example',
      '#configuration'
    ];

    for (const selector of mainSections) {
      const section = page.locator(selector);
      await expect(section).toBeVisible();

      // Verify section has visible content
      const sectionText = await section.textContent();
      expect(sectionText.trim().length).toBeGreaterThan(0);
    }

    // Verify no elements have display:none or visibility:hidden due to JS
    // Check critical content elements are not hidden
    const criticalElements = [
      '.hero__title',
      '.hero__tagline',
      '.hero__cta',
      '.features__grid',
      '.feature-card',
      '.quickstart__steps',
      '.code-block',
      '.config__table'
    ];

    for (const selector of criticalElements) {
      const elements = page.locator(selector);
      const count = await elements.count();

      if (count > 0) {
        const firstElement = elements.first();
        await expect(firstElement).toBeVisible();

        // Verify element is not hidden via CSS
        const isHidden = await firstElement.evaluate(el => {
          const style = window.getComputedStyle(el);
          return style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0';
        });
        expect(isHidden).toBe(false);
      }
    }

    // Verify navigation is visible (not hidden behind hamburger menu that requires JS)
    const headerNav = page.locator('header .header__nav');
    await expect(headerNav).toBeVisible();

    // Verify skip link is functional for accessibility
    const skipLink = page.locator('a.skip-link');
    // Skip links are typically visually hidden until focused, but should exist
    const skipLinkExists = await skipLink.count();
    expect(skipLinkExists).toBe(1);

    // Verify main content container
    const mainContent = page.locator('main#main');
    await expect(mainContent).toBeVisible();

    // Verify configuration table is visible and contains data
    const configTable = page.locator('.config__table');
    await expect(configTable).toBeVisible();

    const configRows = page.locator('.config__row');
    const rowCount = await configRows.count();
    expect(rowCount).toBeGreaterThanOrEqual(1);
  });

  test('Text content is selectable and readable without JavaScript', async ({ page }) => {
    // Verify text in hero is selectable
    const heroTagline = page.locator('.hero__tagline');
    await expect(heroTagline).toBeVisible();

    // Verify feature descriptions are readable
    const featureDesc = page.locator('.feature-card__description').first();
    await expect(featureDesc).toBeVisible();
    const descText = await featureDesc.textContent();
    expect(descText.length).toBeGreaterThan(20);

    // Verify code content is readable
    const codeContent = page.locator('.code-block__code').first();
    await expect(codeContent).toBeVisible();
    const codeText = await codeContent.textContent();
    expect(codeText.trim().length).toBeGreaterThan(0);
  });

  test('Page structure and semantic HTML work without JavaScript', async ({ page }) => {
    // Verify semantic structure
    const header = page.locator('header');
    await expect(header).toBeVisible();

    const main = page.locator('main');
    await expect(main).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify sections have proper IDs for navigation
    const sections = ['#hero', '#features', '#quickstart', '#code-example', '#configuration'];
    for (const id of sections) {
      const section = page.locator(id);
      const exists = await section.count();
      expect(exists).toBe(1);
    }

    // Verify aria labels exist for accessibility
    const ariaLabelledSections = page.locator('[aria-labelledby]');
    const ariaCount = await ariaLabelledSections.count();
    expect(ariaCount).toBeGreaterThanOrEqual(1);
  });
});
