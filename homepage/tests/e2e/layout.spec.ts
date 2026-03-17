/**
 * Layout and Structure E2E Tests
 * Owner: Scenario 1 - Homepage Layout and Structure
 *
 * Tests the overall page structure contains all required sections
 * in the correct order: Header/Navigation, Hero Section, Features Section,
 * Demo Section, Getting Started Section, and Footer.
 */

import { test, expect } from '@playwright/test';

test.describe('Homepage Layout and Structure', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page loads successfully with HTTP 200 status', async ({ page }) => {
    // Navigate and verify successful response
    const response = await page.goto('/');
    expect(response?.status()).toBe(200);

    // Verify page has loaded with correct title
    await expect(page).toHaveTitle(/MirDB/);
  });

  test('TC2: Header element exists containing navigation links and logo', async ({ page }) => {
    // Verify header element exists
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Verify logo is present
    const logo = header.locator('.header__logo');
    await expect(logo).toBeVisible();
    await expect(logo.locator('img')).toBeVisible();
    await expect(logo.locator('.header__logo-text')).toHaveText('MirDB');

    // Verify navigation links exist
    const nav = header.locator('nav.header__nav');
    await expect(nav).toBeVisible();

    const navLinks = nav.locator('.header__nav-link');
    await expect(navLinks).toHaveCount(4);

    // Verify specific navigation items
    await expect(navLinks.nth(0)).toHaveText('Features');
    await expect(navLinks.nth(1)).toHaveText('Demo');
    await expect(navLinks.nth(2)).toHaveText('Getting Started');
    await expect(navLinks.nth(3)).toHaveText('GitHub');
  });

  test('TC3: Hero section exists with headline, subheadline, and CTA buttons', async ({ page }) => {
    // Verify hero section exists
    const hero = page.locator('section#hero.hero');
    await expect(hero).toBeVisible();

    // Verify headline
    const headline = hero.locator('.hero__title');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('Persistent Key-Value Store');
    await expect(headline).toContainText('Memcached Protocol');

    // Verify subheadline
    const subheadline = hero.locator('.hero__subtitle');
    await expect(subheadline).toBeVisible();
    await expect(subheadline).toContainText('Rust');
    await expect(subheadline).toContainText('LSM Trees');

    // Verify CTA buttons
    const ctaButtons = hero.locator('.hero__cta .btn');
    await expect(ctaButtons).toHaveCount(2);
    await expect(ctaButtons.nth(0)).toHaveText('Get Started');
    await expect(ctaButtons.nth(1)).toHaveText('View on GitHub');
  });

  test('TC4: Features section exists with 4 feature cards in grid layout', async ({ page }) => {
    // Verify features section exists
    const features = page.locator('section#features.features');
    await expect(features).toBeVisible();

    // Verify heading
    const heading = features.locator('#features-heading');
    await expect(heading).toHaveText('Features');

    // Verify grid container
    const grid = features.locator('.features-grid');
    await expect(grid).toBeVisible();

    // Verify 4 feature cards
    const featureCards = grid.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify each card has required elements
    for (let i = 0; i < 4; i++) {
      const card = featureCards.nth(i);
      await expect(card.locator('.feature-icon')).toBeVisible();
      await expect(card.locator('.feature-title')).toBeVisible();
      await expect(card.locator('.feature-description')).toBeVisible();
    }

    // Verify feature titles
    await expect(featureCards.nth(0).locator('.feature-title')).toHaveText('Memcached Protocol');
    await expect(featureCards.nth(1).locator('.feature-title')).toHaveText('Durable Storage');
    await expect(featureCards.nth(2).locator('.feature-title')).toHaveText('LSM Tree');
    await expect(featureCards.nth(3).locator('.feature-title')).toHaveText('Rust Powered');
  });

  test('TC5: Demo section exists with demo asset displayed', async ({ page }) => {
    // Verify demo section exists
    const demo = page.locator('section#demo.demo');
    await expect(demo).toBeVisible();

    // Verify heading
    const heading = demo.locator('#demo-heading');
    await expect(heading).toHaveText('See MirDB in Action');

    // Verify demo image/asset
    const demoImage = demo.locator('.demo__image');
    await expect(demoImage).toBeVisible();
    await expect(demoImage).toHaveAttribute('src', /usage\.gif/);

    // Verify caption
    const caption = demo.locator('.demo__caption');
    await expect(caption).toBeVisible();
  });

  test('TC6: Getting started section exists with installation and example subsections', async ({ page }) => {
    // Verify getting started section exists
    const gettingStarted = page.locator('section#getting-started.getting-started');
    await expect(gettingStarted).toBeVisible();

    // Verify heading
    const heading = gettingStarted.locator('#getting-started-heading');
    await expect(heading).toHaveText('Getting Started');

    // Verify installation subsection
    const installation = gettingStarted.locator('#installation');
    await expect(installation).toBeVisible();
    await expect(installation.locator('h3')).toHaveText('Installation');
    await expect(installation.locator('.code-block')).toBeVisible();

    // Verify example subsection
    const example = gettingStarted.locator('#example');
    await expect(example).toBeVisible();
    await expect(example.locator('h3')).toHaveText('Quick Example');
    await expect(example.locator('.code-block')).toBeVisible();
  });

  test('TC7: Footer element exists with GitHub link and copyright notice', async ({ page }) => {
    // Verify footer exists
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify GitHub link
    const githubLink = footer.locator('.footer__link--github');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveText('View on GitHub');
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify copyright notice
    const copyright = footer.locator('.footer__copyright');
    await expect(copyright).toBeVisible();
    await expect(copyright).toContainText('MirDB');
    await expect(copyright).toContainText('All rights reserved');
  });

  test('TC8: HTML uses semantic elements: header, main, section, footer', async ({ page }) => {
    // Verify semantic header element
    const header = page.locator('header[role="banner"]');
    await expect(header).toBeVisible();

    // Verify semantic main element
    const main = page.locator('main[role="main"]');
    await expect(main).toBeVisible();

    // Verify semantic section elements within main
    const sections = main.locator('section');
    await expect(sections).toHaveCount(4); // hero, features, demo, getting-started

    // Verify each section has aria-labelledby for accessibility
    const hero = page.locator('section#hero[aria-labelledby="hero-title"]');
    await expect(hero).toBeVisible();

    const features = page.locator('section#features[aria-labelledby="features-heading"]');
    await expect(features).toBeVisible();

    const demo = page.locator('section#demo[aria-labelledby="demo-heading"]');
    await expect(demo).toBeVisible();

    const gettingStarted = page.locator('section#getting-started[aria-labelledby="getting-started-heading"]');
    await expect(gettingStarted).toBeVisible();

    // Verify semantic footer element
    const footer = page.locator('footer[role="contentinfo"]');
    await expect(footer).toBeVisible();
  });

  test('Sections appear in correct order on the page', async ({ page }) => {
    // Get bounding boxes to verify vertical order
    const header = page.locator('header.header');
    const hero = page.locator('section#hero');
    const features = page.locator('section#features');
    const demo = page.locator('section#demo');
    const gettingStarted = page.locator('section#getting-started');
    const footer = page.locator('footer.footer');

    const headerBox = await header.boundingBox();
    const heroBox = await hero.boundingBox();
    const featuresBox = await features.boundingBox();
    const demoBox = await demo.boundingBox();
    const gettingStartedBox = await gettingStarted.boundingBox();
    const footerBox = await footer.boundingBox();

    // Verify vertical order: header < hero < features < demo < getting-started < footer
    expect(headerBox!.y).toBeLessThan(heroBox!.y);
    expect(heroBox!.y).toBeLessThan(featuresBox!.y);
    expect(featuresBox!.y).toBeLessThan(demoBox!.y);
    expect(demoBox!.y).toBeLessThan(gettingStartedBox!.y);
    expect(gettingStartedBox!.y).toBeLessThan(footerBox!.y);
  });
});
