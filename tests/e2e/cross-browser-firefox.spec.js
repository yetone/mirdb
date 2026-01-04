// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests for Firefox (NFR-4)
 * These tests verify that the MirDB landing page works correctly in Mozilla Firefox.
 */

test.describe('Cross-Browser Compatibility - Firefox', () => {
  // Only run these tests in Firefox
  test.skip(({ browserName }) => browserName !== 'firefox', 'Firefox-only tests');

  test.describe('Test Case 1: Page renders correctly in Firefox', () => {
    test('should load page in Firefox without visual issues', async ({ page }) => {
      await page.goto('/');

      // Verify page title is correct
      await expect(page).toHaveTitle(/MirDB/);

      // Verify hero section renders correctly
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify hero title renders correctly
      const heroTitle = page.locator('.hero h1');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      // Verify tagline renders correctly
      const tagline = page.locator('.hero .tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Key-Value Store');

      // Verify description renders correctly
      const description = page.locator('.hero .description');
      await expect(description).toBeVisible();
    });

    test('should render all sections in Firefox', async ({ page }) => {
      await page.goto('/');

      // Verify features section renders
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Verify features title
      const featuresTitle = featuresSection.locator('h2');
      await expect(featuresTitle).toContainText('Key Features');

      // Verify getting started section renders
      const gettingStartedSection = page.locator('#getting-started');
      await gettingStartedSection.scrollIntoViewIfNeeded();
      await expect(gettingStartedSection).toBeVisible();

      // Verify commands section renders
      const commandsSection = page.locator('.commands');
      await commandsSection.scrollIntoViewIfNeeded();
      await expect(commandsSection).toBeVisible();

      // Verify footer renders
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });

    test('should render feature cards correctly in Firefox', async ({ page }) => {
      await page.goto('/');

      // Navigate to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      // Verify all 4 feature cards render
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(4);

      // Verify each feature card has a title and description
      const firstCard = featureCards.first();
      await expect(firstCard.locator('h3')).toBeVisible();
      await expect(firstCard.locator('p')).toBeVisible();

      // Verify feature card titles
      const cardTitles = page.locator('.feature-card h3');
      await expect(cardTitles.nth(0)).toContainText('Memcached Compatibility');
      await expect(cardTitles.nth(1)).toContainText('Persistent Storage');
      await expect(cardTitles.nth(2)).toContainText('LSM Tree Architecture');
      await expect(cardTitles.nth(3)).toContainText('Built with Rust');
    });

    test('should have no horizontal scroll in Firefox', async ({ page }) => {
      await page.goto('/');

      // Verify no horizontal scrollbar
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });
  });

  test.describe('Test Case 2: All links navigate correctly in Firefox', () => {
    test('should have all navigation links visible and clickable', async ({ page }) => {
      await page.goto('/');

      // Verify navbar is visible
      const navbar = page.locator('.navbar');
      await expect(navbar).toBeVisible();

      // Verify Features link works
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await featuresLink.click();

      // Verify we scrolled to features section
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();

      // Verify Getting Started link works
      const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');
      await expect(gettingStartedLink).toBeVisible();
      await gettingStartedLink.click();

      // Verify we scrolled to getting started section
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeInViewport();
    });

    test('should have external links with proper attributes', async ({ page }) => {
      await page.goto('/');

      // Check GitHub link in navbar
      const githubNavLink = page.locator('.nav-links a[href*="github.com/yetone/mirdb"]').first();
      await expect(githubNavLink).toBeVisible();
      await expect(githubNavLink).toHaveAttribute('target', '_blank');
      await expect(githubNavLink).toHaveAttribute('rel', /noopener/);

      // Check Docs link
      const docsLink = page.locator('.nav-links a[href*="github.com/yetone/mirdb#readme"]');
      await expect(docsLink).toBeVisible();
      await expect(docsLink).toHaveAttribute('target', '_blank');
      await expect(docsLink).toHaveAttribute('rel', /noopener/);
    });

    test('should navigate to features section via CTA button', async ({ page }) => {
      await page.goto('/');

      // Click Get Started CTA
      const ctaButton = page.locator('.cta-primary[href="#getting-started"]');
      await expect(ctaButton).toBeVisible();
      await ctaButton.click();

      // Verify navigation to getting started section
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeInViewport();
    });

    test('should have footer links properly configured', async ({ page }) => {
      await page.goto('/');

      // Scroll to footer
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();

      // Check footer GitHub link
      const footerGithubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();
      await expect(footerGithubLink).toBeVisible();
      await expect(footerGithubLink).toHaveAttribute('target', '_blank');

      // Check Report Issue link
      const issuesLink = footer.locator('a[href*="github.com/yetone/mirdb/issues"]');
      await expect(issuesLink).toBeVisible();
      await expect(issuesLink).toHaveAttribute('target', '_blank');
    });
  });

  test.describe('Test Case 3: CSS styles render correctly in Firefox', () => {
    test('should render CSS Grid layout for feature cards', async ({ page }) => {
      await page.goto('/');

      // Navigate to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      // Verify feature-grid uses CSS Grid
      const featureGrid = page.locator('.feature-grid');
      const display = await featureGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('grid');
    });

    test('should render CSS Flexbox layout for navigation', async ({ page }) => {
      await page.goto('/');

      // Verify navbar uses flexbox
      const navbar = page.locator('.navbar');
      const navbarDisplay = await navbar.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(navbarDisplay).toBe('flex');

      // Verify nav-links uses flexbox
      const navLinks = page.locator('.nav-links');
      const navLinksDisplay = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(navLinksDisplay).toBe('flex');
    });

    test('should render hero section with linear gradient background', async ({ page }) => {
      await page.goto('/');

      const hero = page.locator('.hero');
      const backgroundImage = await hero.evaluate((el) => {
        return window.getComputedStyle(el).backgroundImage;
      });

      // Verify gradient is applied (Firefox renders gradients)
      expect(backgroundImage).toContain('linear-gradient');
    });

    test('should render code blocks with correct styling', async ({ page }) => {
      await page.goto('/');

      // Navigate to getting started section
      const gettingStartedSection = page.locator('#getting-started');
      await gettingStartedSection.scrollIntoViewIfNeeded();

      // Check code example styling
      const codeExample = page.locator('.code-example').first();
      await expect(codeExample).toBeVisible();

      // Verify dark background color for code blocks
      const backgroundColor = await codeExample.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      // The code-bg color is #1e293b which converts to rgb(30, 41, 59)
      expect(backgroundColor).toMatch(/rgb\(30,\s*41,\s*59\)/);
    });

    test('should render buttons with proper styling', async ({ page }) => {
      await page.goto('/');

      // Check primary CTA button styling
      const primaryCta = page.locator('.cta-primary').first();
      const primaryBgColor = await primaryCta.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      // Primary color is #2563eb which converts to rgb(37, 99, 235)
      expect(primaryBgColor).toMatch(/rgb\(37,\s*99,\s*235\)/);

      // Check secondary CTA button styling
      const secondaryCta = page.locator('.cta-secondary').first();
      const borderColor = await secondaryCta.evaluate((el) => {
        return window.getComputedStyle(el).borderColor;
      });
      // Border color is #e5e7eb which converts to rgb(229, 231, 235)
      expect(borderColor).toMatch(/rgb\(229,\s*231,\s*235\)/);
    });

    test('should render smooth scrolling behavior', async ({ page }) => {
      await page.goto('/');

      // Check that smooth scrolling is enabled via CSS
      const scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });
      expect(scrollBehavior).toBe('smooth');
    });

    test('should render sticky navigation in Firefox', async ({ page }) => {
      await page.goto('/');

      // Check navbar is sticky
      const navbar = page.locator('.navbar');
      const position = await navbar.evaluate((el) => {
        return window.getComputedStyle(el).position;
      });
      expect(position).toBe('sticky');
    });

    test('should apply CSS custom properties correctly', async ({ page }) => {
      await page.goto('/');

      // Verify CSS custom properties are working
      const body = page.locator('body');
      const color = await body.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      // Text color is #1f2937 which converts to rgb(31, 41, 55)
      expect(color).toMatch(/rgb\(31,\s*41,\s*55\)/);
    });

    test('should render border-radius on cards correctly', async ({ page }) => {
      await page.goto('/');

      // Navigate to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      // Check border-radius on feature cards
      const featureCard = page.locator('.feature-card').first();
      const borderRadius = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      // Border radius is 0.75rem = 12px
      expect(borderRadius).toBe('12px');
    });

    test('should render font families correctly in Firefox', async ({ page }) => {
      await page.goto('/');

      // Check system font stack is applied
      const body = page.locator('body');
      const fontFamily = await body.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      // Firefox may not include system-ui in computed style, but should have other system fonts
      // The CSS uses: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif
      expect(fontFamily).toMatch(/(-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|sans-serif)/i);
    });
  });
});
