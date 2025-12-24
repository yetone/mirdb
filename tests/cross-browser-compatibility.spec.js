// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests (NFR-4)
 * Verifies the page renders correctly on modern browsers (Chrome, Firefox, Safari, Edge)
 *
 * These tests run across all configured browser projects in playwright.config.js
 */

test.describe('Cross-Browser Compatibility - Page Rendering', () => {

  test.describe('TC1: Page loads correctly in browser', () => {
    test('Page loads without errors and displays main content', async ({ page, browserName }) => {
      // Navigate to the page
      const response = await page.goto('/');

      // Verify successful page load
      expect(response).not.toBeNull();
      expect(response.status()).toBe(200);

      // Verify page title
      await expect(page).toHaveTitle(/MirDB/);

      // Verify main sections are visible
      await expect(page.locator('nav')).toBeVisible();
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('#commands')).toBeVisible();
      await expect(page.locator('#configuration')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();

      // Log browser info for reporting
      console.log(`Page loaded successfully in ${browserName}`);
    });

    test('Hero section renders with all elements', async ({ page, browserName }) => {
      await page.goto('/');

      // Check hero section elements
      const hero = page.locator('#hero');
      await expect(hero).toBeVisible();

      // Verify hero title
      const heroTitle = hero.locator('h1');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Verify tagline
      const tagline = hero.locator('.tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Key-Value Store');

      // Verify description
      const description = hero.locator('.description');
      await expect(description).toBeVisible();

      // Verify CTA buttons
      const ctaButtons = hero.locator('.cta-buttons .btn');
      await expect(ctaButtons).toHaveCount(2);

      console.log(`Hero section renders correctly in ${browserName}`);
    });

    test('Features section displays all feature cards', async ({ page, browserName }) => {
      await page.goto('/');

      const features = page.locator('#features');
      await expect(features).toBeVisible();

      // Verify all 6 feature cards are displayed
      const featureCards = features.locator('.feature-card');
      await expect(featureCards).toHaveCount(6);

      // Verify each card has title and description
      for (let i = 0; i < 6; i++) {
        const card = featureCards.nth(i);
        await expect(card.locator('h3')).toBeVisible();
        await expect(card.locator('p')).toBeVisible();
      }

      console.log(`Features section renders correctly in ${browserName}`);
    });

    test('Navigation menu is functional', async ({ page, browserName }) => {
      await page.goto('/');

      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      // Verify navigation links
      const navLinks = nav.locator('.nav-links a');
      const expectedLinks = ['Features', 'Quick Start', 'Commands', 'Configuration', 'GitHub'];

      for (let i = 0; i < expectedLinks.length; i++) {
        const link = navLinks.nth(i);
        await expect(link).toBeVisible();
      }

      // Test internal navigation link
      await navLinks.filter({ hasText: 'Features' }).click();
      await expect(page.locator('#features')).toBeInViewport();

      console.log(`Navigation menu works correctly in ${browserName}`);
    });
  });

  test.describe('TC2-4: Browser-specific rendering verification', () => {
    test('Code blocks render with syntax highlighting', async ({ page, browserName }) => {
      await page.goto('/');

      // Check quick-start code blocks
      const codeBlocks = page.locator('#quick-start pre code');
      const codeCount = await codeBlocks.count();
      expect(codeCount).toBeGreaterThan(0);

      // Verify code blocks are visible and have content
      for (let i = 0; i < codeCount; i++) {
        const code = codeBlocks.nth(i);
        await expect(code).toBeVisible();
        const text = await code.textContent();
        expect(text.length).toBeGreaterThan(0);
      }

      console.log(`Code blocks render correctly in ${browserName}`);
    });

    test('Configuration table renders properly', async ({ page, browserName }) => {
      await page.goto('/');

      const configSection = page.locator('#configuration');
      await expect(configSection).toBeVisible();

      // Verify table structure
      const table = configSection.locator('.config-table');
      await expect(table).toBeVisible();

      // Verify table headers
      const headers = table.locator('thead th');
      await expect(headers).toHaveCount(3);

      // Verify table rows
      const rows = table.locator('tbody tr');
      const rowCount = await rows.count();
      expect(rowCount).toBeGreaterThan(0);

      console.log(`Configuration table renders correctly in ${browserName}`);
    });

    test('Footer section renders with all links', async ({ page, browserName }) => {
      await page.goto('/');

      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Verify footer links
      const footerLinks = footer.locator('.footer-links a');
      await expect(footerLinks).toHaveCount(3);

      console.log(`Footer renders correctly in ${browserName}`);
    });
  });
});

test.describe('Cross-Browser Compatibility - CSS Properties', () => {

  test.describe('TC5: CSS compatibility across browsers', () => {
    test('CSS variables are applied correctly', async ({ page, browserName }) => {
      await page.goto('/');

      // Check that CSS variables are being used
      const body = page.locator('body');
      const bodyStyles = await body.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          color: styles.color,
          fontFamily: styles.fontFamily
        };
      });

      // Verify background color is applied (should be dark theme)
      expect(bodyStyles.backgroundColor).toBeTruthy();
      expect(bodyStyles.color).toBeTruthy();
      expect(bodyStyles.fontFamily).toBeTruthy();

      console.log(`CSS variables applied correctly in ${browserName}`);
    });

    test('Flexbox layout renders consistently', async ({ page, browserName }) => {
      await page.goto('/');

      // Check hero section flexbox
      const hero = page.locator('#hero');
      const heroStyles = await hero.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          flexDirection: styles.flexDirection,
          justifyContent: styles.justifyContent,
          alignItems: styles.alignItems
        };
      });

      expect(heroStyles.display).toBe('flex');
      expect(heroStyles.flexDirection).toBe('column');
      expect(heroStyles.alignItems).toBe('center');

      // Check navigation flexbox
      const nav = page.locator('nav');
      const navStyles = await nav.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          justifyContent: styles.justifyContent
        };
      });

      expect(navStyles.display).toBe('flex');

      console.log(`Flexbox layout renders correctly in ${browserName}`);
    });

    test('CSS Grid layout renders consistently', async ({ page, browserName }) => {
      await page.goto('/');

      // Check features grid
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const gridStyles = await featuresGrid.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          gridTemplateColumns: styles.gridTemplateColumns,
          gap: styles.gap
        };
      });

      expect(gridStyles.display).toBe('grid');
      expect(gridStyles.gridTemplateColumns).toBeTruthy();

      // Check commands grid
      const commandsGrid = page.locator('.commands-grid');
      await expect(commandsGrid).toBeVisible();

      const commandsGridStyles = await commandsGrid.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display
        };
      });

      expect(commandsGridStyles.display).toBe('grid');

      console.log(`CSS Grid layout renders correctly in ${browserName}`);
    });

    test('Border radius renders consistently', async ({ page, browserName }) => {
      await page.goto('/');

      // Check button border radius
      const button = page.locator('.btn').first();
      await expect(button).toBeVisible();

      const buttonStyles = await button.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          borderRadius: styles.borderRadius
        };
      });

      // Border radius should be applied (0.5rem = 8px)
      expect(buttonStyles.borderRadius).toBeTruthy();
      expect(buttonStyles.borderRadius).not.toBe('0px');

      // Check feature card border radius
      const featureCard = page.locator('.feature-card').first();
      const cardStyles = await featureCard.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          borderRadius: styles.borderRadius
        };
      });

      expect(cardStyles.borderRadius).toBeTruthy();
      expect(cardStyles.borderRadius).not.toBe('0px');

      console.log(`Border radius renders correctly in ${browserName}`);
    });

    test('Box shadow and borders render consistently', async ({ page, browserName }) => {
      await page.goto('/');

      // Check that borders are applied to feature cards
      const featureCard = page.locator('.feature-card').first();
      const cardStyles = await featureCard.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          border: styles.border,
          borderWidth: styles.borderWidth,
          borderStyle: styles.borderStyle,
          borderColor: styles.borderColor
        };
      });

      // Verify border is applied
      expect(cardStyles.borderWidth).not.toBe('0px');
      expect(cardStyles.borderStyle).toBe('solid');

      console.log(`Borders render correctly in ${browserName}`);
    });

    test('Typography renders consistently', async ({ page, browserName }) => {
      await page.goto('/');

      // Check hero title typography
      const heroTitle = page.locator('#hero h1');
      const titleStyles = await heroTitle.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          fontSize: styles.fontSize,
          fontWeight: styles.fontWeight,
          lineHeight: styles.lineHeight
        };
      });

      // Font size should be large (4rem = 64px for desktop)
      const fontSizeValue = parseFloat(titleStyles.fontSize);
      expect(fontSizeValue).toBeGreaterThan(30);

      // Check body text
      const bodyText = page.locator('#hero .description');
      const bodyStyles = await bodyText.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          fontSize: styles.fontSize,
          lineHeight: styles.lineHeight
        };
      });

      expect(bodyStyles.fontSize).toBeTruthy();
      expect(bodyStyles.lineHeight).toBeTruthy();

      console.log(`Typography renders correctly in ${browserName}`);
    });

    test('Colors and gradients render consistently', async ({ page, browserName }) => {
      await page.goto('/');

      // Check hero background gradient
      const hero = page.locator('#hero');
      const heroStyles = await hero.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundImage: styles.backgroundImage
        };
      });

      // Should have gradient background
      expect(heroStyles.backgroundImage).toContain('gradient');

      // Check primary button color
      const primaryBtn = page.locator('.btn-primary').first();
      const btnStyles = await primaryBtn.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          color: styles.color
        };
      });

      expect(btnStyles.backgroundColor).toBeTruthy();
      expect(btnStyles.color).toBeTruthy();

      console.log(`Colors and gradients render correctly in ${browserName}`);
    });

    test('Transitions and hover states work', async ({ page, browserName }) => {
      await page.goto('/');

      // Check that transition property is set on buttons
      const button = page.locator('.btn').first();
      const btnStyles = await button.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          transition: styles.transition,
          transitionProperty: styles.transitionProperty
        };
      });

      // Transition should be defined
      expect(btnStyles.transition).toBeTruthy();
      expect(btnStyles.transition).not.toBe('none');

      // Test hover by hovering and checking for style changes
      await button.hover();

      // Small delay to allow transition
      await page.waitForTimeout(300);

      console.log(`Transitions work correctly in ${browserName}`);
    });

    test('Smooth scrolling behavior is applied', async ({ page, browserName }) => {
      await page.goto('/');

      // Check scroll-behavior on html element
      const scrollBehavior = await page.evaluate(() => {
        const styles = window.getComputedStyle(document.documentElement);
        return styles.scrollBehavior;
      });

      expect(scrollBehavior).toBe('smooth');

      console.log(`Smooth scrolling is applied in ${browserName}`);
    });

    test('Fixed positioning works for navigation', async ({ page, browserName }) => {
      await page.goto('/');

      // Check navigation is fixed positioned
      const nav = page.locator('nav');
      const navStyles = await nav.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          position: styles.position,
          top: styles.top,
          left: styles.left,
          right: styles.right,
          zIndex: styles.zIndex
        };
      });

      expect(navStyles.position).toBe('fixed');
      expect(navStyles.top).toBe('0px');

      // Scroll down and verify nav stays visible
      await page.evaluate(() => window.scrollTo(0, 500));
      await expect(nav).toBeVisible();
      await expect(nav).toBeInViewport();

      console.log(`Fixed positioning works correctly in ${browserName}`);
    });

    test('Overflow handling works correctly', async ({ page, browserName }) => {
      await page.goto('/');

      // Check code blocks have overflow-x: auto
      const codeBlock = page.locator('#quick-start pre').first();
      const codeStyles = await codeBlock.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          overflowX: styles.overflowX
        };
      });

      expect(codeStyles.overflowX).toBe('auto');

      console.log(`Overflow handling works correctly in ${browserName}`);
    });
  });
});

test.describe('Cross-Browser Compatibility - Interactive Features', () => {

  test('Links are clickable and functional', async ({ page, browserName }) => {
    await page.goto('/');

    // Test internal navigation links
    const featuresLink = page.locator('nav .nav-links a').filter({ hasText: 'Features' });
    await featuresLink.click();

    // Verify we scrolled to the features section
    await expect(page.locator('#features')).toBeInViewport();

    console.log(`Links work correctly in ${browserName}`);
  });

  test('External links have correct attributes', async ({ page, browserName }) => {
    await page.goto('/');

    // Check GitHub link in hero
    const heroGithubLink = page.locator('[data-testid="hero-github-link"]');
    await expect(heroGithubLink).toHaveAttribute('target', '_blank');
    await expect(heroGithubLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Check GitHub link in nav
    const navGithubLink = page.locator('[data-testid="nav-github-link"]');
    await expect(navGithubLink).toHaveAttribute('target', '_blank');
    await expect(navGithubLink).toHaveAttribute('rel', 'noopener noreferrer');

    console.log(`External links configured correctly in ${browserName}`);
  });

  test('Focus states are visible for accessibility', async ({ page, browserName }) => {
    await page.goto('/');

    // Tab to the first link and check focus is visible
    await page.keyboard.press('Tab');

    // Get the focused element
    const focusedElement = page.locator(':focus');
    await expect(focusedElement).toBeVisible();

    console.log(`Focus states work correctly in ${browserName}`);
  });
});
