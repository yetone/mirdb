/**
 * User Interaction Patterns E2E Tests
 * Owner: Scenario 16 - User Interaction Patterns
 *
 * Test cases:
 * - CTA button hover states (color, shadow, transform)
 * - Navigation link hover states
 * - Footer link hover states
 * - Progressive disclosure (expand button for detailed info)
 * - Smooth scroll on section links
 */

const { test, expect } = require('@playwright/test');

test.describe('User Interaction Patterns', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test.describe('TC1: CTA Button Hover States', () => {
    test('primary CTA button should have visual hover state change', async ({ page }) => {
      const primaryCta = page.locator('[data-testid="cta-try-demo"]');
      await expect(primaryCta).toBeVisible();

      // Get initial styles
      const initialStyles = await primaryCta.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          transform: styles.transform,
          boxShadow: styles.boxShadow
        };
      });

      // Hover over the button
      await primaryCta.hover();

      // Wait for transition
      await page.waitForTimeout(300);

      // Get hover styles
      const hoverStyles = await primaryCta.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          transform: styles.transform,
          boxShadow: styles.boxShadow
        };
      });

      // Verify visual changes occurred
      // At least one of: color change, transform change, or shadow change should occur
      const hasVisualChange =
        hoverStyles.backgroundColor !== initialStyles.backgroundColor ||
        hoverStyles.transform !== initialStyles.transform ||
        hoverStyles.boxShadow !== initialStyles.boxShadow;

      expect(hasVisualChange).toBe(true);
    });

    test('secondary CTA button should have visual hover state change', async ({ page }) => {
      const secondaryCta = page.locator('[data-testid="cta-get-started"]');
      await expect(secondaryCta).toBeVisible();

      // Get initial styles
      const initialStyles = await secondaryCta.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          borderColor: styles.borderColor,
          transform: styles.transform
        };
      });

      // Hover over the button
      await secondaryCta.hover();

      // Wait for transition
      await page.waitForTimeout(300);

      // Get hover styles
      const hoverStyles = await secondaryCta.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          borderColor: styles.borderColor,
          transform: styles.transform
        };
      });

      // Verify visual changes occurred
      const hasVisualChange =
        hoverStyles.backgroundColor !== initialStyles.backgroundColor ||
        hoverStyles.borderColor !== initialStyles.borderColor ||
        hoverStyles.transform !== initialStyles.transform;

      expect(hasVisualChange).toBe(true);
    });
  });

  test.describe('TC2: Navigation Link Hover States', () => {
    test('navigation links should have visual hover state indication', async ({ page }) => {
      const navLink = page.locator('[data-testid="nav-features"]');
      await expect(navLink).toBeVisible();

      // Get initial styles
      const initialColor = await navLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Hover over the link
      await navLink.hover();

      // Wait for transition
      await page.waitForTimeout(200);

      // Get hover color
      const hoverColor = await navLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Verify color change occurred (hover state)
      expect(hoverColor).not.toBe(initialColor);
    });

    test('GitHub nav link should have hover state', async ({ page }) => {
      const githubLink = page.locator('[data-testid="nav-github"]');
      await expect(githubLink).toBeVisible();

      // Get initial styles
      const initialStyles = await githubLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          color: styles.color
        };
      });

      // Hover over the button
      await githubLink.hover();

      // Wait for transition
      await page.waitForTimeout(200);

      // Get hover styles
      const hoverStyles = await githubLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          color: styles.color
        };
      });

      // Verify background color change on hover
      expect(hoverStyles.backgroundColor).not.toBe(initialStyles.backgroundColor);
    });
  });

  test.describe('TC3: Footer Link Hover States', () => {
    test('footer links should have visual hover state indication', async ({ page }) => {
      // Scroll to footer to ensure it's visible
      await page.locator('#footer').scrollIntoViewIfNeeded();

      const footerLink = page.locator('[data-testid="footer-github"]');
      await expect(footerLink).toBeVisible();

      // Get initial color
      const initialColor = await footerLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Hover over the link
      await footerLink.hover();

      // Wait for transition
      await page.waitForTimeout(200);

      // Get hover color
      const hoverColor = await footerLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Verify color change occurred
      expect(hoverColor).not.toBe(initialColor);
    });

    test('footer social link should have hover state', async ({ page }) => {
      // Scroll to footer
      await page.locator('#footer').scrollIntoViewIfNeeded();

      const socialLink = page.locator('[data-testid="footer-github-icon"]');
      await expect(socialLink).toBeVisible();

      // Get initial styles
      const initialStyles = await socialLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          color: styles.color
        };
      });

      // Hover over the link
      await socialLink.hover();

      // Wait for transition
      await page.waitForTimeout(200);

      // Get hover styles
      const hoverStyles = await socialLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          color: styles.color
        };
      });

      // Verify visual change on hover
      const hasVisualChange =
        hoverStyles.backgroundColor !== initialStyles.backgroundColor ||
        hoverStyles.color !== initialStyles.color;

      expect(hasVisualChange).toBe(true);
    });

    test('footer license link should have hover state', async ({ page }) => {
      await page.locator('#footer').scrollIntoViewIfNeeded();

      const licenseLink = page.locator('[data-testid="footer-license-link"]');
      await expect(licenseLink).toBeVisible();

      // Get initial color
      const initialColor = await licenseLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Hover over the link
      await licenseLink.hover();

      // Wait for transition
      await page.waitForTimeout(200);

      // Get hover color
      const hoverColor = await licenseLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Verify color change occurred
      expect(hoverColor).not.toBe(initialColor);
    });
  });

  test.describe('TC4: Progressive Disclosure', () => {
    test('expand button should reveal additional technical details', async ({ page }) => {
      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      // Find the expand button for LSM tree
      const expandBtn = page.locator('[data-testid="expand-btn-lsm"]');
      await expect(expandBtn).toBeVisible();

      // Get the details section
      const detailsSection = page.locator('[data-testid="lsm-details"]');

      // Initially, details should be hidden
      await expect(detailsSection).toHaveAttribute('aria-hidden', 'true');
      await expect(expandBtn).toHaveAttribute('aria-expanded', 'false');

      // Click the expand button
      await expandBtn.click();

      // Wait for animation
      await page.waitForTimeout(400);

      // Details should now be visible
      await expect(detailsSection).toHaveAttribute('aria-hidden', 'false');
      await expect(expandBtn).toHaveAttribute('aria-expanded', 'true');

      // Verify technical content is revealed
      const technicalContent = detailsSection.locator('text=Write Path');
      await expect(technicalContent).toBeVisible();
    });

    test('clicking expand button again should collapse details', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const expandBtn = page.locator('[data-testid="expand-btn-lsm"]');
      const detailsSection = page.locator('[data-testid="lsm-details"]');

      // Expand first
      await expandBtn.click();
      await page.waitForTimeout(400);
      await expect(detailsSection).toHaveAttribute('aria-hidden', 'false');

      // Collapse
      await expandBtn.click();
      await page.waitForTimeout(400);

      // Should be hidden again
      await expect(detailsSection).toHaveAttribute('aria-hidden', 'true');
      await expect(expandBtn).toHaveAttribute('aria-expanded', 'false');
    });

    test('memcached expand button should work correctly', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const expandBtn = page.locator('[data-testid="expand-btn-memcached"]');
      const detailsSection = page.locator('[data-testid="memcached-details"]');

      await expect(expandBtn).toBeVisible();
      await expect(detailsSection).toHaveAttribute('aria-hidden', 'true');

      // Click to expand
      await expandBtn.click();
      await page.waitForTimeout(400);

      await expect(detailsSection).toHaveAttribute('aria-hidden', 'false');

      // Verify supported commands content is visible
      const supportedCommands = detailsSection.locator('text=Supported Commands');
      await expect(supportedCommands).toBeVisible();
    });

    test('expand button should have proper hover state', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const expandBtn = page.locator('[data-testid="expand-btn-lsm"]');
      await expect(expandBtn).toBeVisible();

      // Get initial styles
      const initialStyles = await expandBtn.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          color: styles.color
        };
      });

      // Hover over the button
      await expandBtn.hover();
      await page.waitForTimeout(200);

      // Get hover styles
      const hoverStyles = await expandBtn.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          color: styles.color
        };
      });

      // Verify visual change on hover
      const hasVisualChange =
        hoverStyles.backgroundColor !== initialStyles.backgroundColor ||
        hoverStyles.color !== initialStyles.color;

      expect(hasVisualChange).toBe(true);
    });
  });

  test.describe('TC5: Smooth Scroll on Section Links', () => {
    test('clicking navigation link should smoothly animate to target section', async ({ page }) => {
      // Start at the top of the page
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(200);

      const initialScrollY = await page.evaluate(() => window.scrollY);
      expect(initialScrollY).toBeLessThan(100);

      // Click on Features nav link
      const featuresLink = page.locator('[data-testid="nav-features"]');
      await featuresLink.click();

      // Wait for smooth scroll animation (CSS smooth scroll + JS animation)
      await page.waitForTimeout(1000);

      // Get final scroll position
      const finalScrollY = await page.evaluate(() => window.scrollY);

      // Should have scrolled down to features section
      expect(finalScrollY).toBeGreaterThan(initialScrollY);

      // Features section should be in view
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('clicking demo link should scroll to demo section', async ({ page }) => {
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(200);

      const demoLink = page.locator('[data-testid="nav-demo"]');
      await demoLink.click();

      await page.waitForTimeout(1000);

      const demoSection = page.locator('#demo');
      await expect(demoSection).toBeInViewport();
    });

    test('clicking quickstart link should scroll to quickstart section', async ({ page }) => {
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(200);

      const quickstartLink = page.locator('[data-testid="nav-quickstart"]');
      await quickstartLink.click();

      await page.waitForTimeout(1000);

      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport();
    });

    test('hero CTA buttons should scroll to target sections', async ({ page }) => {
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(200);

      // Click Try Demo button
      const tryDemoBtn = page.locator('[data-testid="cta-try-demo"]');
      await tryDemoBtn.click();

      await page.waitForTimeout(1000);

      // Demo section should be in view
      const demoSection = page.locator('#demo');
      await expect(demoSection).toBeInViewport();
    });

    test('footer section links should scroll to sections', async ({ page }) => {
      // Scroll to footer
      await page.locator('#footer').scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);

      // Click on Features link in footer
      const footerFeaturesLink = page.locator('.footer__links-list a[href="#features"]');
      await footerFeaturesLink.click();

      await page.waitForTimeout(1000);

      // Features section should be in view
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });
  });

  test.describe('Feature Card Hover States', () => {
    test('feature cards should have hover transform effect', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCard = page.locator('[data-testid="feature-memcached"]');
      await expect(featureCard).toBeVisible();

      // Get initial transform
      const initialTransform = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Hover over the card
      await featureCard.hover();
      await page.waitForTimeout(300);

      // Get hover transform
      const hoverTransform = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Verify transform change (translateY effect)
      expect(hoverTransform).not.toBe(initialTransform);
    });

    test('feature cards should have hover shadow effect', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCard = page.locator('[data-testid="feature-sstables"]');
      await expect(featureCard).toBeVisible();

      // Get initial shadow
      const initialShadow = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).boxShadow;
      });

      // Hover over the card
      await featureCard.hover();
      await page.waitForTimeout(300);

      // Get hover shadow
      const hoverShadow = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).boxShadow;
      });

      // Verify shadow change
      expect(hoverShadow).not.toBe(initialShadow);
    });
  });
});
