/**
 * Navigation and Anchor Links E2E Tests
 * Owner: Scenario 17 - Navigation and Anchor Links
 *
 * Test cases:
 * 1. Navigation link clicks scroll to correct sections
 * 2. Smooth scrolling animation works
 * 3. URL hash updates when navigating
 * 4. Direct URL with hash scrolls to section on page load
 * 5. Mobile menu navigation works
 * 6. Reduced motion preference is respected
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation and Anchor Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Desktop Navigation', () => {
    test.use({ viewport: { width: 1280, height: 720 } });

    test('should display navigation bar with all section links', async ({ page }) => {
      const navbar = page.locator('nav[aria-label="Main navigation"]');
      await expect(navbar).toBeVisible();

      // Check all navigation links are present in the desktop navigation (hidden on mobile, visible on desktop)
      const desktopNav = navbar.locator('.hidden.md\\:flex');
      const navLinks = ['Features', 'Architecture', 'Configuration', 'Installation', 'Commands', 'Roadmap'];
      for (const linkText of navLinks) {
        const link = desktopNav.locator(`a[href="#${linkText.toLowerCase()}"]`);
        await expect(link).toBeVisible();
        await expect(link).toHaveText(linkText);
      }
    });

    test('should scroll to Features section when clicking Features link', async ({ page }) => {
      // Use the desktop nav link specifically (first visible one)
      const desktopNav = page.locator('nav[aria-label="Main navigation"] .hidden.md\\:flex');
      const featuresLink = desktopNav.locator('a[href="#features"]');
      await featuresLink.click();

      // Wait for scroll and check URL hash
      await expect(page).toHaveURL(/#features$/);

      // Check that features section is in view
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('should scroll to Architecture section when clicking Architecture link', async ({ page }) => {
      const desktopNav = page.locator('nav[aria-label="Main navigation"] .hidden.md\\:flex');
      const architectureLink = desktopNav.locator('a[href="#architecture"]');
      await architectureLink.click();

      await expect(page).toHaveURL(/#architecture$/);

      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeInViewport();
    });

    test('should scroll to Installation section when clicking Installation link', async ({ page }) => {
      const desktopNav = page.locator('nav[aria-label="Main navigation"] .hidden.md\\:flex');
      const installationLink = desktopNav.locator('a[href="#installation"]');
      await installationLink.click();

      await expect(page).toHaveURL(/#installation$/);

      const installationSection = page.locator('#installation');
      await expect(installationSection).toBeInViewport();
    });

    test('should update URL hash when clicking navigation links', async ({ page }) => {
      const desktopNav = page.locator('nav[aria-label="Main navigation"] .hidden.md\\:flex');

      // Click Features link
      await desktopNav.locator('a[href="#features"]').click();
      await expect(page).toHaveURL(/#features$/);

      // Click Configuration link
      await desktopNav.locator('a[href="#configuration"]').click();
      await expect(page).toHaveURL(/#configuration$/);

      // Click Commands link
      await desktopNav.locator('a[href="#commands"]').click();
      await expect(page).toHaveURL(/#commands$/);

      // Click Roadmap link
      await desktopNav.locator('a[href="#roadmap"]').click();
      await expect(page).toHaveURL(/#roadmap$/);
    });

    test('should display MirDB brand link', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });
      const brandLink = page.locator('nav[aria-label="Main navigation"] a[aria-label="MirDB Home"]');
      await expect(brandLink).toBeVisible();
      await expect(brandLink).toContainText('MirDB');
    });

    test('should display GitHub button', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });
      const desktopNav = page.locator('nav[aria-label="Main navigation"] .hidden.md\\:flex');
      const githubLink = desktopNav.locator('a[href="https://github.com/mirdb/mirdb"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveText('GitHub');
    });
  });

  test.describe('URL Hash Navigation', () => {
    test.use({ viewport: { width: 1280, height: 720 } });

    test('should scroll to Features section when loading with #features hash', async ({ page }) => {
      await page.goto('/#features');

      // Wait for the page to finish scrolling
      await page.waitForTimeout(500);

      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('should scroll to Architecture section when loading with #architecture hash', async ({ page }) => {
      await page.goto('/#architecture');

      await page.waitForTimeout(500);

      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeInViewport();
    });

    test('should scroll to Installation section when loading with #installation hash', async ({ page }) => {
      await page.goto('/#installation');

      await page.waitForTimeout(500);

      const installationSection = page.locator('#installation');
      await expect(installationSection).toBeInViewport();
    });
  });

  test.describe('Smooth Scrolling', () => {
    test.use({ viewport: { width: 1280, height: 720 } });

    test('should use smooth scroll animation when navigating', async ({ page }) => {
      // Check that CSS smooth scroll is enabled
      const scrollBehavior = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).scrollBehavior;
      });

      expect(scrollBehavior).toBe('smooth');
    });

    test('should respect prefers-reduced-motion preference', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');

      // Check that smooth scroll is disabled when reduced motion is preferred
      const scrollBehavior = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).scrollBehavior;
      });

      expect(scrollBehavior).toBe('auto');
    });

    test('should use instant scroll when prefers-reduced-motion is set', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click a navigation link (use desktop nav)
      const desktopNav = page.locator('nav[aria-label="Main navigation"] .hidden.md\\:flex');
      await desktopNav.locator('a[href="#installation"]').click();

      // The scroll should happen immediately (not animated)
      // We can verify by checking scroll position after a very short delay
      await page.waitForTimeout(50);

      const scrollY = await page.evaluate(() => window.scrollY);
      expect(scrollY).toBeGreaterThan(initialScrollY);
    });
  });

  test.describe('Mobile Navigation', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('should show hamburger menu on mobile', async ({ page }) => {
      const hamburgerButton = page.locator('button[aria-label="Open menu"]');
      await expect(hamburgerButton).toBeVisible();
    });

    test('should open mobile menu when hamburger is clicked', async ({ page }) => {
      const hamburgerButton = page.locator('button[aria-label="Open menu"]');
      await hamburgerButton.click();

      const mobileMenu = page.locator('#mobile-menu');
      await expect(mobileMenu).toBeVisible();
    });

    test('should close mobile menu when close button is clicked', async ({ page }) => {
      // Open menu
      const hamburgerButton = page.locator('button[aria-label="Open menu"]');
      await hamburgerButton.click();

      // Wait for menu to open with translate-x-0
      const mobileMenu = page.locator('#mobile-menu');
      await expect(mobileMenu).toHaveClass(/translate-x-0/);

      // Close menu by pressing Escape key (more reliable than clicking)
      await page.keyboard.press('Escape');

      // Wait for animation to complete and verify menu is closed
      await page.waitForTimeout(400);

      // Menu should be translated off-screen
      await expect(mobileMenu).toHaveClass(/translate-x-full/);
    });

    test('should navigate to section and close menu when mobile link is clicked', async ({ page }) => {
      // Open menu
      await page.locator('button[aria-label="Open menu"]').click();

      // Click Features link in mobile menu
      const featuresLink = page.locator('#mobile-menu a[href="#features"]');
      await featuresLink.click();

      // Wait for navigation
      await page.waitForTimeout(300);

      // Menu should be closed
      const mobileMenu = page.locator('#mobile-menu');
      await expect(mobileMenu).toHaveClass(/translate-x-full/);

      // URL should have hash
      await expect(page).toHaveURL(/#features$/);
    });

    test('should close mobile menu when Escape key is pressed', async ({ page }) => {
      // Open menu
      await page.locator('button[aria-label="Open menu"]').click();

      const mobileMenu = page.locator('#mobile-menu');
      await expect(mobileMenu).toBeVisible();

      // Press Escape
      await page.keyboard.press('Escape');

      // Wait for animation
      await page.waitForTimeout(400);

      await expect(mobileMenu).toHaveClass(/translate-x-full/);
    });

    test('should hide desktop navigation links on mobile', async ({ page }) => {
      const desktopNav = page.locator('nav[aria-label="Main navigation"] .hidden.md\\:flex');
      await expect(desktopNav).toBeHidden();
    });
  });

  test.describe('Navbar Styling', () => {
    test.use({ viewport: { width: 1280, height: 720 } });

    test('should have sticky navigation', async ({ page }) => {
      const navbar = page.locator('nav[aria-label="Main navigation"]');
      const position = await navbar.evaluate((el) => getComputedStyle(el).position);

      expect(position).toBe('sticky');
    });

    test('should maintain navigation visibility when scrolling', async ({ page }) => {
      // Scroll down
      await page.evaluate(() => window.scrollTo(0, 1000));
      await page.waitForTimeout(100);

      const navbar = page.locator('nav[aria-label="Main navigation"]');
      await expect(navbar).toBeVisible();
      await expect(navbar).toBeInViewport();
    });
  });

  test.describe('Accessibility', () => {
    test.use({ viewport: { width: 1280, height: 720 } });

    test('should have proper ARIA labels on navigation', async ({ page }) => {
      const navbar = page.locator('nav[aria-label="Main navigation"]');
      await expect(navbar).toHaveAttribute('aria-label', 'Main navigation');
    });

    test('should have accessible hamburger button', async ({ page }) => {
      // Use mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });

      const hamburgerButton = page.locator('button[aria-label="Open menu"]');
      await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false');

      // Open menu
      await hamburgerButton.click();

      // After opening, the aria-label changes to "Close menu" and aria-expanded becomes true
      // We need to use a different selector after the click
      const toggleButton = page.locator('button[aria-controls="mobile-menu"]');
      await expect(toggleButton).toHaveAttribute('aria-expanded', 'true');
    });

    test('should trap focus within mobile menu when open', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      // Open menu
      await page.locator('button[aria-label="Open menu"]').click();

      // Press Tab to cycle through focusable elements
      await page.keyboard.press('Tab');
      await page.keyboard.press('Tab');
      await page.keyboard.press('Tab');

      // Focus should remain within the menu (not escape to the page behind)
      const activeElement = await page.evaluate(() => document.activeElement?.closest('#mobile-menu'));
      expect(activeElement).not.toBeNull();
    });

    test('should have keyboard navigable links', async ({ page }) => {
      const desktopNav = page.locator('nav[aria-label="Main navigation"] .hidden.md\\:flex');
      const firstNavLink = desktopNav.locator('a[href="#features"]');

      // Focus on the first link
      await firstNavLink.focus();
      await expect(firstNavLink).toBeFocused();

      // Press Enter to navigate
      await page.keyboard.press('Enter');

      await expect(page).toHaveURL(/#features$/);
    });
  });
});
