const { test, expect } = require('@playwright/test');

test('navigation contains expected menu items', async ({ page }) => {
  await page.goto('http://localhost:3000');

  const navigationItems = ['About', 'Features', 'Getting Started', 'Documentation'];

  for (const item of navigationItems) {
    const navLink = page.locator('nav a', { hasText: item });
    await expect(navLink).toBeVisible();
    await expect(navLink).toHaveAttribute('href', `#${item.replace(/\s+/g, '-').toLowerCase()}`);
  }
});

test('clicking GitHub link opens repository', async ({ page }) => {
  await page.goto('http://localhost:3000');

  const [newPage] = await Promise.all([
    page.context().waitForEvent('page'),
    page.click('a[href="https://github.com/yetone/mirdb"]')
  ]);

  await newPage.waitForLoadState();
  expect(newPage.url()).toContain('github.com/yetone/mirdb');
});

test('mobile navigation menu is accessible', async ({ page, isMobile }) => {
  await page.goto('http://localhost:3000');

  if (isMobile) {
    // Mobile viewport test
    await page.click('button[aria-label="Toggle menu"]');
    const mobileNav = page.locator('nav[aria-hidden="false"]');
    await expect(mobileNav).toBeVisible();

    const navigationItems = ['About', 'Features', 'Getting Started', 'Documentation'];
    for (const item of navigationItems) {
      await expect(page.locator('nav a', { hasText: item })).toBeVisible();
    }
  } else {
    // Desktop shouldn't show mobile menu by default
    await expect(page.locator('nav.md\\:hidden')).not.toBeVisible();
  }
});