const { test, expect } = require('@playwright/test');

test('Usage section contains visual demonstrations', async ({ page }) => {
  await page.goto('http://localhost:5000');

  // Verify memcached protocol GIF exists with correct alt text
  const memcachedGif = page.locator('img[alt="Demonstration of memcached protocol commands in MirDB showing SET and GET operations"]');
  await expect(memcachedGif).toHaveAttribute('src', '/assets/images/usage/memcached-protocol.gif');
  await expect(memcachedGif).toBeVisible();

  // Verify persistence demo GIF exists with correct alt text
  const persistenceGif = page.locator('img[alt="Demonstration of MirDB\'s persistence capabilities showing data retention after restart"]');
  await expect(persistenceGif).toHaveAttribute('src', '/assets/images/usage/persistence-demo.gif');
  await expect(persistenceGif).toBeVisible();

  // Verify both GIFs have appropriate dimensions
  await expect(memcachedGif).toHaveCSS('width', '800px');
  await expect(memcachedGif).toHaveCSS('height', '450px');
  await expect(persistenceGif).toHaveCSS('width', '800px');
  await expect(persistenceGif).toHaveCSS('height', '450px');
});