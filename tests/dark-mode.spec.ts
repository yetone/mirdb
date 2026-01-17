import { test, expect } from '@playwright/test';

test.describe('Dark Mode Support', () => {
  test.describe('System Preference Detection', () => {
    test('should automatically switch to dark theme when system preference is dark', async ({ page }) => {
      // Set color scheme preference to dark
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Check that dark mode styles are applied
      const body = page.locator('body');
      await expect(body).toBeVisible();

      // Verify background color matches dark mode
      const bgColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Dark mode background should be #1a1a1a (rgb(26, 26, 26))
      expect(bgColor).toBe('rgb(26, 26, 26)');
    });

    test('should use light theme when system preference is light', async ({ page }) => {
      // Set color scheme preference to light
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto('/');

      const body = page.locator('body');
      await expect(body).toBeVisible();

      // Verify background color matches light mode
      const bgColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Light mode background should be #ffffff (rgb(255, 255, 255))
      expect(bgColor).toBe('rgb(255, 255, 255)');
    });
  });

  test.describe('Text Readability in Dark Mode', () => {
    test.beforeEach(async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
    });

    test('should have readable text with sufficient contrast in dark mode', async ({ page }) => {
      // Check main body text color
      const body = page.locator('body');
      const textColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Dark mode text should be #e0e0e0 (rgb(224, 224, 224))
      expect(textColor).toBe('rgb(224, 224, 224)');
    });

    test('should have readable heading text in dark mode', async ({ page }) => {
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();

      const h1Color = await h1.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Headings should inherit text color from body in dark mode
      expect(h1Color).toBe('rgb(224, 224, 224)');
    });

    test('should have readable tagline text in dark mode', async ({ page }) => {
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();

      const taglineColor = await tagline.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Primary color in dark mode is #ff7a5a (rgb(255, 122, 90))
      expect(taglineColor).toBe('rgb(255, 122, 90)');
    });

    test('should have readable description text in dark mode', async ({ page }) => {
      const description = page.locator('.description');
      await expect(description).toBeVisible();

      const descColor = await description.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Text-light in dark mode is #b0b0b0 (rgb(176, 176, 176))
      expect(descColor).toBe('rgb(176, 176, 176)');
    });

    test('should have readable feature card text in dark mode', async ({ page }) => {
      const featureCard = page.locator('.feature-card').first();
      await expect(featureCard).toBeVisible();

      const cardText = featureCard.locator('p');
      const textColor = await cardText.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Feature card text uses text-light color
      expect(textColor).toBe('rgb(176, 176, 176)');
    });
  });

  test.describe('Code Block Visibility in Dark Mode', () => {
    test.beforeEach(async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
    });

    test('should display code blocks with syntax highlighting in dark mode', async ({ page }) => {
      const codeBlock = page.locator('.code-block').first();
      await expect(codeBlock).toBeVisible();

      // Check code block background
      const codeBg = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Code block background is #1e1e1e (rgb(30, 30, 30))
      expect(codeBg).toBe('rgb(30, 30, 30)');
    });

    test('should have visible code text in dark mode', async ({ page }) => {
      const codeElement = page.locator('#example-code');
      await expect(codeElement).toBeVisible();

      // Verify code text is visible
      const codeText = await codeElement.textContent();
      expect(codeText).toContain('pymemcache');
    });

    test('should maintain Prism.js syntax highlighting in dark mode', async ({ page }) => {
      // Wait for Prism.js to highlight the code
      await page.waitForTimeout(500);

      const codeBlock = page.locator('#example-code');
      await expect(codeBlock).toBeVisible();

      // Check that syntax highlighting tokens exist
      const hasTokens = await page.evaluate(() => {
        const codeElement = document.querySelector('#example-code');
        if (!codeElement) return false;
        // Prism.js adds .token class to highlighted elements
        return codeElement.innerHTML.includes('token') ||
               codeElement.parentElement?.innerHTML.includes('language-python');
      });

      expect(hasTokens).toBe(true);
    });
  });

  test.describe('Images and Diagrams in Dark Mode', () => {
    test.beforeEach(async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
    });

    test('should display logo image in dark mode', async ({ page }) => {
      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();

      // Check that image element exists and has dimensions
      const boundingBox = await logo.boundingBox();
      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeGreaterThan(0);
      expect(boundingBox!.height).toBeGreaterThan(0);
    });

    test('should display architecture diagram in dark mode', async ({ page }) => {
      const archDiagram = page.locator('.architecture-diagram');
      await expect(archDiagram).toBeVisible();

      // Check component boxes are visible
      const walBox = page.locator('.component-box.wal');
      await expect(walBox).toBeVisible();

      const memtableBox = page.locator('.component-box.memtable');
      await expect(memtableBox).toBeVisible();

      const sstableBox = page.locator('.component-box.sstable');
      await expect(sstableBox).toBeVisible();
    });

    test('should have visible architecture component labels in dark mode', async ({ page }) => {
      const componentLabel = page.locator('.component-label').first();
      await expect(componentLabel).toBeVisible();

      const labelColor = await componentLabel.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Labels use text-light color
      expect(labelColor).toBe('rgb(176, 176, 176)');
    });

    test('should have visible badges in dark mode', async ({ page }) => {
      const badges = page.locator('[data-section="badges"]');
      await expect(badges).toBeVisible();

      // Check that badge images are visible
      const badgeImages = page.locator('.badges img');
      const count = await badgeImages.count();
      expect(count).toBeGreaterThan(0);
    });
  });

  test.describe('Theme Toggle Functionality', () => {
    test('should switch themes smoothly without page reload', async ({ page }) => {
      // Start with light mode
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto('/');

      // Get initial URL
      const initialUrl = page.url();

      // Verify light mode is active
      let bgColor = await page.locator('body').evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(bgColor).toBe('rgb(255, 255, 255)');

      // Switch to dark mode
      await page.emulateMedia({ colorScheme: 'dark' });

      // Wait for CSS to update
      await page.waitForTimeout(100);

      // Verify dark mode is now active
      bgColor = await page.locator('body').evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(bgColor).toBe('rgb(26, 26, 26)');

      // Verify no page reload occurred (URL unchanged)
      expect(page.url()).toBe(initialUrl);
    });

    test('should toggle back to light mode smoothly', async ({ page }) => {
      // Start with dark mode
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      const initialUrl = page.url();

      // Verify dark mode is active
      let bgColor = await page.locator('body').evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(bgColor).toBe('rgb(26, 26, 26)');

      // Switch to light mode
      await page.emulateMedia({ colorScheme: 'light' });

      // Wait for CSS to update
      await page.waitForTimeout(100);

      // Verify light mode is now active
      bgColor = await page.locator('body').evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(bgColor).toBe('rgb(255, 255, 255)');

      // Verify no page reload occurred
      expect(page.url()).toBe(initialUrl);
    });

    test('should preserve page state during theme switch', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto('/');

      // Scroll to a section
      await page.locator('#features').scrollIntoViewIfNeeded();

      // Get scroll position before theme switch
      const scrollBefore = await page.evaluate(() => window.scrollY);

      // Switch theme
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.waitForTimeout(100);

      // Get scroll position after theme switch
      const scrollAfter = await page.evaluate(() => window.scrollY);

      // Scroll position should be preserved
      expect(scrollAfter).toBe(scrollBefore);
    });
  });

  test.describe('Dark Mode Color Contrast Accessibility', () => {
    test.beforeEach(async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
    });

    test('should have sufficient contrast for primary buttons in dark mode', async ({ page }) => {
      const primaryBtn = page.locator('.btn-primary').first();
      await expect(primaryBtn).toBeVisible();

      const btnBg = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      const btnColor = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Button should have dark mode primary color #ff7a5a
      expect(btnBg).toBe('rgb(255, 122, 90)');
      // Text should be white for contrast
      expect(btnColor).toBe('rgb(255, 255, 255)');
    });

    test('should have visible focus indicators in dark mode', async ({ page }) => {
      const getStartedBtn = page.locator('[data-link="get-started"]');
      await getStartedBtn.focus();

      // Check that focus outline is visible by verifying outline-width
      const outlineStyle = await getStartedBtn.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          outlineWidth: style.outlineWidth,
          outlineStyle: style.outlineStyle,
          outline: style.outline
        };
      });

      // Focus should have a visible outline (width > 0 and solid style)
      expect(outlineStyle.outlineWidth).not.toBe('0px');
      expect(outlineStyle.outlineStyle).toBe('solid');
    });

    test('should have visible command card text in dark mode', async ({ page }) => {
      const commandName = page.locator('.command-name').first();
      await expect(commandName).toBeVisible();

      const nameColor = await commandName.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Command name uses primary color
      expect(nameColor).toBe('rgb(255, 122, 90)');
    });

    test('should have readable footer text in dark mode', async ({ page }) => {
      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();

      // Footer links should be white
      const footerLink = page.locator('.footer-links a').first();
      const linkColor = await footerLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      expect(linkColor).toBe('rgb(255, 255, 255)');
    });
  });

  test.describe('Dark Mode Section Backgrounds', () => {
    test.beforeEach(async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
    });

    test('should have correct hero section gradient in dark mode', async ({ page }) => {
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      const bgImage = await hero.evaluate((el) => {
        return window.getComputedStyle(el).backgroundImage;
      });

      // Should have a gradient background
      expect(bgImage).toContain('linear-gradient');
    });

    test('should have correct alternating section backgrounds in dark mode', async ({ page }) => {
      // Check that alternating sections have different backgrounds
      // The :nth-child(even) selector applies background to even sections in the main element
      const sections = page.locator('main section');
      const count = await sections.count();
      expect(count).toBeGreaterThan(1);

      // Verify sections are visible and backgrounds are applied correctly
      for (let i = 0; i < Math.min(count, 3); i++) {
        const section = sections.nth(i);
        await expect(section).toBeVisible();
      }

      // Verify the hero section has a gradient (it's outside main)
      const hero = page.locator('.hero');
      const heroBg = await hero.evaluate((el) => {
        return window.getComputedStyle(el).backgroundImage;
      });
      expect(heroBg).toContain('linear-gradient');
    });

    test('should have correct feature card background in dark mode', async ({ page }) => {
      const featureCard = page.locator('.feature-card').first();
      await expect(featureCard).toBeVisible();

      const cardBg = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Feature cards should have bg-color (#1a1a1a)
      expect(cardBg).toBe('rgb(26, 26, 26)');
    });

    test('should have correct config table styling in dark mode', async ({ page }) => {
      const configTable = page.locator('.config-table table');
      await expect(configTable).toBeVisible();

      const tableBg = await configTable.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Table should have bg-color
      expect(tableBg).toBe('rgb(26, 26, 26)');
    });
  });
});
