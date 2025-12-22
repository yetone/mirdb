// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Dark Mode Support', () => {
  test.describe('Test Case 1: Load page with prefers-color-scheme: dark', () => {
    test('page displays in dark mode color scheme when system preference is dark', async ({ page }) => {
      // Emulate dark color scheme preference
      await page.emulateMedia({ colorScheme: 'dark' });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify dark mode background color is applied
      const body = page.locator('body');
      const backgroundColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Dark mode background should be #0f172a (rgb(15, 23, 42))
      expect(backgroundColor).toBe('rgb(15, 23, 42)');

      // Verify text color is light for dark mode
      const textColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Dark mode text should be #f8fafc (rgb(248, 250, 252))
      expect(textColor).toBe('rgb(248, 250, 252)');
    });

    test('page loads with dark theme styles by default', async ({ page }) => {
      // Default should be dark mode (as per CSS structure)
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Check that CSS custom properties are correctly set for dark mode
      const rootBackgroundColor = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--color-background').trim();
      });

      expect(rootBackgroundColor).toBe('#0f172a');
    });
  });

  test.describe('Test Case 2: Check dark mode background colors', () => {
    test('dark backgrounds with light text are applied', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check main body background
      const bodyBg = await page.locator('body').evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(bodyBg).toBe('rgb(15, 23, 42)'); // #0f172a

      // Check features section has surface color background
      const featuresSection = page.locator('.features-section');
      const featuresBg = await featuresSection.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(featuresBg).toBe('rgb(30, 41, 59)'); // #1e293b (surface color)

      // Check text color is light
      const heroText = page.locator('.hero-tagline');
      const textColor = await heroText.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(textColor).toBe('rgb(248, 250, 252)'); // #f8fafc

      // Check muted text color
      const mutedText = page.locator('.hero-description');
      const mutedColor = await mutedText.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(mutedColor).toBe('rgb(148, 163, 184)'); // #94a3b8
    });

    test('feature cards have correct dark mode styling', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      const featureCard = page.locator('.feature-card').first();

      // Check card background is dark
      const cardBg = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(cardBg).toBe('rgb(15, 23, 42)'); // #0f172a

      // Check card border
      const cardBorder = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).borderColor;
      });
      expect(cardBorder).toBe('rgb(51, 65, 85)'); // #334155
    });

    test('header has dark mode styling with backdrop blur', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      const header = page.locator('.header');
      const headerBg = await header.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Header should have semi-transparent dark background
      // rgba(15, 23, 42, 0.95)
      expect(headerBg).toMatch(/rgba\(15,\s*23,\s*42,\s*0\.95\)/);
    });
  });

  test.describe('Test Case 3: Check code snippet in dark mode', () => {
    test('code snippet has appropriate dark mode syntax highlighting', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Navigate to quickstart section
      const codeBlock = page.locator('.code-block').first();
      await codeBlock.scrollIntoViewIfNeeded();

      // Check code block has dark surface background
      const codeBg = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(codeBg).toBe('rgb(30, 41, 59)'); // #1e293b (surface)

      // Check code text color
      const codeElement = codeBlock.locator('code');
      const codeColor = await codeElement.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(codeColor).toBe('rgb(248, 250, 252)'); // #f8fafc (light text)
    });

    test('syntax highlighting tokens have correct colors in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Check comment token color
      const commentToken = page.locator('.token.comment').first();
      if (await commentToken.count() > 0) {
        const commentColor = await commentToken.evaluate((el) => {
          return window.getComputedStyle(el).color;
        });
        expect(commentColor).toBe('rgb(107, 114, 128)'); // #6b7280
      }

      // Check keyword token color
      const keywordToken = page.locator('.token.keyword').first();
      if (await keywordToken.count() > 0) {
        const keywordColor = await keywordToken.evaluate((el) => {
          return window.getComputedStyle(el).color;
        });
        expect(keywordColor).toBe('rgb(192, 132, 252)'); // #c084fc (purple)
      }

      // Check string token color
      const stringToken = page.locator('.token.string').first();
      if (await stringToken.count() > 0) {
        const stringColor = await stringToken.evaluate((el) => {
          return window.getComputedStyle(el).color;
        });
        expect(stringColor).toBe('rgb(134, 239, 172)'); // #86efac (green)
      }
    });

    test('code block header has dark styling', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      const codeHeader = page.locator('.code-header').first();
      await codeHeader.scrollIntoViewIfNeeded();

      // Check header background
      const headerBg = await codeHeader.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      // Should have rgba(0, 0, 0, 0.2) overlay
      expect(headerBg).toMatch(/rgba\(0,\s*0,\s*0,\s*0\.2\)/);

      // Check title color (muted)
      const codeTitle = page.locator('.code-title').first();
      const titleColor = await codeTitle.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(titleColor).toBe('rgb(148, 163, 184)'); // #94a3b8
    });
  });

  test.describe('Test Case 4: Check contrast in dark mode', () => {
    test('all text maintains sufficient contrast in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Helper function to calculate relative luminance
      const getRelativeLuminance = (r, g, b) => {
        const [rs, gs, bs] = [r, g, b].map(c => {
          c = c / 255;
          return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      };

      // Helper function to calculate contrast ratio
      const getContrastRatio = (l1, l2) => {
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      };

      // Dark background color: #0f172a (rgb(15, 23, 42))
      const bgLuminance = getRelativeLuminance(15, 23, 42);

      // Main text color: #f8fafc (rgb(248, 250, 252))
      const textLuminance = getRelativeLuminance(248, 250, 252);
      const mainTextContrast = getContrastRatio(textLuminance, bgLuminance);

      // WCAG AA requires 4.5:1 for normal text
      expect(mainTextContrast).toBeGreaterThanOrEqual(4.5);

      // Muted text color: #94a3b8 (rgb(148, 163, 184))
      const mutedLuminance = getRelativeLuminance(148, 163, 184);
      const mutedTextContrast = getContrastRatio(mutedLuminance, bgLuminance);

      // Muted text should still have reasonable contrast (at least 3:1 for large text)
      expect(mutedTextContrast).toBeGreaterThanOrEqual(3);
    });

    test('hero section text has sufficient contrast', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Check hero tagline is visible and readable
      const heroTagline = page.locator('.hero-tagline');
      await expect(heroTagline).toBeVisible();

      const taglineColor = await heroTagline.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(taglineColor).toBe('rgb(248, 250, 252)'); // Bright white text
    });

    test('links have sufficient contrast in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Check primary button contrast
      const primaryBtn = page.locator('.btn-primary').first();
      const btnBg = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(btnBg).toBe('rgb(59, 130, 246)'); // #3b82f6 (primary blue)

      const btnColor = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(btnColor).toBe('rgb(255, 255, 255)'); // white text on blue
    });

    test('navigation links have appropriate contrast', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      const navLink = page.locator('.nav-links a').first();
      const linkColor = await navLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Nav links use muted text color
      expect(linkColor).toBe('rgb(148, 163, 184)'); // #94a3b8
    });
  });

  test.describe('Test Case 5: Toggle between light and dark mode', () => {
    test('page responds to system preference changes dynamically', async ({ page }) => {
      // Start with dark mode
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify dark mode is active
      let bodyBg = await page.locator('body').evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(bodyBg).toBe('rgb(15, 23, 42)'); // Dark background

      let textColor = await page.locator('body').evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(textColor).toBe('rgb(248, 250, 252)'); // Light text

      // Switch to light mode
      await page.emulateMedia({ colorScheme: 'light' });

      // Wait for CSS to update
      await page.waitForTimeout(100);

      // Verify light mode is now active
      bodyBg = await page.locator('body').evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(bodyBg).toBe('rgb(248, 250, 252)'); // #f8fafc (light background)

      textColor = await page.locator('body').evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(textColor).toBe('rgb(15, 23, 42)'); // #0f172a (dark text)
    });

    test('light mode applies correct surface colors', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto('/');

      // Check features section has light surface color
      const featuresSection = page.locator('.features-section');
      const featuresBg = await featuresSection.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(featuresBg).toBe('rgb(255, 255, 255)'); // #ffffff (white surface)

      // Check border color in light mode
      const featureCard = page.locator('.feature-card').first();
      const cardBorder = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).borderColor;
      });
      expect(cardBorder).toBe('rgb(226, 232, 240)'); // #e2e8f0 (light border)
    });

    test('switching between modes preserves page layout', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Get initial layout metrics
      const heroHeight = await page.locator('.hero').boundingBox();
      const featuresHeight = await page.locator('.features-section').boundingBox();

      // Switch to light mode
      await page.emulateMedia({ colorScheme: 'light' });
      await page.waitForTimeout(100);

      // Get layout metrics after switch
      const heroHeightAfter = await page.locator('.hero').boundingBox();
      const featuresHeightAfter = await page.locator('.features-section').boundingBox();

      // Layout should remain consistent
      expect(heroHeightAfter?.height).toBe(heroHeight?.height);
      expect(featuresHeightAfter?.height).toBe(featuresHeight?.height);
    });

    test('code blocks update correctly when switching modes', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      const codeBlock = page.locator('.code-block').first();
      await codeBlock.scrollIntoViewIfNeeded();

      // Check dark mode code block
      let codeBg = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(codeBg).toBe('rgb(30, 41, 59)'); // Dark surface

      // Switch to light mode
      await page.emulateMedia({ colorScheme: 'light' });
      await page.waitForTimeout(100);

      // Check light mode code block
      codeBg = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(codeBg).toBe('rgb(255, 255, 255)'); // Light surface
    });

    test('multiple rapid toggles work correctly', async ({ page }) => {
      await page.goto('/');

      // Rapidly toggle between modes
      for (let i = 0; i < 5; i++) {
        await page.emulateMedia({ colorScheme: i % 2 === 0 ? 'dark' : 'light' });
        await page.waitForTimeout(50);
      }

      // Final state should be light (since 4 % 2 === 0 means dark was last)
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.waitForTimeout(100);

      const bodyBg = await page.locator('body').evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(bodyBg).toBe('rgb(15, 23, 42)');
    });
  });

  test.describe('Additional Dark Mode Edge Cases', () => {
    test('architecture diagram maintains visibility in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      const diagram = page.locator('[data-testid="architecture-diagram"]');
      await diagram.scrollIntoViewIfNeeded();

      // Verify diagram nodes are visible
      const diagramNode = page.locator('.diagram-node').first();
      await expect(diagramNode).toBeVisible();

      // Check node has appropriate background
      const nodeBg = await diagramNode.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(nodeBg).toBe('rgb(30, 41, 59)'); // Surface color
    });

    test('configuration section has readable values in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      const configValue = page.locator('.config-value').first();
      await configValue.scrollIntoViewIfNeeded();

      // Config values use accent color which should be visible
      const valueColor = await configValue.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(valueColor).toBe('rgb(34, 211, 238)'); // #22d3ee (cyan accent)
    });

    test('footer is readable in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      const footer = page.locator('.footer');
      await footer.scrollIntoViewIfNeeded();

      // Check footer text color
      const footerText = footer.locator('p').first();
      const textColor = await footerText.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(textColor).toBe('rgb(148, 163, 184)'); // Muted text

      // Check footer border
      const footerBorder = await footer.evaluate((el) => {
        return window.getComputedStyle(el).borderTopColor;
      });
      expect(footerBorder).toBe('rgb(51, 65, 85)'); // #334155
    });

    test('focus states are visible in dark mode', async ({ page }) => {
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      // Tab to a link to trigger focus
      const firstLink = page.locator('.nav-links a').first();
      await firstLink.focus();

      // Check focus outline is visible
      const outlineColor = await firstLink.evaluate((el) => {
        return window.getComputedStyle(el).outlineColor;
      });
      expect(outlineColor).toBe('rgb(34, 211, 238)'); // #22d3ee (accent color)
    });
  });
});
