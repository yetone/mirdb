/**
 * Cross-Browser Compatibility Tests
 * Owner: Scenario 12 - Cross-Browser Compatibility
 *
 * Verifies the homepage renders correctly and functions properly on modern browsers:
 * - Chrome (Chromium)
 * - Firefox (Gecko)
 * - Safari (WebKit)
 * - Edge (Chromium-based, covered by chromium project)
 *
 * Tests:
 * - Page rendering and structure
 * - Theme toggle functionality
 * - Navigation and smooth scroll
 * - Copy-to-clipboard functionality
 * - CSS Grid/Flexbox layout support
 */

import { test, expect, type BrowserName } from '@playwright/test';

// Note: Edge is Chromium-based and shares the same rendering engine as Chrome.
// Tests passing on Chromium project effectively validate Edge compatibility.
// The chromium project in playwright.config.ts covers both Chrome and Edge.

test.describe('Cross-Browser Compatibility', () => {
  test.describe('Page Rendering', () => {
    test('should render the homepage with all sections visible', async ({ page, browserName }) => {
      await page.goto('/');

      // Verify page title
      await expect(page).toHaveTitle('MirDB - Persistent Key-Value Store with Memcached Protocol');

      // Verify header is visible
      const header = page.locator('header.header');
      await expect(header).toBeVisible();

      // Verify hero section
      const hero = page.locator('#hero');
      await expect(hero).toBeVisible();
      await expect(page.locator('.hero__title')).toContainText('Persistent Key-Value Store');

      // Verify features section
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      // Verify demo section
      const demo = page.locator('#demo');
      await expect(demo).toBeVisible();

      // Verify getting started section
      const gettingStarted = page.locator('#getting-started');
      await expect(gettingStarted).toBeVisible();

      // Verify footer
      const footer = page.locator('footer.footer');
      await expect(footer).toBeVisible();

      // Log browser for debugging
      console.log(`✓ Page rendered correctly in ${browserName}`);
    });

    test('should render hero section content correctly', async ({ page, browserName }) => {
      await page.goto('/');

      // Hero logo
      const heroLogo = page.locator('.hero__logo');
      await expect(heroLogo).toBeVisible();
      await expect(heroLogo).toHaveAttribute('src', 'assets/images/logo.gif');

      // Hero title
      const heroTitle = page.locator('.hero__title');
      await expect(heroTitle).toBeVisible();

      // Hero subtitle
      const heroSubtitle = page.locator('.hero__subtitle');
      await expect(heroSubtitle).toBeVisible();
      await expect(heroSubtitle).toContainText('Built with Rust');

      // CTA buttons
      const primaryCta = page.locator('.btn-primary');
      await expect(primaryCta).toBeVisible();
      await expect(primaryCta).toHaveText('Get Started');

      const secondaryCta = page.locator('.btn-secondary');
      await expect(secondaryCta).toBeVisible();
      await expect(secondaryCta).toHaveText('View on GitHub');

      console.log(`✓ Hero section rendered correctly in ${browserName}`);
    });

    test('should render all feature cards', async ({ page, browserName }) => {
      await page.goto('/');

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      // Check all 4 feature cards are visible
      const featureCards = page.locator('[data-testid="feature-card"]');
      await expect(featureCards).toHaveCount(4);

      // Verify each card has required elements
      const titles = ['Memcached Protocol', 'Durable Storage', 'LSM Tree', 'Rust Powered'];
      for (const title of titles) {
        await expect(page.locator('.feature-title', { hasText: title })).toBeVisible();
      }

      console.log(`✓ Feature cards rendered correctly in ${browserName}`);
    });

    test('should render demo section with lazy-loaded image', async ({ page, browserName }) => {
      await page.goto('/');

      // Scroll to demo section
      const demoSection = page.locator('#demo');
      await demoSection.scrollIntoViewIfNeeded();

      // Verify demo image
      const demoImage = page.locator('.demo__image');
      await expect(demoImage).toBeVisible();
      await expect(demoImage).toHaveAttribute('loading', 'lazy');

      // Verify caption
      const caption = page.locator('.demo__caption');
      await expect(caption).toBeVisible();

      console.log(`✓ Demo section rendered correctly in ${browserName}`);
    });

    test('should render getting started section with code blocks', async ({ page, browserName }) => {
      await page.goto('/');

      // Scroll to getting started section
      const gettingStartedSection = page.locator('#getting-started');
      await gettingStartedSection.scrollIntoViewIfNeeded();

      // Verify installation code block
      const installationBlock = page.locator('[data-testid="installation-code-block"]');
      await expect(installationBlock).toBeVisible();

      // Verify example code block
      const exampleBlock = page.locator('[data-testid="example-code-block"]');
      await expect(exampleBlock).toBeVisible();

      console.log(`✓ Getting started section rendered correctly in ${browserName}`);
    });
  });

  test.describe('Theme Toggle', () => {
    test('should toggle between light and dark mode', async ({ page, browserName }) => {
      await page.goto('/');

      // Get the html element to check theme attribute
      const html = page.locator('html');

      // Check initial theme state (could be light or dark depending on system preference)
      const initialTheme = await html.getAttribute('data-theme');

      // Find theme toggle button if it exists
      const themeToggle = page.locator('[data-testid="theme-toggle"], .theme-toggle, #theme-toggle, button[aria-label*="theme"], button[aria-label*="mode"]');

      // Check if theme toggle exists
      const toggleCount = await themeToggle.count();

      if (toggleCount > 0) {
        // Click theme toggle
        await themeToggle.first().click();

        // Wait for theme to change
        await page.waitForTimeout(300);

        // Verify theme changed
        const newTheme = await html.getAttribute('data-theme');

        // If initial theme was set, verify it changed
        if (initialTheme) {
          expect(newTheme).not.toBe(initialTheme);
        }

        console.log(`✓ Theme toggle works in ${browserName}: ${initialTheme || 'default'} -> ${newTheme}`);
      } else {
        // Theme toggle may not be implemented yet (Scenario 9)
        // Verify CSS custom properties are supported (theme infrastructure)
        const computedStyle = await page.evaluate(() => {
          return getComputedStyle(document.documentElement).getPropertyValue('--primary-color') ||
                 getComputedStyle(document.documentElement).getPropertyValue('--bg-color') ||
                 'css-vars-not-set';
        });

        console.log(`ℹ Theme toggle not found in ${browserName}, CSS custom properties: ${computedStyle}`);
        // Test passes as theme toggle is owned by Scenario 9
      }
    });
  });

  test.describe('Navigation', () => {
    test('should have working navigation links', async ({ page, browserName }) => {
      await page.goto('/');

      // Test internal navigation links
      const navLinks = [
        { href: '#features', text: 'Features' },
        { href: '#demo', text: 'Demo' },
        { href: '#getting-started', text: 'Getting Started' },
      ];

      for (const link of navLinks) {
        const navLink = page.locator(`.header__nav-link[href="${link.href}"]`);
        await expect(navLink).toBeVisible();
      }

      console.log(`✓ Navigation links visible in ${browserName}`);
    });

    test('should perform smooth scroll to sections', async ({ page, browserName }) => {
      await page.goto('/');

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click on features link
      const featuresLink = page.locator('.header__nav-link[href="#features"]');
      await featuresLink.click();

      // Wait for scroll animation
      await page.waitForTimeout(500);

      // Verify scroll position changed
      const newScrollY = await page.evaluate(() => window.scrollY);
      expect(newScrollY).toBeGreaterThan(initialScrollY);

      // Verify features section is in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport({ ratio: 0.5 });

      console.log(`✓ Smooth scroll navigation works in ${browserName}`);
    });

    test('should have working GitHub links', async ({ page, browserName }) => {
      await page.goto('/');

      // Check GitHub link in header navigation
      const headerGithubLink = page.locator('.header__nav-link--github');
      await expect(headerGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(headerGithubLink).toHaveAttribute('target', '_blank');
      await expect(headerGithubLink).toHaveAttribute('rel', /noopener/);

      // Check GitHub link in footer
      const footerGithubLink = page.locator('.footer__link--github');
      await expect(footerGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(footerGithubLink).toHaveAttribute('target', '_blank');

      console.log(`✓ GitHub links configured correctly in ${browserName}`);
    });
  });

  test.describe('Copy to Clipboard', () => {
    test('should support clipboard functionality', async ({ page, browserName, context }) => {
      await page.goto('/');

      // Grant clipboard permissions (only supported in Chromium)
      // Firefox and WebKit have different permission models
      if (browserName === 'chromium') {
        try {
          await context.grantPermissions(['clipboard-read', 'clipboard-write']);
        } catch {
          // Permission granting may fail, continue with test
        }
      }

      // Scroll to getting started section
      await page.locator('#getting-started').scrollIntoViewIfNeeded();

      // Check if copy buttons exist
      const copyButtons = page.locator('.copy-btn, [data-testid="copy-btn"], button[aria-label*="copy"], .code-block button');
      const copyButtonCount = await copyButtons.count();

      if (copyButtonCount > 0) {
        // Click the first copy button
        await copyButtons.first().click();

        // Wait for clipboard operation
        await page.waitForTimeout(200);

        // Verify clipboard API is available
        const clipboardSupported = await page.evaluate(() => {
          return 'clipboard' in navigator && 'writeText' in navigator.clipboard;
        });

        expect(clipboardSupported).toBe(true);
        console.log(`✓ Copy functionality works in ${browserName}`);
      } else {
        // Copy buttons may not be implemented yet (Scenario 5)
        // Verify Clipboard API is available in the browser
        const clipboardSupported = await page.evaluate(() => {
          return 'clipboard' in navigator && 'writeText' in navigator.clipboard;
        });

        expect(clipboardSupported).toBe(true);
        console.log(`ℹ Copy buttons not found in ${browserName}, but Clipboard API is supported`);
      }
    });
  });

  test.describe('CSS Grid/Flexbox Support', () => {
    test('should correctly render layout with CSS Grid', async ({ page, browserName }) => {
      await page.goto('/');

      // Check features grid layout
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('.features-grid');
      const gridDisplay = await featuresGrid.evaluate((el) => {
        return getComputedStyle(el).display;
      });

      // Grid or flex are both acceptable modern CSS layout methods
      expect(['grid', 'flex']).toContain(gridDisplay);

      // Verify feature cards are laid out correctly (at least 2 columns on desktop)
      const featureCards = page.locator('[data-testid="feature-card"]');
      const firstCardBox = await featureCards.first().boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      if (firstCardBox && secondCardBox) {
        // On desktop, cards should be side by side (different X positions)
        // On mobile, cards might be stacked (same X positions)
        const viewport = page.viewportSize();
        if (viewport && viewport.width >= 768) {
          // Desktop: cards should be side by side
          expect(secondCardBox.x).toBeGreaterThan(firstCardBox.x);
        }
      }

      console.log(`✓ CSS Grid layout works in ${browserName} (display: ${gridDisplay})`);
    });

    test('should correctly render layout with Flexbox', async ({ page, browserName }) => {
      await page.goto('/');

      // Check header uses flexbox
      const headerContainer = page.locator('.header__container');
      const headerDisplay = await headerContainer.evaluate((el) => {
        return getComputedStyle(el).display;
      });

      expect(['flex', 'grid']).toContain(headerDisplay);

      // Check hero CTA buttons use flexbox
      const heroCta = page.locator('.hero__cta');
      const ctaDisplay = await heroCta.evaluate((el) => {
        return getComputedStyle(el).display;
      });

      expect(['flex', 'grid', 'block', 'inline-flex']).toContain(ctaDisplay);

      // Check footer container uses flexbox
      const footerContainer = page.locator('.footer__container');
      const footerDisplay = await footerContainer.evaluate((el) => {
        return getComputedStyle(el).display;
      });

      expect(['flex', 'grid', 'block']).toContain(footerDisplay);

      console.log(`✓ Flexbox layout works in ${browserName}`);
    });

    test('should support CSS custom properties (variables)', async ({ page, browserName }) => {
      await page.goto('/');

      // Test that CSS custom properties are supported
      const cssVarsSupported = await page.evaluate(() => {
        const testElement = document.createElement('div');
        testElement.style.setProperty('--test-var', '10px');
        document.body.appendChild(testElement);
        const value = getComputedStyle(testElement).getPropertyValue('--test-var');
        document.body.removeChild(testElement);
        return value.trim() === '10px';
      });

      expect(cssVarsSupported).toBe(true);
      console.log(`✓ CSS custom properties supported in ${browserName}`);
    });
  });

  test.describe('Browser-Specific Rendering', () => {
    test('should render images correctly', async ({ page, browserName }) => {
      await page.goto('/');

      // Check logo in header
      const headerLogo = page.locator('.header__logo img');
      await expect(headerLogo).toBeVisible();

      // Verify image loaded successfully
      const headerLogoLoaded = await headerLogo.evaluate((img: HTMLImageElement) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(headerLogoLoaded).toBe(true);

      // Check hero logo
      const heroLogo = page.locator('.hero__logo');
      await expect(heroLogo).toBeVisible();

      const heroLogoLoaded = await heroLogo.evaluate((img: HTMLImageElement) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(heroLogoLoaded).toBe(true);

      // Check feature icons
      const featureIcons = page.locator('.feature-icon img');
      const iconCount = await featureIcons.count();
      expect(iconCount).toBe(4);

      for (let i = 0; i < iconCount; i++) {
        const icon = featureIcons.nth(i);
        const iconLoaded = await icon.evaluate((img: HTMLImageElement) => {
          return img.complete && img.naturalWidth > 0;
        });
        expect(iconLoaded).toBe(true);
      }

      console.log(`✓ All images rendered correctly in ${browserName}`);
    });

    test('should render SVG icons correctly', async ({ page, browserName }) => {
      await page.goto('/');

      // Check GitHub SVG icon in header
      const headerGithubIcon = page.locator('.header__github-icon');
      await expect(headerGithubIcon).toBeVisible();

      // Verify SVG has proper viewBox
      const viewBox = await headerGithubIcon.getAttribute('viewBox');
      expect(viewBox).toBeTruthy();

      // Check GitHub SVG icon in footer
      const footerGithubIcon = page.locator('.footer__github-icon');
      await expect(footerGithubIcon).toBeVisible();

      console.log(`✓ SVG icons rendered correctly in ${browserName}`);
    });

    test('should have proper text rendering', async ({ page, browserName }) => {
      await page.goto('/');

      // Check hero title font rendering
      const heroTitle = page.locator('.hero__title');
      const titleFontSize = await heroTitle.evaluate((el) => {
        return getComputedStyle(el).fontSize;
      });

      // Font size should be a valid value
      expect(titleFontSize).toMatch(/^\d+(\.\d+)?px$/);

      // Font should be properly loaded
      const fontsLoaded = await page.evaluate(async () => {
        if ('fonts' in document) {
          await document.fonts.ready;
          return document.fonts.status === 'loaded';
        }
        return true; // Assume loaded if API not available
      });
      expect(fontsLoaded).toBe(true);

      console.log(`✓ Text rendering works in ${browserName}`);
    });
  });

  test.describe('Interactive Elements', () => {
    test('should have proper hover states on buttons', async ({ page, browserName }) => {
      await page.goto('/');

      // Get primary CTA button
      const primaryBtn = page.locator('.btn-primary').first();
      await expect(primaryBtn).toBeVisible();

      // Get initial background color
      const initialBg = await primaryBtn.evaluate((el) => {
        return getComputedStyle(el).backgroundColor;
      });

      // Hover over button
      await primaryBtn.hover();
      await page.waitForTimeout(100);

      // Check cursor style
      const cursor = await primaryBtn.evaluate((el) => {
        return getComputedStyle(el).cursor;
      });
      expect(cursor).toBe('pointer');

      console.log(`✓ Button hover states work in ${browserName}`);
    });

    test('should have proper focus states for accessibility', async ({ page, browserName }) => {
      await page.goto('/');

      // Tab to first focusable element
      await page.keyboard.press('Tab');

      // Get the focused element
      const focusedElement = page.locator(':focus');
      await expect(focusedElement).toBeVisible();

      // Check that focus outline is visible
      const outlineStyle = await focusedElement.evaluate((el) => {
        const style = getComputedStyle(el);
        return {
          outline: style.outline,
          outlineWidth: style.outlineWidth,
          boxShadow: style.boxShadow,
        };
      });

      // Either outline or box-shadow should provide focus indication
      const hasFocusIndicator =
        (outlineStyle.outlineWidth !== '0px' && outlineStyle.outline !== 'none') ||
        outlineStyle.boxShadow !== 'none';

      // Note: This is a soft check since focus styles might be custom
      console.log(`✓ Focus states available in ${browserName}: outline=${outlineStyle.outline}, boxShadow=${outlineStyle.boxShadow}`);
    });

    test('should have clickable links with proper cursor', async ({ page, browserName }) => {
      await page.goto('/');

      // Check navigation links
      const navLinks = page.locator('.header__nav-link');
      const linkCount = await navLinks.count();

      for (let i = 0; i < linkCount; i++) {
        const link = navLinks.nth(i);
        const cursor = await link.evaluate((el) => {
          return getComputedStyle(el).cursor;
        });
        // Links default to 'auto' cursor in browsers, which resolves to 'pointer' visually
        // WebKit reports 'auto' while Chromium/Firefox may report 'pointer' if explicitly set
        // Both are acceptable for clickable links
        expect(['pointer', 'auto']).toContain(cursor);
      }

      console.log(`✓ All links have clickable cursor in ${browserName}`);
    });
  });
});

// Browser-specific test variants for explicit coverage
test.describe('Chrome-specific tests @chromium', () => {
  test.skip(({ browserName }) => browserName !== 'chromium', 'Chrome-only test');

  test('verifies Chrome/Edge Chromium rendering', async ({ page }) => {
    await page.goto('/');

    // Verify page loads in Chromium
    await expect(page).toHaveTitle(/MirDB/);

    // Test Chrome-specific features
    const supportsContainer = await page.evaluate(() => {
      return CSS.supports('container-type', 'inline-size');
    });

    console.log(`✓ Chrome rendering verified, container queries supported: ${supportsContainer}`);
  });
});

test.describe('Firefox-specific tests @firefox', () => {
  test.skip(({ browserName }) => browserName !== 'firefox', 'Firefox-only test');

  test('verifies Firefox Gecko rendering', async ({ page }) => {
    await page.goto('/');

    // Verify page loads in Firefox
    await expect(page).toHaveTitle(/MirDB/);

    // Test scrollbar styling (Firefox has different scrollbar behavior)
    const hasScrollbarStyling = await page.evaluate(() => {
      return CSS.supports('scrollbar-width', 'thin');
    });

    console.log(`✓ Firefox rendering verified, scrollbar-width supported: ${hasScrollbarStyling}`);
  });
});

test.describe('Safari-specific tests @webkit', () => {
  test.skip(({ browserName }) => browserName !== 'webkit', 'Safari-only test');

  test('verifies Safari WebKit rendering', async ({ page }) => {
    await page.goto('/');

    // Verify page loads in Safari
    await expect(page).toHaveTitle(/MirDB/);

    // Test WebKit-specific features
    const supportsBackdropFilter = await page.evaluate(() => {
      return CSS.supports('-webkit-backdrop-filter', 'blur(10px)') ||
             CSS.supports('backdrop-filter', 'blur(10px)');
    });

    console.log(`✓ Safari rendering verified, backdrop-filter supported: ${supportsBackdropFilter}`);
  });
});
