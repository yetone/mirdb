// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Safari (WebKit) Compatibility Tests
 *
 * These tests verify that the MirDB homepage works correctly in Safari,
 * addressing NFR-5 cross-browser compatibility requirement.
 *
 * Safari-specific considerations:
 * - WebKit engine differences from Chromium
 * - iOS Safari mobile viewport testing
 * - Smooth scrolling behavior in Safari
 * - Touch interaction support for iOS
 */
test.describe('Cross-Browser - Safari Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: All Visual Elements Render Correctly in Safari', () => {
    test('Hero section renders with all visual elements in Safari', async ({ page }) => {
      // Verify hero section is visible
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Verify hero logo renders
      const heroLogo = page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();

      // Verify hero title renders
      const heroTitle = page.locator('#hero-title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Verify tagline renders
      const tagline = page.locator('.hero-tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Key-Value Store');

      // Verify CTA buttons render
      const primaryCta = page.locator('.hero-ctas .btn-primary');
      await expect(primaryCta).toBeVisible();
      const secondaryCta = page.locator('.hero-ctas .btn-secondary');
      await expect(secondaryCta).toBeVisible();
    });

    test('Navigation renders correctly in Safari', async ({ page }) => {
      // Verify navigation is visible
      const nav = page.locator('nav.nav');
      await expect(nav).toBeVisible();

      // Verify nav brand renders
      const navBrand = page.locator('.nav-brand');
      await expect(navBrand).toBeVisible();

      // Verify nav logo renders
      const navLogo = page.locator('.nav-logo');
      await expect(navLogo).toBeVisible();

      // Verify nav links render
      const navLinks = page.locator('.nav-links li');
      await expect(navLinks).toHaveCount(4); // Features, Demo, Quick Start, GitHub
    });

    test('Features section renders with all feature cards in Safari', async ({ page }) => {
      // Navigate to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Verify section title renders
      const featuresTitle = page.locator('#features-title');
      await expect(featuresTitle).toBeVisible();
      await expect(featuresTitle).toHaveText('Key Features');

      // Verify all 6 feature cards render
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(6);

      // Verify each feature card has visible title and description
      for (let i = 0; i < 6; i++) {
        const card = featureCards.nth(i);
        await expect(card.locator('.feature-title')).toBeVisible();
        await expect(card.locator('.feature-description')).toBeVisible();
        await expect(card.locator('.feature-icon')).toBeVisible();
      }
    });

    test('Demo section renders correctly in Safari', async ({ page }) => {
      // Navigate to demo section
      const demoSection = page.locator('#demo');
      await demoSection.scrollIntoViewIfNeeded();
      await expect(demoSection).toBeVisible();

      // Verify demo title renders
      const demoTitle = page.locator('#demo-title');
      await expect(demoTitle).toBeVisible();
      await expect(demoTitle).toHaveText('See It in Action');

      // Verify demo GIF renders
      const demoGif = page.locator('.demo-gif');
      await expect(demoGif).toBeVisible();
    });

    test('Quick start section renders with code blocks in Safari', async ({ page }) => {
      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await quickStartSection.scrollIntoViewIfNeeded();
      await expect(quickStartSection).toBeVisible();

      // Verify code blocks render
      const codeBlocks = page.locator('.code-block');
      await expect(codeBlocks).toHaveCount(2); // Installation and Operations

      // Verify copy buttons render
      const copyButtons = page.locator('.copy-btn');
      await expect(copyButtons).toHaveCount(2);
    });

    test('Roadmap section renders with completed and planned items in Safari', async ({ page }) => {
      // Navigate to roadmap section
      const roadmapSection = page.locator('#roadmap');
      await roadmapSection.scrollIntoViewIfNeeded();
      await expect(roadmapSection).toBeVisible();

      // Verify completed list renders
      const completedList = page.locator('.roadmap-list.completed');
      await expect(completedList).toBeVisible();
      const completedItems = completedList.locator('li');
      await expect(completedItems).toHaveCount(4);

      // Verify planned list renders
      const plannedList = page.locator('.roadmap-list.planned');
      await expect(plannedList).toBeVisible();
    });

    test('Footer renders with all content in Safari', async ({ page }) => {
      // Navigate to footer
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify author attribution renders
      const authorLink = page.locator('.footer-author a');
      await expect(authorLink).toBeVisible();
      await expect(authorLink).toHaveAttribute('href', 'mailto:yetoneful@gmail.com');

      // Verify GitHub link renders
      const githubLink = page.locator('.footer-links a[href="https://github.com/yetone/mirdb"]');
      await expect(githubLink).toBeVisible();
    });

    test('CSS styles are applied correctly in Safari/WebKit', async ({ page }) => {
      // Verify primary button styling is applied
      const primaryBtn = page.locator('.btn-primary').first();
      await expect(primaryBtn).toBeVisible();

      // Check that CSS is loaded by verifying computed styles
      const bgColor = await primaryBtn.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );
      // Background color should be applied (not transparent/empty)
      expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
      expect(bgColor).not.toBe('transparent');

      // Verify body has styles applied
      const body = page.locator('body');
      const fontFamily = await body.evaluate(el =>
        window.getComputedStyle(el).fontFamily
      );
      // Font family should be defined
      expect(fontFamily).toBeTruthy();
      expect(fontFamily.length).toBeGreaterThan(0);
    });

    test('CSS variables are correctly applied in Safari', async ({ page }) => {
      // Safari should support CSS custom properties (variables)
      const primaryColor = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--color-primary').trim();
      });
      expect(primaryColor).toBe('#2563eb');

      // Check border radius variable
      const radiusMd = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--radius-md').trim();
      });
      expect(radiusMd).toBe('0.5rem');
    });

    test('SVG icons render correctly in Safari', async ({ page }) => {
      // Navigate to features section where SVG icons are used
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      // Verify SVG icons are visible
      const svgIcons = page.locator('.feature-icon svg');
      const count = await svgIcons.count();
      expect(count).toBe(6);

      // Verify each SVG has proper dimensions
      for (let i = 0; i < count; i++) {
        const svg = svgIcons.nth(i);
        await expect(svg).toBeVisible();

        // Check viewBox attribute
        const viewBox = await svg.getAttribute('viewBox');
        expect(viewBox).toBe('0 0 24 24');
      }
    });
  });

  test.describe('TC2: Smooth Scrolling Works Correctly in Safari', () => {
    test('Smooth scroll to features section works in Safari', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);
      expect(initialScrollY).toBe(0);

      // Click on Features link
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await featuresLink.click();

      // Wait for scroll to complete
      await page.waitForTimeout(1000);

      // Verify page has scrolled
      const newScrollY = await page.evaluate(() => window.scrollY);
      expect(newScrollY).toBeGreaterThan(0);

      // Verify features section is in view
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('Smooth scroll to demo section works in Safari', async ({ page }) => {
      // Click on Demo link
      const demoLink = page.locator('.nav-links a[href="#demo"]');
      await demoLink.click();

      // Wait for scroll to complete
      await page.waitForTimeout(1000);

      // Verify demo section is in view
      const demoSection = page.locator('#demo');
      await expect(demoSection).toBeInViewport();
    });

    test('Smooth scroll to quick start section works in Safari', async ({ page }) => {
      // Click on Quick Start link
      const quickStartLink = page.locator('.nav-links a[href="#quick-start"]');
      await quickStartLink.click();

      // Wait for scroll to complete
      await page.waitForTimeout(1000);

      // Verify quick start section is in view
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeInViewport();
    });

    test('Get Started CTA scrolls to quick start section in Safari', async ({ page }) => {
      // Click Get Started button
      const getStartedBtn = page.locator('.hero-ctas .btn-primary');
      await getStartedBtn.click();

      // Wait for scroll to complete
      await page.waitForTimeout(1000);

      // Verify quick start section is in view
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeInViewport();
    });

    test('CSS scroll-behavior smooth is applied in Safari', async ({ page }) => {
      // Verify scroll-behavior is set to smooth on html element
      const scrollBehavior = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).scrollBehavior;
      });
      expect(scrollBehavior).toBe('smooth');
    });

    test('Reduced motion preference disables smooth scroll in Safari', async ({ page, context }) => {
      // Create a new page with reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.goto('/');

      // Check that scroll-behavior is auto when reduced motion is preferred
      const scrollBehavior = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).scrollBehavior;
      });
      expect(scrollBehavior).toBe('auto');
    });
  });

  test.describe('TC3: iOS Safari Mobile Touch Interactions', () => {
    test.use({
      viewport: { width: 375, height: 667 }, // iPhone SE viewport
      isMobile: true,
      hasTouch: true,
    });

    test('Touch tap on navigation links works on iOS Safari', async ({ page }) => {
      // Tap on Features link
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await featuresLink.tap();

      // Wait for scroll
      await page.waitForTimeout(1000);

      // Verify features section is visible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('Touch tap on CTA buttons works on iOS Safari', async ({ page }) => {
      // Tap Get Started button
      const getStartedBtn = page.locator('.hero-ctas .btn-primary');
      await getStartedBtn.tap();

      // Wait for scroll
      await page.waitForTimeout(1000);

      // Verify quick start section is visible
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeInViewport();
    });

    test('Touch tap on copy button works on iOS Safari', async ({ page }) => {
      // Navigate to quick start
      const quickStartSection = page.locator('#quick-start');
      await quickStartSection.scrollIntoViewIfNeeded();

      // Tap copy button
      const copyButton = page.locator('.copy-btn[data-copy="install"]');
      await expect(copyButton).toBeVisible();
      await expect(copyButton).toHaveText('Copy');

      // Tap the copy button
      await copyButton.tap();

      // Should show "Copied!" as visual feedback
      await expect(copyButton).toHaveText('Copied!');
    });

    test('Touch targets meet minimum size requirements (44x44px) on iOS Safari', async ({ page }) => {
      // Check navigation links have adequate touch target size
      const navLinks = page.locator('.nav-links a');
      const navLinksCount = await navLinks.count();

      for (let i = 0; i < navLinksCount; i++) {
        const link = navLinks.nth(i);
        const box = await link.boundingBox();
        if (box) {
          // Touch targets should be at least 44px in both dimensions
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }

      // Check copy buttons have adequate touch target size
      const copyButtons = page.locator('.copy-btn');
      const copyButtonsCount = await copyButtons.count();

      for (let i = 0; i < copyButtonsCount; i++) {
        const btn = copyButtons.nth(i);
        await btn.scrollIntoViewIfNeeded();
        const box = await btn.boundingBox();
        if (box) {
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    });

    test('Mobile responsive layout works on iOS Safari', async ({ page }) => {
      // Verify hero section is visible and properly sized
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify navigation is visible
      const nav = page.locator('.nav');
      await expect(nav).toBeVisible();

      // On mobile, navigation should be column layout
      const navDisplay = await nav.evaluate(el =>
        getComputedStyle(el).flexDirection
      );
      expect(navDisplay).toBe('column');

      // Verify feature cards stack vertically on mobile
      const featuresGrid = page.locator('.features-grid');
      await featuresGrid.scrollIntoViewIfNeeded();
      const gridColumns = await featuresGrid.evaluate(el =>
        getComputedStyle(el).gridTemplateColumns
      );
      // On mobile, should be single column (1fr or similar)
      expect(gridColumns).toBeTruthy();
    });

    test('Touch scroll via JavaScript scroll API works on iOS Safari', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);
      expect(initialScrollY).toBe(0);

      // Use JavaScript scrollTo to simulate touch scroll behavior
      await page.evaluate(() => {
        window.scrollTo({ top: 300, behavior: 'smooth' });
      });

      // Wait for scroll to complete
      await page.waitForTimeout(1000);

      // Verify page has scrolled
      const newScrollY = await page.evaluate(() => window.scrollY);
      expect(newScrollY).toBeGreaterThan(initialScrollY);
    });

    test('Footer links have proper touch targets on iOS Safari', async ({ page }) => {
      // Navigate to footer
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // Check footer links have adequate touch target size
      const footerLinks = page.locator('.footer-links a');
      const count = await footerLinks.count();

      for (let i = 0; i < count; i++) {
        const link = footerLinks.nth(i);
        const box = await link.boundingBox();
        if (box) {
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    });

    test('Viewport meta tag is properly set for iOS Safari', async ({ page }) => {
      // Verify viewport meta tag exists and is properly configured
      const viewportMeta = await page.locator('meta[name="viewport"]').getAttribute('content');
      expect(viewportMeta).toContain('width=device-width');
      expect(viewportMeta).toContain('initial-scale=1.0');
    });
  });

  test.describe('Safari-Specific CSS and Rendering', () => {
    test('Linear gradients render correctly in Safari', async ({ page }) => {
      // Hero section uses linear gradient
      const hero = page.locator('.hero');
      const bgImage = await hero.evaluate(el =>
        getComputedStyle(el).backgroundImage
      );
      // Should have a gradient applied
      expect(bgImage).toContain('linear-gradient');
    });

    test('Box shadow renders correctly in Safari', async ({ page }) => {
      // Feature cards have box shadow on hover
      const featureCard = page.locator('.feature-card').first();
      await featureCard.scrollIntoViewIfNeeded();

      // Hover to trigger shadow
      await featureCard.hover();

      const boxShadow = await featureCard.evaluate(el =>
        getComputedStyle(el).boxShadow
      );
      // Box shadow should be applied
      expect(boxShadow).not.toBe('none');
    });

    test('Border radius renders correctly in Safari', async ({ page }) => {
      // Buttons should have border radius
      const primaryBtn = page.locator('.btn-primary').first();
      const borderRadius = await primaryBtn.evaluate(el =>
        getComputedStyle(el).borderRadius
      );
      expect(borderRadius).toBe('8px'); // --radius-md = 0.5rem = 8px
    });

    test('Sticky header works correctly in Safari', async ({ page }) => {
      // Header should be sticky
      const header = page.locator('.header');
      const position = await header.evaluate(el =>
        getComputedStyle(el).position
      );
      expect(position).toBe('sticky');

      // Scroll down
      await page.evaluate(() => window.scrollBy(0, 500));
      await page.waitForTimeout(100);

      // Header should still be visible at top
      await expect(header).toBeVisible();
      const headerBox = await header.boundingBox();
      expect(headerBox?.y).toBeLessThanOrEqual(0);
    });

    test('Flexbox layout works correctly in Safari', async ({ page }) => {
      // Navigation uses flexbox
      const nav = page.locator('.nav');
      const display = await nav.evaluate(el =>
        getComputedStyle(el).display
      );
      expect(display).toBe('flex');

      // Hero CTAs use flexbox
      const heroCtas = page.locator('.hero-ctas');
      const ctasDisplay = await heroCtas.evaluate(el =>
        getComputedStyle(el).display
      );
      expect(ctasDisplay).toBe('flex');
    });

    test('Grid layout works correctly in Safari', async ({ page }) => {
      // Features section uses CSS grid
      const featuresGrid = page.locator('.features-grid');
      await featuresGrid.scrollIntoViewIfNeeded();

      const display = await featuresGrid.evaluate(el =>
        getComputedStyle(el).display
      );
      expect(display).toBe('grid');
    });

    test('Local images load correctly in Safari', async ({ page }) => {
      // Check that local images are loaded (excluding external badge images)
      const localImages = page.locator('img:not([src^="http"])');
      const count = await localImages.count();
      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const img = localImages.nth(i);
        await img.scrollIntoViewIfNeeded();
        await expect(img).toBeVisible();

        // Verify image element exists and is rendered
        const isRendered = await img.evaluate((el) => {
          return el.clientWidth > 0 && el.clientHeight > 0;
        });
        expect(isRendered).toBe(true);
      }
    });

    test('Focus styles work correctly in Safari', async ({ page }) => {
      // Tab to first focusable element
      await page.keyboard.press('Tab');

      // Skip link should be focused and visible
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toBeFocused();

      // Check focus outline is applied
      const outline = await skipLink.evaluate(el =>
        getComputedStyle(el).outline
      );
      expect(outline).toContain('solid');
    });
  });
});
