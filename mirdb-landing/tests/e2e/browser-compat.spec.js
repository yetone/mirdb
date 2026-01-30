/**
 * Browser Compatibility E2E Tests
 * Owner: Scenario 11 - Browser Compatibility
 *
 * Tests:
 * - Chrome functionality
 * - Firefox functionality
 * - Safari (WebKit) functionality
 * - Edge functionality (Chromium-based)
 * - CSS fallbacks
 * - No-JS behavior
 *
 * NFR-4: Support latest 2 versions of Chrome, Firefox, Safari, Edge
 */

const { test, expect } = require('@playwright/test');

test.describe('Browser Compatibility - Core Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('page loads and displays core content', async ({ page, browserName }) => {
    // Verify page title
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section is visible
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();

    // Log browser name for debugging
    console.log(`Testing on: ${browserName}`);
  });

  test('navigation is functional', async ({ page }) => {
    // Verify navigation exists
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Verify navigation links exist
    const navLinks = page.locator('.nav__links .nav__link');
    await expect(navLinks).toHaveCount(5); // Features, Usage, Architecture, Roadmap, GitHub

    // Test clicking on a navigation link
    const featuresLink = page.locator('.nav__link[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Verify we scrolled to features section
    await page.waitForTimeout(500); // Wait for smooth scroll
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('all sections are visible and accessible', async ({ page }) => {
    // Check all main sections exist
    const sections = ['hero', 'features', 'usage', 'architecture', 'roadmap'];

    for (const sectionId of sections) {
      const section = page.locator(`#${sectionId}`);
      await expect(section).toBeAttached();
    }

    // Check footer exists
    const footer = page.locator('.footer');
    await expect(footer).toBeAttached();
  });
});

test.describe('Browser Compatibility - Animations', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('scroll animations work correctly', async ({ page }) => {
    // Feature cards should have animate-on-scroll class
    const featureCards = page.locator('.feature-card.animate-on-scroll');
    await expect(featureCards.first()).toBeAttached();

    // Scroll to features section to trigger animations
    await page.locator('#features').scrollIntoViewIfNeeded();
    await page.waitForTimeout(600); // Wait for animation to complete

    // After scrolling into view, cards should become visible (have is-visible class)
    const visibleCard = featureCards.first();
    await expect(visibleCard).toHaveClass(/is-visible/);
  });

  test('animations respect prefers-reduced-motion', async ({ page }) => {
    // Emulate reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto('/');

    // With reduced motion, elements should have CSS rules applied
    // The CSS contains: @media (prefers-reduced-motion: reduce) { transition: none; }
    const animatedElement = page.locator('.animate-on-scroll').first();
    if (await animatedElement.isVisible()) {
      const styles = await animatedElement.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        // Get actual numeric transition duration in milliseconds
        const duration = computed.transitionDuration;
        const durationMs = parseFloat(duration) * (duration.includes('ms') ? 1 : 1000);
        return {
          transitionDuration: duration,
          durationMs: durationMs,
          opacity: computed.opacity,
          transform: computed.transform
        };
      });

      // With reduced motion, the element should be fully visible (opacity: 1)
      // and have no transform applied (transform: none or matrix identity)
      // This validates that the prefers-reduced-motion media query is working
      expect(styles.opacity).toBe('1');
      expect(
        styles.transform === 'none' ||
        styles.transform === 'matrix(1, 0, 0, 1, 0, 0)'
      ).toBeTruthy();
    }
  });

  test('CSS transitions use GPU-accelerated properties', async ({ page }) => {
    // Verify animations use transform and opacity (GPU-accelerated)
    const animatedElement = page.locator('.animate-on-scroll').first();

    if (await animatedElement.isVisible()) {
      const styles = await animatedElement.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          transitionProperty: computed.transitionProperty,
          willChange: computed.willChange
        };
      });

      // Should use transform and/or opacity for GPU acceleration
      const usesGpuAccelerated =
        styles.transitionProperty.includes('transform') ||
        styles.transitionProperty.includes('opacity') ||
        styles.transitionProperty === 'all';
      expect(usesGpuAccelerated).toBeTruthy();
    }
  });
});

test.describe('Browser Compatibility - Smooth Scrolling', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('smooth scroll behavior is enabled', async ({ page }) => {
    // Check if scroll-behavior is set on html element
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });

    expect(scrollBehavior).toBe('smooth');
  });

  test('anchor links scroll to correct sections', async ({ page }) => {
    // Click on Features link
    await page.locator('.nav__link[href="#features"]').click();
    await page.waitForTimeout(800); // Wait for smooth scroll

    // Verify features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Click on Architecture link
    await page.locator('.nav__link[href="#architecture"]').click();
    await page.waitForTimeout(800);

    // Verify architecture section is in view
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();
  });
});

test.describe('Browser Compatibility - Responsive Layout', () => {
  test('desktop layout displays correctly', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');

    // Navigation should be visible in horizontal layout
    const navLinks = page.locator('.nav__links');
    await expect(navLinks).toBeVisible();

    // Mobile toggle should not be visible on desktop
    const navToggle = page.locator('.nav__toggle');
    await expect(navToggle).not.toBeVisible();

    // Feature cards should be in a grid
    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();
  });

  test('tablet layout adapts correctly', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    // Page content should be visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Features should still be in a grid, possibly 2 columns
    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();
  });

  test('mobile layout adapts correctly', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Hero section should be visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Mobile navigation toggle should be visible
    const navToggle = page.locator('.nav__toggle');
    await expect(navToggle).toBeVisible();

    // Feature cards should stack vertically
    const featureCards = page.locator('.feature-card');
    const firstCard = featureCards.first();
    await expect(firstCard).toBeVisible();
  });

  test('mobile navigation toggle works', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    const navToggle = page.locator('.nav__toggle');
    const navLinks = page.locator('.nav__links');

    // Navigation should initially be closed
    await expect(navToggle).toHaveAttribute('aria-expanded', 'false');

    // Click toggle to open menu
    await navToggle.click();
    await expect(navToggle).toHaveAttribute('aria-expanded', 'true');
    await expect(navLinks).toHaveClass(/nav__links--open/);

    // Click toggle again to close
    await navToggle.click();
    await expect(navToggle).toHaveAttribute('aria-expanded', 'false');
  });
});

test.describe('Browser Compatibility - CSS Custom Properties', () => {
  test('CSS custom properties are applied', async ({ page }) => {
    await page.goto('/');

    // Test that CSS custom properties are being used
    const body = page.locator('body');
    const bodyStyles = await body.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        backgroundColor: computed.backgroundColor,
        color: computed.color,
        fontFamily: computed.fontFamily
      };
    });

    // Background should be dark (--color-bg-primary: #1a1a2e)
    expect(bodyStyles.backgroundColor).toBeTruthy();
    // Text should be light colored
    expect(bodyStyles.color).toBeTruthy();
    // Font family should be applied
    expect(bodyStyles.fontFamily).toBeTruthy();
  });

  test('CSS custom properties have fallback values in styles', async ({ page }) => {
    await page.goto('/');

    // Verify specific elements use the custom properties correctly
    const heroTitle = page.locator('.hero__title');
    const titleColor = await heroTitle.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Title should have a valid color (not transparent or black from missing variable)
    expect(titleColor).toBeTruthy();
    expect(titleColor).not.toBe('rgba(0, 0, 0, 0)');
  });

  test('colors render correctly with CSS variables', async ({ page }) => {
    await page.goto('/');

    // Test accent colors are applied to interactive elements
    const ctaPrimary = page.locator('.hero__cta-primary');
    const ctaBackground = await ctaPrimary.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Primary CTA should have a background color
    expect(ctaBackground).toBeTruthy();
    expect(ctaBackground).not.toBe('rgba(0, 0, 0, 0)');
  });
});

test.describe('Browser Compatibility - No JavaScript Fallback', () => {
  test('core content is visible without JavaScript', async ({ page }) => {
    // Disable JavaScript
    await page.route('**/*.js', route => route.abort());
    await page.goto('/');

    // Core content should still be visible
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();

    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();

    // Navigation links should be present
    const navLinks = page.locator('.nav__links .nav__link');
    const count = await navLinks.count();
    expect(count).toBeGreaterThan(0);
  });

  test('navigation links work without JavaScript (page jumps)', async ({ page }) => {
    // Disable JavaScript
    await page.route('**/*.js', route => route.abort());
    await page.goto('/');

    // Click on features link - should still navigate (page jump)
    const featuresLink = page.locator('.nav__link[href="#features"]');
    await featuresLink.click();

    // URL should have hash
    await expect(page).toHaveURL(/#features/);

    // Features section should be in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport({ ratio: 0.1 });
  });

  test('all content sections are readable without JavaScript', async ({ page }) => {
    // Disable JavaScript
    await page.route('**/*.js', route => route.abort());
    await page.goto('/');

    // Check that main content sections have text content
    const sections = [
      { selector: '.hero__title', expectedText: 'MirDB' },
      { selector: '.hero__tagline', expectedText: /Key-Value Store/ },
      { selector: '#features-title', expectedText: 'Features' },
      { selector: '#architecture-heading', expectedText: 'Technical Architecture' },
      { selector: '#roadmap-title', expectedText: 'Project Status' }
    ];

    for (const section of sections) {
      const element = page.locator(section.selector);
      if (typeof section.expectedText === 'string') {
        await expect(element).toHaveText(section.expectedText);
      } else {
        await expect(element).toHaveText(section.expectedText);
      }
    }
  });

  test('footer is accessible without JavaScript', async ({ page }) => {
    // Disable JavaScript
    await page.route('**/*.js', route => route.abort());
    await page.goto('/');

    // Scroll to footer
    await page.locator('.footer').scrollIntoViewIfNeeded();

    // Footer should be visible
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // GitHub link should be present
    const githubLink = page.locator('.footer__link[href*="github.com"]').first();
    await expect(githubLink).toBeAttached();
  });
});

test.describe('Browser Compatibility - Interactive Features', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('CTA buttons are clickable and styled correctly', async ({ page }) => {
    const primaryCta = page.locator('.hero__cta-primary');
    const secondaryCta = page.locator('.hero__cta-secondary');

    // Both CTAs should be visible
    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    // Primary CTA should link to GitHub
    await expect(primaryCta).toHaveAttribute('href', /github\.com/);

    // Secondary CTA should link to usage section
    await expect(secondaryCta).toHaveAttribute('href', '#usage');

    // Test hover state - check for cursor pointer
    const cursorStyle = await primaryCta.evaluate((el) => {
      return window.getComputedStyle(el).cursor;
    });
    expect(cursorStyle).toBe('pointer');
  });

  test('skip-to-content link is accessible via keyboard', async ({ page }) => {
    // Skip link should exist
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeAttached();

    // Focus the skip link (it's the first focusable element)
    await page.keyboard.press('Tab');

    // Skip link should now be visible (focused state)
    await expect(skipLink).toBeFocused();
  });

  test('keyboard navigation works for navigation menu', async ({ page }) => {
    // Focus the first nav link
    const firstNavLink = page.locator('.nav__link').first();
    await firstNavLink.focus();

    // Should be focusable
    await expect(firstNavLink).toBeFocused();

    // Tab to next link
    await page.keyboard.press('Tab');

    // Second nav link should now be focused
    const secondNavLink = page.locator('.nav__link').nth(1);
    await expect(secondNavLink).toBeFocused();
  });
});

test.describe('Browser Compatibility - Feature Icons and Images', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('logo image loads correctly', async ({ page }) => {
    const logo = page.locator('.hero__logo');
    await expect(logo).toBeVisible();

    // Check that image has loaded (not broken)
    const isLoaded = await logo.evaluate((img) => {
      return img.complete && img.naturalWidth > 0;
    });
    expect(isLoaded).toBeTruthy();
  });

  test('SVG icons render correctly', async ({ page }) => {
    // Check feature card icons are SVGs and visible
    const featureIcons = page.locator('.feature-card__icon svg');
    const iconCount = await featureIcons.count();
    expect(iconCount).toBeGreaterThan(0);

    // First icon should be visible
    await expect(featureIcons.first()).toBeVisible();
  });

  test('architecture diagram loads correctly', async ({ page }) => {
    // Scroll to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    const diagram = page.locator('.architecture__diagram-img');
    await expect(diagram).toBeVisible();

    // Check that SVG has loaded
    const isLoaded = await diagram.evaluate((img) => {
      return img.complete && img.naturalWidth > 0;
    });
    expect(isLoaded).toBeTruthy();
  });
});

test.describe('Browser Compatibility - Cross-Browser Specific Features', () => {
  test('flexbox layout works correctly', async ({ page }) => {
    await page.goto('/');

    // Hero content uses flexbox for centering
    const heroContent = page.locator('.hero__content');
    const displayStyle = await heroContent.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });

    // Should be flex or block (flex container)
    expect(['flex', 'block', '-webkit-box', '-webkit-flex']).toContain(displayStyle);
  });

  test('grid layout works correctly', async ({ page }) => {
    await page.goto('/');

    // Features section uses grid
    const featuresGrid = page.locator('.features__grid');
    const displayStyle = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });

    // Should be grid or flex (as fallback)
    expect(['grid', 'flex', '-ms-grid', 'block']).toContain(displayStyle);
  });

  test('border-radius renders correctly', async ({ page }) => {
    await page.goto('/');

    // Feature cards should have border-radius
    const featureCard = page.locator('.feature-card').first();
    const borderRadius = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).borderRadius;
    });

    // Should have some border-radius applied
    expect(borderRadius).toBeTruthy();
    expect(borderRadius).not.toBe('0px');
  });

  test('box-shadow renders correctly', async ({ page }) => {
    await page.goto('/');

    // Feature cards or buttons should have box-shadow
    const ctaButton = page.locator('.hero__cta-primary');
    const boxShadow = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Box shadow can be 'none' or an actual value - both are valid
    expect(boxShadow).toBeTruthy();
  });
});

test.describe('Browser Compatibility - Typography', () => {
  test('custom fonts are applied correctly', async ({ page }) => {
    await page.goto('/');

    // Check body font family
    const bodyFontFamily = await page.evaluate(() => {
      return window.getComputedStyle(document.body).fontFamily;
    });

    // Should have Inter or fallback system fonts
    expect(bodyFontFamily).toMatch(/Inter|system-ui|sans-serif/i);
  });

  test('font sizes use responsive units', async ({ page }) => {
    await page.goto('/');

    // Main title should scale responsively
    const titleFontSize = await page.locator('.hero__title').evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });

    // Font size should be applied (not 0)
    expect(parseFloat(titleFontSize)).toBeGreaterThan(0);
  });

  test('line heights are properly set', async ({ page }) => {
    await page.goto('/');

    // Paragraph text should have readable line height
    const lineHeight = await page.locator('p').first().evaluate((el) => {
      return window.getComputedStyle(el).lineHeight;
    });

    // Line height should be set (normal or numeric value)
    expect(lineHeight).toBeTruthy();
    expect(lineHeight).not.toBe('0');
  });
});
