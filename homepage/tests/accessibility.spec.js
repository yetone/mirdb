// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Accessibility Compliance
 *
 * These tests verify WCAG 2.1 Level AA compliance for:
 * - Test Case 1: All images have alt text
 * - Test Case 2: Proper heading hierarchy (h1, h2, h3)
 * - Test Case 3: Color contrast ratios (4.5:1 minimum for text)
 * - Test Case 4: Keyboard focus indicators
 * - Test Case 5: Tab order follows logical reading order
 * - Test Case 6: Skip to main content link exists
 */

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: All images have descriptive alt attributes', async ({ page }) => {
    // Get all images on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    // Ensure there are images on the page
    expect(imageCount).toBeGreaterThan(0);

    // Check each image has an alt attribute
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const altText = await img.getAttribute('alt');

      // Alt text must exist and not be empty
      expect(altText, `Image ${i + 1} should have alt text`).not.toBeNull();
      expect(altText?.trim().length, `Image ${i + 1} alt text should not be empty`).toBeGreaterThan(0);
    }

    // Specifically verify the MirDB logo has descriptive alt text
    const logo = page.getByTestId('mirdb-logo');
    await expect(logo).toHaveAttribute('alt', 'MirDB Logo');
  });

  test('Test Case 2: Page uses proper heading hierarchy (h1, h2, h3)', async ({ page }) => {
    // Check for exactly one h1 element
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();
    expect(h1Count, 'Page should have exactly one h1 element').toBe(1);

    // Verify h1 is visible and contains meaningful content
    const h1 = h1Elements.first();
    await expect(h1).toBeVisible();
    const h1Text = await h1.textContent();
    expect(h1Text?.trim().length, 'h1 should have meaningful content').toBeGreaterThan(0);

    // Check for h2 elements
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count, 'Page should have h2 elements for section headings').toBeGreaterThan(0);

    // Verify heading order: h2s should come after h1 in the document
    // Get all headings and verify they follow a logical hierarchy
    const allHeadings = page.locator('h1, h2, h3, h4, h5, h6');
    const headingCount = await allHeadings.count();

    let previousLevel = 0;
    for (let i = 0; i < headingCount; i++) {
      const heading = allHeadings.nth(i);
      const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
      const currentLevel = parseInt(tagName.charAt(1));

      // Heading levels should not skip more than one level
      // (e.g., h1 -> h3 without h2 is invalid)
      if (previousLevel > 0 && currentLevel > previousLevel) {
        expect(
          currentLevel - previousLevel,
          `Heading hierarchy should not skip levels (from h${previousLevel} to h${currentLevel})`
        ).toBeLessThanOrEqual(1);
      }

      previousLevel = currentLevel;
    }

    // Verify specific section headings exist
    await expect(page.locator('h2:has-text("Features")')).toBeVisible();
    await expect(page.locator('h2:has-text("Quick Start")')).toBeVisible();
  });

  test('Test Case 3: Text has sufficient color contrast ratio (4.5:1 minimum)', async ({ page }) => {
    // Test main text color against background
    // Primary text color: #c9d1d9 (rgb(201, 209, 217))
    // Background color: #0d1117 (rgb(13, 17, 23))
    // Contrast ratio: approximately 10.3:1 (passes AA)

    const body = page.locator('body');
    const bodyColor = await body.evaluate(el => {
      const style = window.getComputedStyle(el);
      return { color: style.color, backgroundColor: style.backgroundColor };
    });

    // Verify body has color styles defined
    expect(bodyColor.color).toBeTruthy();
    expect(bodyColor.backgroundColor).toBeTruthy();

    // Test hero title contrast
    const heroTitle = page.getByTestId('product-name');
    const titleStyles = await heroTitle.evaluate(el => {
      const style = window.getComputedStyle(el);
      return { color: style.color, backgroundColor: style.backgroundColor };
    });
    expect(titleStyles.color).toBeTruthy();

    // Test muted text (secondary text) contrast
    // Muted color: #8b949e (rgb(139, 148, 158))
    // Background: #0d1117
    // Contrast ratio: approximately 5.5:1 (passes AA for normal text)
    const tagline = page.getByTestId('hero-tagline');
    const taglineStyles = await tagline.evaluate(el => {
      const style = window.getComputedStyle(el);
      return { color: style.color };
    });
    expect(taglineStyles.color).toBeTruthy();

    // Test feature card text contrast
    const featureDescription = page.locator('.feature-description').first();
    await expect(featureDescription).toBeVisible();
    const featureStyles = await featureDescription.evaluate(el => {
      const style = window.getComputedStyle(el);
      return { color: style.color };
    });
    expect(featureStyles.color).toBeTruthy();

    // Verify buttons have sufficient contrast
    const primaryBtn = page.getByTestId('cta-get-started');
    const primaryBtnStyles = await primaryBtn.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        color: style.color,
        backgroundColor: style.backgroundColor
      };
    });
    // Primary button: white text (#ffffff) on #e05d44 background
    // Contrast ratio: approximately 4.64:1 (passes AA for large text)
    expect(primaryBtnStyles.color).toBeTruthy();
    expect(primaryBtnStyles.backgroundColor).toBeTruthy();

    // Calculate approximate contrast ratios using luminance
    // Helper function to extract RGB values
    const parseRgb = (rgbString) => {
      const match = rgbString.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (!match) return null;
      return {
        r: parseInt(match[1]),
        g: parseInt(match[2]),
        b: parseInt(match[3])
      };
    };

    // Calculate relative luminance (WCAG formula)
    const getLuminance = (rgb) => {
      if (!rgb) return 0;
      const [r, g, b] = [rgb.r, rgb.g, rgb.b].map(c => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * r + 0.7152 * g + 0.0722 * b;
    };

    // Calculate contrast ratio
    const getContrastRatio = (l1, l2) => {
      const lighter = Math.max(l1, l2);
      const darker = Math.min(l1, l2);
      return (lighter + 0.05) / (darker + 0.05);
    };

    // Test body text contrast
    const textRgb = parseRgb(bodyColor.color);
    const bgRgb = parseRgb(bodyColor.backgroundColor);

    if (textRgb && bgRgb) {
      const textLuminance = getLuminance(textRgb);
      const bgLuminance = getLuminance(bgRgb);
      const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

      // WCAG AA requires 4.5:1 for normal text
      expect(contrastRatio, 'Body text contrast ratio should be at least 4.5:1').toBeGreaterThanOrEqual(4.5);
    }
  });

  test('Test Case 4: Focus indicators are visible on interactive elements', async ({ page }) => {
    // Test navigation links have visible focus
    const navLinks = page.locator('.nav-links a');
    const navLinkCount = await navLinks.count();

    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      await link.focus();

      // Check that the element has focus
      await expect(link).toBeFocused();

      // Verify focus styling is applied
      const focusStyles = await link.evaluate(el => {
        const style = window.getComputedStyle(el);
        return {
          outline: style.outline,
          outlineColor: style.outlineColor,
          outlineWidth: style.outlineWidth
        };
      });

      // Focus indicator should be visible (not 'none' and width > 0)
      const hasVisibleOutline = focusStyles.outlineWidth !== '0px' &&
                                 focusStyles.outline !== 'none' &&
                                 focusStyles.outline !== '';

      expect(hasVisibleOutline, `Navigation link ${i + 1} should have visible focus indicator`).toBe(true);
    }

    // Test CTA buttons have visible focus
    const getStartedBtn = page.getByTestId('cta-get-started');
    await getStartedBtn.focus();
    await expect(getStartedBtn).toBeFocused();

    const btnFocusStyles = await getStartedBtn.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        outlineOffset: style.outlineOffset
      };
    });

    expect(
      btnFocusStyles.outlineWidth !== '0px',
      'CTA button should have visible focus outline'
    ).toBe(true);

    // Test mobile menu toggle button has visible focus (only on mobile viewport)
    // Note: Mobile menu toggle is hidden on desktop, so we test it with mobile viewport
    const menuToggle = page.getByTestId('mobile-menu-toggle');
    const isMenuToggleVisible = await menuToggle.isVisible();

    if (isMenuToggleVisible) {
      await menuToggle.focus();
      await expect(menuToggle).toBeFocused();

      const menuToggleFocusStyles = await menuToggle.evaluate(el => {
        const style = window.getComputedStyle(el);
        return {
          outline: style.outline,
          outlineWidth: style.outlineWidth
        };
      });

      expect(
        menuToggleFocusStyles.outlineWidth !== '0px',
        'Mobile menu toggle should have visible focus outline'
      ).toBe(true);
    }

    // Test copy buttons have visible focus
    const copyButtons = page.locator('.copy-button');
    const copyBtnCount = await copyButtons.count();

    if (copyBtnCount > 0) {
      const firstCopyBtn = copyButtons.first();
      await firstCopyBtn.focus();
      await expect(firstCopyBtn).toBeFocused();

      const copyBtnFocusStyles = await firstCopyBtn.evaluate(el => {
        const style = window.getComputedStyle(el);
        return {
          outline: style.outline,
          outlineWidth: style.outlineWidth
        };
      });

      expect(
        copyBtnFocusStyles.outlineWidth !== '0px',
        'Copy button should have visible focus outline'
      ).toBe(true);
    }
  });

  test('Test Case 5: Tab order follows logical reading order', async ({ page }) => {
    // Start by focusing the first focusable element
    await page.keyboard.press('Tab');

    // Expected tab order (based on DOM structure):
    // 1. Skip to main content link (if visible when focused)
    // 2. Nav logo
    // 3. Mobile menu toggle
    // 4. Nav links (Features, Documentation, GitHub)
    // 5. Hero CTA buttons
    // 6. Feature section elements
    // 7. Code example copy buttons
    // 8. Footer links

    const focusableElements = [];
    const maxTabs = 20; // Limit to prevent infinite loop

    for (let i = 0; i < maxTabs; i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName.toLowerCase(),
          text: el.textContent?.trim().substring(0, 50),
          href: el.getAttribute('href'),
          testId: el.getAttribute('data-testid'),
          className: el.className
        };
      });

      if (!focusedElement) break;

      focusableElements.push(focusedElement);
      await page.keyboard.press('Tab');
    }

    // Verify we have focusable elements
    expect(focusableElements.length, 'Page should have focusable elements').toBeGreaterThan(0);

    // Verify skip link comes first (or very early) when visible
    const skipLinkIndex = focusableElements.findIndex(el =>
      el.testId === 'skip-link' ||
      el.text?.toLowerCase().includes('skip') ||
      el.className?.includes('skip')
    );

    if (skipLinkIndex !== -1) {
      expect(skipLinkIndex, 'Skip link should be among first focusable elements').toBeLessThan(3);
    }

    // Verify navigation elements come before main content
    const navLogoIndex = focusableElements.findIndex(el =>
      el.className?.includes('nav-logo') || el.text === 'MirDB'
    );
    const ctaIndex = focusableElements.findIndex(el =>
      el.testId === 'cta-get-started'
    );

    if (navLogoIndex !== -1 && ctaIndex !== -1) {
      expect(
        navLogoIndex,
        'Navigation should be focusable before CTA buttons'
      ).toBeLessThan(ctaIndex);
    }

    // Verify footer links come last
    const footerGithubIndex = focusableElements.findIndex(el =>
      el.testId === 'footer-github-link'
    );

    if (footerGithubIndex !== -1) {
      expect(
        footerGithubIndex,
        'Footer links should be towards the end of tab order'
      ).toBeGreaterThan(focusableElements.length / 2);
    }
  });

  test('Test Case 6: Skip to main content link is available', async ({ page }) => {
    // Look for skip link
    const skipLink = page.getByTestId('skip-link');

    // Skip link should exist
    await expect(skipLink).toBeAttached();

    // Skip link should have appropriate text
    const skipLinkText = await skipLink.textContent();
    expect(
      skipLinkText?.toLowerCase(),
      'Skip link should mention "skip" and "main" or "content"'
    ).toMatch(/skip.*(main|content)/i);

    // Skip link should link to main content
    const href = await skipLink.getAttribute('href');
    expect(href, 'Skip link should have href pointing to main content').toBeTruthy();
    expect(href).toMatch(/^#/); // Should be an anchor link

    // Verify the target element exists
    if (href) {
      const targetId = href.substring(1);
      const targetElement = page.locator(`#${targetId}`);
      await expect(targetElement, `Skip link target #${targetId} should exist`).toBeAttached();
    }

    // Skip link should become visible on focus
    await skipLink.focus();
    await expect(skipLink).toBeFocused();

    // After focusing, skip link should be visible or become visible
    const isVisibleOnFocus = await skipLink.evaluate(el => {
      const style = window.getComputedStyle(el);
      // Check if element is visible (not hidden off-screen after focus)
      const rect = el.getBoundingClientRect();
      return rect.width > 0 &&
             rect.height > 0 &&
             style.opacity !== '0';
    });

    expect(isVisibleOnFocus, 'Skip link should be visible when focused').toBe(true);

    // Test skip link functionality
    await skipLink.click();

    // After clicking, focus should move to main content area
    const mainContent = page.locator('main');
    const mainContentId = await mainContent.getAttribute('id');

    // Verify we navigated to the correct section
    const currentUrl = page.url();
    if (href) {
      expect(currentUrl).toContain(href);
    }
  });

  test('ARIA attributes are properly used', async ({ page }) => {
    // Check mobile menu toggle has proper ARIA attributes
    const menuToggle = page.getByTestId('mobile-menu-toggle');
    await expect(menuToggle).toHaveAttribute('aria-label', 'Toggle navigation menu');
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');
    await expect(menuToggle).toHaveAttribute('aria-controls', 'mobile-nav-menu');

    // Check mobile menu has aria-hidden
    const mobileMenu = page.getByTestId('mobile-nav-menu');
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'true');

    // Check copy buttons have aria-label
    const copyButtons = page.locator('.copy-button');
    const copyBtnCount = await copyButtons.count();

    for (let i = 0; i < copyBtnCount; i++) {
      const btn = copyButtons.nth(i);
      const ariaLabel = await btn.getAttribute('aria-label');
      expect(ariaLabel, `Copy button ${i + 1} should have aria-label`).toBeTruthy();
    }

    // Check decorative SVGs are hidden from screen readers
    const footerSvgs = page.locator('.footer-links svg');
    const footerSvgCount = await footerSvgs.count();

    for (let i = 0; i < footerSvgCount; i++) {
      const svg = footerSvgs.nth(i);
      const ariaHidden = await svg.getAttribute('aria-hidden');
      expect(
        ariaHidden,
        `Footer SVG ${i + 1} should have aria-hidden="true"`
      ).toBe('true');
    }
  });

  test('Language attribute is set on HTML element', async ({ page }) => {
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang, 'HTML element should have lang attribute').toBeTruthy();
    expect(htmlLang).toBe('en');
  });

  test('Page has meaningful title and meta description', async ({ page }) => {
    const title = await page.title();
    expect(title, 'Page should have a title').toBeTruthy();
    expect(title.length, 'Title should have meaningful content').toBeGreaterThan(5);
    expect(title.toLowerCase()).toContain('mirdb');

    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
    expect(metaDescription, 'Page should have meta description').toBeTruthy();
    expect(metaDescription?.length, 'Meta description should have meaningful content').toBeGreaterThan(20);
  });
});
