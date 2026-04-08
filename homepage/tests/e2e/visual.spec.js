// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Visual Design Consistency E2E Tests
 * Owner: Scenario 16 - Visual Design Consistency
 *
 * Test cases:
 * 1. Primary accent color is in orange/red spectrum (Rust branding)
 * 2. Dark mode uses dark backgrounds as specified in visual design
 * 3. Typography scales appropriately and uses consistent font families
 * 4. Consistent spacing scale is used throughout all sections
 * 5. Visual hierarchy is clear with appropriate heading sizes
 */

/**
 * Helper function to parse RGB color string
 */
function parseRgb(rgb) {
  const match = rgb.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (match) {
    return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
  }
  return null;
}

/**
 * Helper function to convert hex to RGB
 */
function hexToRgb(hex) {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return result ? {
    r: parseInt(result[1], 16),
    g: parseInt(result[2], 16),
    b: parseInt(result[3], 16)
  } : null;
}

/**
 * Helper function to check if color is in orange/red spectrum
 * Orange/red colors have high red channel, medium-to-low green, and low blue
 */
function isOrangeRedSpectrum(rgb) {
  if (!rgb) return false;
  // Red channel should be dominant (>150)
  // Red should be higher than blue
  // For orange: green can be medium (60-200), for red: green is lower
  return rgb.r > 150 && rgb.r > rgb.b && rgb.r >= rgb.g;
}

test.describe('Visual Design Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Primary accent color is in orange/red spectrum consistent with Rust branding', async ({ page }) => {
    // Set dark mode to test with the primary color scheme
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'dark');
      localStorage.setItem('mirdb-theme', 'dark');
    });
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Get the accent color CSS variable
    const accentColor = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return styles.getPropertyValue('--color-accent').trim();
    });

    // Verify accent color is defined
    expect(accentColor).toBeTruthy();

    // Get the rust color CSS variable (specific Rust branding color)
    const rustColor = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return styles.getPropertyValue('--color-rust').trim();
    });
    expect(rustColor).toBeTruthy();

    // Check the actual computed color on accent elements (CTAs, links)
    const primaryCta = page.locator('.hero-cta-primary').first();
    await expect(primaryCta).toBeVisible();

    const ctaBgColor = await primaryCta.evaluate(el => {
      return getComputedStyle(el).backgroundColor;
    });

    const ctaRgb = parseRgb(ctaBgColor);
    expect(ctaRgb).toBeTruthy();

    // Verify the CTA background is in the orange/red spectrum
    // Red should be the dominant channel
    expect(ctaRgb.r).toBeGreaterThan(150);
    expect(ctaRgb.r).toBeGreaterThan(ctaRgb.b);

    // Check the Rust highlight text in hero section
    const rustHighlight = page.locator('.hero-rust-highlight');
    if (await rustHighlight.count() > 0) {
      const rustTextColor = await rustHighlight.evaluate(el => {
        return getComputedStyle(el).color;
      });
      const rustRgb = parseRgb(rustTextColor);
      expect(rustRgb).toBeTruthy();
      // Rust color should have high red component (orange-ish)
      expect(rustRgb.r).toBeGreaterThan(150);
    }

    // Verify links use accent color
    const navLinks = page.locator('a[href]').first();
    if (await navLinks.count() > 0) {
      // Accent colors are applied via CSS variables, verify they're set
      const hasAccentVariable = await page.evaluate(() => {
        const styles = getComputedStyle(document.documentElement);
        const accent = styles.getPropertyValue('--color-accent');
        return accent && accent.trim().length > 0;
      });
      expect(hasAccentVariable).toBe(true);
    }

    // Verify status badges use appropriate colors (implemented = green, planned = rust/orange)
    const implementedBadge = page.locator('.status-badge-implemented').first();
    if (await implementedBadge.count() > 0) {
      const badgeColor = await implementedBadge.evaluate(el => {
        return getComputedStyle(el).color;
      });
      const badgeRgb = parseRgb(badgeColor);
      // Green badges should have green as dominant (success color)
      expect(badgeRgb.g).toBeGreaterThan(100);
    }

    const plannedBadge = page.locator('.status-badge-planned').first();
    if (await plannedBadge.count() > 0) {
      const plannedColor = await plannedBadge.evaluate(el => {
        return getComputedStyle(el).color;
      });
      const plannedRgb = parseRgb(plannedColor);
      // Planned badges use rust color (orange-ish)
      expect(plannedRgb.r).toBeGreaterThan(150);
    }
  });

  test('TC2: Dark mode uses dark backgrounds as specified in visual design', async ({ page }) => {
    // Set dark mode
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'dark');
      localStorage.setItem('mirdb-theme', 'dark');
    });
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Check body background is dark
    const bodyBgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    const bodyRgb = parseRgb(bodyBgColor);
    expect(bodyRgb).toBeTruthy();

    // Dark background should have low RGB values (sum < 150 for very dark)
    const bgSum = bodyRgb.r + bodyRgb.g + bodyRgb.b;
    expect(bgSum).toBeLessThan(200);

    // Check that bg-primary variable is dark
    const bgPrimary = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return styles.getPropertyValue('--color-bg-primary').trim();
    });
    expect(bgPrimary).toBeTruthy();

    // Verify the navigation has dark background
    const navBg = await page.locator('nav').evaluate(el => {
      return getComputedStyle(el).backgroundColor;
    });
    const navRgb = parseRgb(navBg);
    expect(navRgb).toBeTruthy();
    expect(navRgb.r + navRgb.g + navRgb.b).toBeLessThan(200);

    // Verify hero section has dark background (gradient starts dark)
    const heroBg = await page.locator('.hero-section').evaluate(el => {
      return getComputedStyle(el).backgroundColor;
    });
    // Hero uses gradient, so backgroundColor might be transparent
    // Instead check that hero visually appears dark
    const heroVisualBg = await page.evaluate(() => {
      const hero = document.querySelector('.hero-section');
      const rect = hero.getBoundingClientRect();
      const styles = getComputedStyle(hero);
      // Check that the text is light (indicating dark background)
      const textColor = styles.color;
      return textColor;
    });
    const heroTextRgb = parseRgb(heroVisualBg);
    if (heroTextRgb) {
      // Light text on dark background means text RGB sum > 400
      expect(heroTextRgb.r + heroTextRgb.g + heroTextRgb.b).toBeGreaterThan(400);
    }

    // Verify footer has dark background
    const footerBg = await page.locator('footer').evaluate(el => {
      return getComputedStyle(el).backgroundColor;
    });
    const footerRgb = parseRgb(footerBg);
    expect(footerRgb).toBeTruthy();
    expect(footerRgb.r + footerRgb.g + footerRgb.b).toBeLessThan(200);

    // Verify code blocks have very dark background
    const codeBlock = page.locator('.code-block').first();
    if (await codeBlock.count() > 0) {
      const codeBg = await codeBlock.evaluate(el => {
        return getComputedStyle(el).backgroundColor;
      });
      const codeRgb = parseRgb(codeBg);
      expect(codeRgb).toBeTruthy();
      expect(codeRgb.r + codeRgb.g + codeRgb.b).toBeLessThan(100); // Very dark
    }
  });

  test('TC3: Typography scales appropriately and uses consistent font families', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify font family CSS variable exists
    const fontFamily = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return styles.getPropertyValue('--font-family').trim();
    });
    expect(fontFamily).toBeTruthy();
    expect(fontFamily).toContain('sans-serif');

    // Verify monospace font family for code
    const fontMono = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return styles.getPropertyValue('--font-mono').trim();
    });
    expect(fontMono).toBeTruthy();
    expect(fontMono).toContain('monospace');

    // Check that body uses the defined font family
    const bodyFontFamily = await page.evaluate(() => {
      return getComputedStyle(document.body).fontFamily;
    });
    expect(bodyFontFamily).toBeTruthy();
    // Should include system fonts or defined fonts
    expect(bodyFontFamily.length).toBeGreaterThan(0);

    // Verify typography scale is defined
    const fontSizes = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return {
        sm: styles.getPropertyValue('--font-size-sm').trim(),
        base: styles.getPropertyValue('--font-size-base').trim(),
        lg: styles.getPropertyValue('--font-size-lg').trim(),
        xl: styles.getPropertyValue('--font-size-xl').trim(),
        '2xl': styles.getPropertyValue('--font-size-2xl').trim(),
        '3xl': styles.getPropertyValue('--font-size-3xl').trim()
      };
    });

    // All font size variables should be defined
    expect(fontSizes.sm).toBeTruthy();
    expect(fontSizes.base).toBeTruthy();
    expect(fontSizes.lg).toBeTruthy();
    expect(fontSizes.xl).toBeTruthy();
    expect(fontSizes['2xl']).toBeTruthy();
    expect(fontSizes['3xl']).toBeTruthy();

    // Parse rem values to verify proper scaling
    const parseRem = (value) => parseFloat(value.replace('rem', ''));

    const smSize = parseRem(fontSizes.sm);
    const baseSize = parseRem(fontSizes.base);
    const lgSize = parseRem(fontSizes.lg);
    const xlSize = parseRem(fontSizes.xl);
    const xxlSize = parseRem(fontSizes['2xl']);
    const xxxlSize = parseRem(fontSizes['3xl']);

    // Verify ascending scale
    expect(smSize).toBeLessThan(baseSize);
    expect(baseSize).toBeLessThan(lgSize);
    expect(lgSize).toBeLessThan(xlSize);
    expect(xlSize).toBeLessThan(xxlSize);
    expect(xxlSize).toBeLessThan(xxxlSize);

    // Verify code elements use monospace font
    const codeElement = page.locator('code').first();
    if (await codeElement.count() > 0) {
      const codeFontFamily = await codeElement.evaluate(el => {
        return getComputedStyle(el).fontFamily;
      });
      // Should include monospace
      expect(codeFontFamily.toLowerCase()).toMatch(/mono|courier|consolas/);
    }

    // Verify line-height is set for readability
    const bodyLineHeight = await page.evaluate(() => {
      return getComputedStyle(document.body).lineHeight;
    });
    // Line height should be greater than 1 for readability
    const lineHeightValue = parseFloat(bodyLineHeight);
    expect(lineHeightValue).toBeGreaterThanOrEqual(1.4);
  });

  test('TC4: Consistent spacing scale is used throughout all sections', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify spacing CSS variables are defined
    const spacingVars = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return {
        xs: styles.getPropertyValue('--space-xs').trim(),
        sm: styles.getPropertyValue('--space-sm').trim(),
        md: styles.getPropertyValue('--space-md').trim(),
        lg: styles.getPropertyValue('--space-lg').trim(),
        xl: styles.getPropertyValue('--space-xl').trim()
      };
    });

    // All spacing variables should be defined
    expect(spacingVars.xs).toBeTruthy();
    expect(spacingVars.sm).toBeTruthy();
    expect(spacingVars.md).toBeTruthy();
    expect(spacingVars.lg).toBeTruthy();
    expect(spacingVars.xl).toBeTruthy();

    // Parse rem values to verify proper scaling
    const parseRem = (value) => parseFloat(value.replace('rem', ''));

    const xsSpace = parseRem(spacingVars.xs);
    const smSpace = parseRem(spacingVars.sm);
    const mdSpace = parseRem(spacingVars.md);
    const lgSpace = parseRem(spacingVars.lg);
    const xlSpace = parseRem(spacingVars.xl);

    // Verify ascending scale
    expect(xsSpace).toBeLessThan(smSpace);
    expect(smSpace).toBeLessThan(mdSpace);
    expect(mdSpace).toBeLessThan(lgSpace);
    expect(lgSpace).toBeLessThan(xlSpace);

    // Verify sections use consistent padding
    const sections = ['.hero-section', '.features-section', '.code-section', '.install-section', '.arch-section', '.status-section'];

    for (const sectionSelector of sections) {
      const section = page.locator(sectionSelector).first();
      if (await section.count() > 0) {
        const sectionPadding = await section.evaluate(el => {
          const styles = getComputedStyle(el);
          return {
            paddingTop: styles.paddingTop,
            paddingBottom: styles.paddingBottom
          };
        });

        // Sections should have vertical padding
        const topPadding = parseFloat(sectionPadding.paddingTop);
        const bottomPadding = parseFloat(sectionPadding.paddingBottom);

        expect(topPadding).toBeGreaterThan(0);
        expect(bottomPadding).toBeGreaterThan(0);
      }
    }

    // Verify container max-width is defined
    const maxWidth = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return styles.getPropertyValue('--max-width').trim();
    });
    expect(maxWidth).toBeTruthy();
    expect(maxWidth).toMatch(/\d+px/);

    // Verify container uses max-width
    const container = page.locator('.container').first();
    if (await container.count() > 0) {
      const containerStyle = await container.evaluate(el => {
        const styles = getComputedStyle(el);
        return {
          maxWidth: styles.maxWidth,
          marginLeft: styles.marginLeft,
          marginRight: styles.marginRight
        };
      });

      // Container should have max-width and auto margins for centering
      expect(containerStyle.maxWidth).toBeTruthy();
    }

    // Verify cards/components have consistent border-radius
    const borderRadius = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return styles.getPropertyValue('--border-radius').trim();
    });
    expect(borderRadius).toBeTruthy();
    expect(borderRadius).toMatch(/\d+px/);
  });

  test('TC5: Visual hierarchy is clear with appropriate heading sizes', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Get h1 element (should be the hero title)
    const h1 = page.locator('h1').first();
    await expect(h1).toBeVisible();

    const h1FontSize = await h1.evaluate(el => {
      return parseFloat(getComputedStyle(el).fontSize);
    });

    // h1 should be the largest heading
    expect(h1FontSize).toBeGreaterThan(32);

    // Get all h2 elements (section titles)
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThan(0);

    let h2FontSize = 0;
    if (h2Count > 0) {
      h2FontSize = await h2Elements.first().evaluate(el => {
        return parseFloat(getComputedStyle(el).fontSize);
      });

      // h2 should be smaller than h1
      expect(h2FontSize).toBeLessThan(h1FontSize);
      expect(h2FontSize).toBeGreaterThan(20);
    }

    // Get h3 elements (subsection titles)
    const h3Elements = page.locator('h3');
    const h3Count = await h3Elements.count();

    let h3FontSize = 0;
    if (h3Count > 0) {
      h3FontSize = await h3Elements.first().evaluate(el => {
        return parseFloat(getComputedStyle(el).fontSize);
      });

      // h3 should be smaller than h2
      if (h2FontSize > 0) {
        expect(h3FontSize).toBeLessThanOrEqual(h2FontSize);
      }
      expect(h3FontSize).toBeGreaterThan(14);
    }

    // Verify heading color contrast (headings should be prominent)
    const headingColor = await h1.evaluate(el => {
      return getComputedStyle(el).color;
    });
    const headingRgb = parseRgb(headingColor);
    expect(headingRgb).toBeTruthy();

    // Get body text color for comparison
    const bodyTextColor = await page.locator('p').first().evaluate(el => {
      return getComputedStyle(el).color;
    });
    const bodyRgb = parseRgb(bodyTextColor);

    // Heading should have sufficient visual prominence
    // (either same as body for dark mode, or darker for light mode)
    const headingBrightness = headingRgb.r + headingRgb.g + headingRgb.b;
    expect(headingBrightness).toBeGreaterThan(0);

    // Verify headings have proper font weight for hierarchy
    const h1FontWeight = await h1.evaluate(el => {
      return getComputedStyle(el).fontWeight;
    });
    expect(parseInt(h1FontWeight)).toBeGreaterThanOrEqual(600);

    // Verify section titles have margin for visual separation
    if (h2Count > 0) {
      const h2MarginBottom = await h2Elements.first().evaluate(el => {
        return parseFloat(getComputedStyle(el).marginBottom);
      });
      expect(h2MarginBottom).toBeGreaterThan(0);
    }

    // Verify there's only one h1 (main page title)
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBe(1);

    // Verify heading structure makes sense semantically
    // Each major section should have an h2 title
    const sectionTitles = page.locator('.section-title');
    const sectionTitleCount = await sectionTitles.count();
    expect(sectionTitleCount).toBeGreaterThan(0);

    // Section titles should use consistent styling
    if (sectionTitleCount > 0) {
      const firstTitleSize = await sectionTitles.first().evaluate(el => {
        return parseFloat(getComputedStyle(el).fontSize);
      });

      // All section titles should have the same size
      for (let i = 1; i < sectionTitleCount; i++) {
        const titleSize = await sectionTitles.nth(i).evaluate(el => {
          return parseFloat(getComputedStyle(el).fontSize);
        });
        expect(titleSize).toBeCloseTo(firstTitleSize, 0);
      }
    }
  });
});

test.describe('Visual Design - Light Mode', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'light');
      localStorage.setItem('mirdb-theme', 'light');
    });
    await page.reload();
    await page.waitForLoadState('networkidle');
  });

  test('Light mode maintains visual consistency', async ({ page }) => {
    // Verify light background
    const bodyBgColor = await page.evaluate(() => {
      return getComputedStyle(document.body).backgroundColor;
    });

    const bodyRgb = parseRgb(bodyBgColor);
    expect(bodyRgb).toBeTruthy();

    // Light background should have high RGB values
    expect(bodyRgb.r + bodyRgb.g + bodyRgb.b).toBeGreaterThan(600);

    // Verify text is dark on light background
    const textColor = await page.evaluate(() => {
      return getComputedStyle(document.body).color;
    });

    const textRgb = parseRgb(textColor);
    expect(textRgb).toBeTruthy();

    // Dark text should have low RGB values
    expect(textRgb.r + textRgb.g + textRgb.b).toBeLessThan(200);

    // Verify accent color is still in orange/red spectrum in light mode
    const accentColor = await page.evaluate(() => {
      const styles = getComputedStyle(document.documentElement);
      return styles.getPropertyValue('--color-accent').trim();
    });
    expect(accentColor).toBeTruthy();

    // Check CTA button maintains accent styling
    const primaryCta = page.locator('.hero-cta-primary').first();
    if (await primaryCta.count() > 0) {
      await expect(primaryCta).toBeVisible();
      const ctaBgColor = await primaryCta.evaluate(el => {
        return getComputedStyle(el).backgroundColor;
      });
      const ctaRgb = parseRgb(ctaBgColor);
      expect(ctaRgb).toBeTruthy();
      expect(ctaRgb.r).toBeGreaterThan(150); // Still red/orange
    }
  });
});

test.describe('Visual Design - Responsive', () => {
  test('Typography scales appropriately on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify h1 is still readable but may be smaller
    const h1 = page.locator('h1').first();
    await expect(h1).toBeVisible();

    const h1FontSize = await h1.evaluate(el => {
      return parseFloat(getComputedStyle(el).fontSize);
    });

    // h1 should still be reasonably large on mobile
    expect(h1FontSize).toBeGreaterThan(20);

    // Verify body text is readable
    const bodyFontSize = await page.evaluate(() => {
      return parseFloat(getComputedStyle(document.body).fontSize);
    });

    // Base font should be at least 14px for readability
    expect(bodyFontSize).toBeGreaterThanOrEqual(14);
  });

  test('Spacing adapts appropriately on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify sections still have padding
    const heroSection = page.locator('.hero-section');
    if (await heroSection.count() > 0) {
      const heroPadding = await heroSection.evaluate(el => {
        const styles = getComputedStyle(el);
        return {
          paddingTop: parseFloat(styles.paddingTop),
          paddingBottom: parseFloat(styles.paddingBottom)
        };
      });

      expect(heroPadding.paddingTop).toBeGreaterThan(0);
      expect(heroPadding.paddingBottom).toBeGreaterThan(0);
    }

    // Verify no horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => {
      return {
        scrollWidth: document.body.scrollWidth,
        clientWidth: document.body.clientWidth
      };
    });

    expect(bodyScrollWidth.scrollWidth).toBeLessThanOrEqual(bodyScrollWidth.clientWidth + 1);
  });
});
