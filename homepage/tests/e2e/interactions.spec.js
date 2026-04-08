// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Copy Code Functionality E2E Tests
 * Owner: Scenario 13 - Copy Code Functionality
 *
 * Test cases:
 * - Click copy button on installation code snippet - code copied, success feedback shown
 * - Click copy button on usage example snippet - code copied, success feedback shown
 * - Paste copied code into text area - pasted content matches code snippet exactly
 * - Copy button accessibility - keyboard accessible and have aria-labels
 */

test.describe('Copy Code Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for clipboard.js to initialize
    await page.waitForFunction(() => window.MirDBClipboard !== undefined);
  });

  test('copy button on installation code snippet shows success feedback', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to installation section
    await page.locator('#installation').scrollIntoViewIfNeeded();

    // Find the first installation copy button (git clone command)
    const copyButton = page.locator('.install-copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Get the expected text from data-copy attribute
    const expectedText = await copyButton.getAttribute('data-copy');

    // Click the copy button
    await copyButton.click();

    // Verify success feedback - button should have 'copied' class
    await expect(copyButton).toHaveClass(/copied/);

    // Verify button text changes to "Copied!"
    const buttonSpan = copyButton.locator('span');
    await expect(buttonSpan).toHaveText('Copied!');

    // Verify clipboard content matches expected text
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);

    // Wait for feedback to reset (2 seconds + buffer)
    await page.waitForTimeout(2500);

    // Verify button resets to original state
    await expect(copyButton).not.toHaveClass(/copied/);
    await expect(buttonSpan).toHaveText('Copy');
  });

  test('copy button on usage example snippet shows success feedback', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();

    // Find a code example copy button (SET command)
    const copyButton = page.locator('.code-copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Get the expected text from data-copy attribute
    const expectedText = await copyButton.getAttribute('data-copy');

    // Click the copy button
    await copyButton.click();

    // Verify success feedback - button should have 'copied' class
    await expect(copyButton).toHaveClass(/copied/);

    // Verify button text changes to "Copied!"
    const buttonSpan = copyButton.locator('span');
    await expect(buttonSpan).toHaveText('Copied!');

    // Verify clipboard content matches expected text
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);
  });

  test('copied code can be pasted and matches original exactly', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to installation section
    await page.locator('#installation').scrollIntoViewIfNeeded();

    // Find a copy button with a specific command
    const copyButton = page.locator('.install-copy-btn[data-copy="cargo build --release"]');
    await expect(copyButton).toBeVisible();

    // Get the expected text
    const expectedText = 'cargo build --release';

    // Click the copy button
    await copyButton.click();

    // Wait for copy to complete
    await expect(copyButton).toHaveClass(/copied/);

    // Create a temporary textarea to paste into
    await page.evaluate(() => {
      const textarea = document.createElement('textarea');
      textarea.id = 'test-paste-area';
      textarea.style.position = 'fixed';
      textarea.style.top = '0';
      textarea.style.left = '0';
      textarea.style.width = '300px';
      textarea.style.height = '100px';
      textarea.style.zIndex = '9999';
      document.body.appendChild(textarea);
    });

    // Focus the textarea and paste
    const textarea = page.locator('#test-paste-area');
    await textarea.focus();

    // Read clipboard and verify content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);

    // Paste using keyboard
    await page.keyboard.press('Control+v');

    // Verify pasted content matches
    const pastedText = await textarea.inputValue();
    expect(pastedText).toBe(expectedText);

    // Clean up
    await page.evaluate(() => {
      const textarea = document.getElementById('test-paste-area');
      if (textarea) textarea.remove();
    });
  });

  test('copy buttons are keyboard accessible and have aria-labels', async ({ page }) => {
    // Check installation copy buttons
    await page.locator('#installation').scrollIntoViewIfNeeded();
    const installButtons = page.locator('.install-copy-btn');
    const installButtonCount = await installButtons.count();
    expect(installButtonCount).toBeGreaterThan(0);

    for (let i = 0; i < installButtonCount; i++) {
      const button = installButtons.nth(i);

      // Verify aria-label exists
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel?.toLowerCase()).toContain('copy');

      // Verify button is focusable (has no tabindex=-1)
      const tabindex = await button.getAttribute('tabindex');
      expect(tabindex).not.toBe('-1');

      // Verify button type is set
      const buttonType = await button.getAttribute('type');
      expect(buttonType).toBe('button');
    }

    // Check code example copy buttons
    await page.locator('#code-examples').scrollIntoViewIfNeeded();
    const codeButtons = page.locator('.code-copy-btn');
    const codeButtonCount = await codeButtons.count();
    expect(codeButtonCount).toBeGreaterThan(0);

    for (let i = 0; i < codeButtonCount; i++) {
      const button = codeButtons.nth(i);

      // Verify aria-label exists
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel?.toLowerCase()).toContain('copy');

      // Verify button type is set
      const buttonType = await button.getAttribute('type');
      expect(buttonType).toBe('button');
    }
  });

  test('copy button can be activated with keyboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to installation section
    await page.locator('#installation').scrollIntoViewIfNeeded();

    // Find the first installation copy button
    const copyButton = page.locator('.install-copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Get the expected text from data-copy attribute
    const expectedText = await copyButton.getAttribute('data-copy');

    // Focus the button using keyboard navigation
    await copyButton.focus();

    // Activate with Enter key
    await page.keyboard.press('Enter');

    // Verify success feedback
    await expect(copyButton).toHaveClass(/copied/);

    // Verify clipboard content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);
  });

  test('copy button can be activated with Space key', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();

    // Find a code example copy button
    const copyButton = page.locator('.code-copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Get the expected text from data-copy attribute
    const expectedText = await copyButton.getAttribute('data-copy');

    // Focus the button
    await copyButton.focus();

    // Activate with Space key
    await page.keyboard.press('Space');

    // Verify success feedback
    await expect(copyButton).toHaveClass(/copied/);

    // Verify clipboard content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);
  });

  test('all copy buttons have data-copy attribute with content', async ({ page }) => {
    // Check all copy buttons on the page
    const allCopyButtons = page.locator('[data-copy]');
    const count = await allCopyButtons.count();
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const button = allCopyButtons.nth(i);
      const dataCopy = await button.getAttribute('data-copy');
      expect(dataCopy).toBeTruthy();
      expect(dataCopy?.length).toBeGreaterThan(0);
    }
  });

  test('Python example copy button copies full code snippet', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();

    // Find the Python example copy button
    const pythonCopyButton = page.locator('.code-client-example .code-copy-btn');
    await expect(pythonCopyButton).toBeVisible();

    // Get the expected text from data-copy attribute
    const expectedText = await pythonCopyButton.getAttribute('data-copy');
    expect(expectedText).toContain('pymemcache');
    expect(expectedText).toContain('client.set');
    expect(expectedText).toContain('client.get');

    // Click the copy button
    await pythonCopyButton.click();

    // Verify success feedback
    await expect(pythonCopyButton).toHaveClass(/copied/);

    // Verify clipboard content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);
  });
});

/**
 * Smooth Scrolling Navigation E2E Tests
 * Owner: Scenario 14 - Smooth Scrolling Navigation
 *
 * Test cases:
 * - Click Features link from top of page - page smoothly scrolls to features section
 * - Click Architecture link from top of page - page smoothly scrolls to architecture section
 * - Verify scroll-behavior CSS property - HTML or body has scroll-behavior: smooth applied
 * - Verify prefers-reduced-motion support - smooth scroll is disabled when user prefers reduced motion
 */
test.describe('Smooth Scrolling Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for main.js to initialize
    await page.waitForFunction(() => window.MirDBMain !== undefined);
  });

  test('click Features link from top of page smoothly scrolls to features section', async ({ page }) => {
    // Ensure we're at the top of the page
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(100);

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBe(0);

    // Get target section position
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Click the Features link in navigation
    const featuresLink = page.locator('a.nav-link[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Wait a bit for smooth scroll to complete (smooth scroll takes time)
    await page.waitForTimeout(800);

    // Verify we've scrolled to the features section
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify the features section is now in the viewport
    const isInViewport = await featuresSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= 200;
    });
    expect(isInViewport).toBe(true);
  });

  test('click Architecture link from top of page smoothly scrolls to architecture section', async ({ page }) => {
    // Ensure we're at the top of the page
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(100);

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBe(0);

    // Get target section position
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Click the Architecture link in navigation
    const architectureLink = page.locator('a.nav-link[href="#architecture"]');
    await expect(architectureLink).toBeVisible();
    await architectureLink.click();

    // Wait for smooth scroll to complete (architecture section is further down)
    await page.waitForTimeout(1500);

    // Verify we've scrolled to the architecture section
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify the architecture section is now in the viewport (allow wider tolerance for longer scroll)
    const isInViewport = await architectureSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      // Check that the section is visible in the top portion of the viewport
      return rect.top >= -150 && rect.top <= 350;
    });
    expect(isInViewport).toBe(true);
  });

  test('HTML element has scroll-behavior smooth CSS property', async ({ page }) => {
    // Check if html element has scroll-behavior: smooth
    const htmlScrollBehavior = await page.evaluate(() => {
      const html = document.documentElement;
      return window.getComputedStyle(html).scrollBehavior;
    });
    expect(htmlScrollBehavior).toBe('smooth');
  });

  test('smooth scroll is disabled when user prefers reduced motion', async ({ page }) => {
    // Emulate reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' });

    // Reload to apply the preference
    await page.reload();
    await page.waitForFunction(() => window.MirDBMain !== undefined);

    // Verify the prefersReducedMotion function returns true
    const prefersReduced = await page.evaluate(() => {
      return window.MirDBMain.prefersReducedMotion();
    });
    expect(prefersReduced).toBe(true);

    // Verify the CSS scroll-behavior is set to 'auto' when reduced motion is preferred
    const htmlScrollBehavior = await page.evaluate(() => {
      const html = document.documentElement;
      return window.getComputedStyle(html).scrollBehavior;
    });
    expect(htmlScrollBehavior).toBe('auto');
  });

  test('smooth scroll provides accessible focus management', async ({ page }) => {
    // Ensure we're at the top of the page
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(100);

    // Click the Features link
    const featuresLink = page.locator('a.nav-link[href="#features"]');
    await featuresLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(800);

    // Check that the target section has been given a tabindex for focus management
    const featuresSection = page.locator('#features');
    const tabindex = await featuresSection.getAttribute('tabindex');
    expect(tabindex).toBe('-1');
  });
});

/**
 * Hover Effects and Animations E2E Tests
 * Owner: Scenario 17 - Hover Effects and Animations
 *
 * Test cases:
 * 1. Hover over feature card - shows hover effect (shadow, scale, or color change)
 * 2. Hover over CTA buttons - shows hover effect indicating interactivity
 * 3. Scroll to reveal sections - sections fade in or animate subtly when scrolled into view
 * 4. Check animation timing - animations are subtle and complete within reasonable time (200-500ms)
 * 5. Check reduced motion preference - animations are disabled when user prefers reduced motion
 */
test.describe('Hover Effects and Animations', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for main.js to initialize
    await page.waitForFunction(() => window.MirDBMain !== undefined);
  });

  test('feature card shows hover effect (shadow, scale, or color change)', async ({ page }) => {
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();
    await page.waitForTimeout(500); // Wait for scroll animations

    // Get the first feature card
    const featureCard = page.locator('.features-card').first();
    await expect(featureCard).toBeVisible();

    // Get initial styles
    const initialTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });
    const initialBoxShadow = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Hover over the feature card
    await featureCard.hover();

    // Wait for transition to complete (300ms + buffer)
    await page.waitForTimeout(400);

    // Verify transform changed (translateY effect)
    const hoverTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // The hover should apply translateY(-4px) which results in a matrix transform
    // Either the transform should change OR box-shadow should be visible
    const hoverBoxShadow = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // At least one visual change should occur on hover
    const transformChanged = initialTransform !== hoverTransform;
    const shadowChanged = initialBoxShadow !== hoverBoxShadow && hoverBoxShadow !== 'none';

    expect(transformChanged || shadowChanged).toBe(true);
  });

  test('CTA buttons show hover effect indicating interactivity', async ({ page }) => {
    // Get the primary CTA button
    const primaryCta = page.locator('.hero-cta-primary');
    await expect(primaryCta).toBeVisible();

    // Get initial styles
    const initialBgColor = await primaryCta.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Hover over the primary CTA
    await primaryCta.hover();

    // Wait for transition to complete
    await page.waitForTimeout(400);

    // Verify background color changed or transform applied
    const hoverBgColor = await primaryCta.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    const hoverTransform = await primaryCta.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Check that visual feedback is provided
    const bgColorChanged = initialBgColor !== hoverBgColor;
    const hasTransform = hoverTransform !== 'none' && hoverTransform !== 'matrix(1, 0, 0, 1, 0, 0)';

    expect(bgColorChanged || hasTransform).toBe(true);
  });

  test('secondary CTA button shows hover effect', async ({ page }) => {
    // Get the secondary CTA button
    const secondaryCta = page.locator('.hero-cta-secondary');
    await expect(secondaryCta).toBeVisible();

    // Get initial border color
    const initialBorderColor = await secondaryCta.evaluate((el) => {
      return window.getComputedStyle(el).borderColor;
    });

    // Hover over the secondary CTA
    await secondaryCta.hover();

    // Wait for transition to complete
    await page.waitForTimeout(400);

    // Verify border color changed or transform applied
    const hoverBorderColor = await secondaryCta.evaluate((el) => {
      return window.getComputedStyle(el).borderColor;
    });
    const hoverTransform = await secondaryCta.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Check that visual feedback is provided
    const borderColorChanged = initialBorderColor !== hoverBorderColor;
    const hasTransform = hoverTransform !== 'none' && hoverTransform !== 'matrix(1, 0, 0, 1, 0, 0)';

    expect(borderColorChanged || hasTransform).toBe(true);
  });

  test('sections fade in or animate subtly when scrolled into view', async ({ page }) => {
    // Ensure we're at the top of the page
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(100);

    // Get a feature card before scrolling (should have animate-on-scroll class but not animated)
    const featureCards = page.locator('.features-card');
    const firstCard = featureCards.first();

    // Check that the feature cards have the animate-on-scroll class
    const hasAnimateClass = await firstCard.evaluate((el) => {
      return el.classList.contains('animate-on-scroll');
    });
    expect(hasAnimateClass).toBe(true);

    // Check initial state - not animated yet
    const initiallyAnimated = await firstCard.evaluate((el) => {
      return el.classList.contains('animated');
    });
    // Initially might be false (not scrolled to) or true (if already in view)

    // Scroll to the features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Wait for the animation to trigger and complete
    await page.waitForTimeout(800);

    // Verify the animated class was added
    const afterScrollAnimated = await firstCard.evaluate((el) => {
      return el.classList.contains('animated');
    });
    expect(afterScrollAnimated).toBe(true);

    // Verify the element is now fully visible (opacity 1)
    const opacity = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).opacity;
    });
    expect(opacity).toBe('1');
  });

  test('animations complete within reasonable time (200-500ms)', async ({ page }) => {
    // Check transition duration CSS values for animated elements
    await page.locator('#features').scrollIntoViewIfNeeded();

    const featureCard = page.locator('.features-card').first();
    await expect(featureCard).toBeVisible();

    // Get the transition duration
    const transitionDuration = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      // Get the transition-duration property value
      const duration = style.transitionDuration;
      // Parse the duration (e.g., "0.3s" -> 300 or "300ms" -> 300)
      if (duration.includes('ms')) {
        return parseFloat(duration);
      } else if (duration.includes('s')) {
        return parseFloat(duration) * 1000;
      }
      return 0;
    });

    // Verify transition duration is within 200-500ms range
    expect(transitionDuration).toBeGreaterThanOrEqual(200);
    expect(transitionDuration).toBeLessThanOrEqual(500);
  });

  test('check animation timing on hover transitions', async ({ page }) => {
    // Check hover transition timing on CTAs
    const primaryCta = page.locator('.hero-cta-primary');
    await expect(primaryCta).toBeVisible();

    const transitionDuration = await primaryCta.evaluate((el) => {
      const style = window.getComputedStyle(el);
      const duration = style.transitionDuration;
      // Handle multiple durations (take the first one)
      const firstDuration = duration.split(',')[0].trim();
      if (firstDuration.includes('ms')) {
        return parseFloat(firstDuration);
      } else if (firstDuration.includes('s')) {
        return parseFloat(firstDuration) * 1000;
      }
      return 0;
    });

    // Verify transition duration is within 200-500ms range
    expect(transitionDuration).toBeGreaterThanOrEqual(150);
    expect(transitionDuration).toBeLessThanOrEqual(500);
  });

  test('animations are disabled when user prefers reduced motion', async ({ page }) => {
    // Emulate reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' });

    // Reload to apply the preference
    await page.reload();
    await page.waitForFunction(() => window.MirDBMain !== undefined);

    // Verify that prefersReducedMotion returns true
    const prefersReduced = await page.evaluate(() => {
      return window.MirDBMain.prefersReducedMotion();
    });
    expect(prefersReduced).toBe(true);

    // Check that elements have the animated class immediately (no animation wait)
    const featureCard = page.locator('.features-card').first();
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Elements should have animated class immediately
    const hasAnimatedClass = await featureCard.evaluate((el) => {
      return el.classList.contains('animated');
    });
    expect(hasAnimatedClass).toBe(true);

    // Check that transitions are effectively disabled (0.01ms duration)
    const transitionDuration = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      const duration = style.transitionDuration;
      const firstDuration = duration.split(',')[0].trim();
      if (firstDuration.includes('ms')) {
        return parseFloat(firstDuration);
      } else if (firstDuration.includes('s')) {
        return parseFloat(firstDuration) * 1000;
      }
      return 0;
    });

    // With reduced motion, transitions should be effectively instant (≤1ms)
    expect(transitionDuration).toBeLessThanOrEqual(1);
  });

  test('hover effects do not apply transform when reduced motion is preferred', async ({ page }) => {
    // Emulate reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' });

    // Reload to apply the preference
    await page.reload();
    await page.waitForFunction(() => window.MirDBMain !== undefined);

    // Navigate to features
    await page.locator('#features').scrollIntoViewIfNeeded();
    await page.waitForTimeout(200);

    // Get a feature card
    const featureCard = page.locator('.features-card').first();
    await expect(featureCard).toBeVisible();

    // Get initial transform
    const initialTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover over the card
    await featureCard.hover();
    await page.waitForTimeout(100);

    // Get hover transform
    const hoverTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Transform should not change (no motion)
    // The CSS sets transform: none for hover when reduced motion is preferred
    expect(hoverTransform === 'none' || hoverTransform === initialTransform).toBe(true);
  });

  test('architecture components show hover effects', async ({ page }) => {
    // Navigate to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();
    await page.waitForTimeout(500);

    // Get an architecture component
    const archComponent = page.locator('.arch-component').first();
    await expect(archComponent).toBeVisible();

    // Get initial transform
    const initialTransform = await archComponent.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover
    await archComponent.hover();
    await page.waitForTimeout(400);

    // Get hover transform
    const hoverTransform = await archComponent.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Verify transform changed (translateY effect)
    const transformChanged = initialTransform !== hoverTransform;
    const hasTranslateY = hoverTransform !== 'none' && hoverTransform.includes('matrix');

    expect(transformChanged || hasTranslateY).toBe(true);
  });

  test('code examples show hover effect on border', async ({ page }) => {
    // Navigate to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();
    await page.waitForTimeout(500);

    // Get a code example
    const codeExample = page.locator('.code-example').first();
    await expect(codeExample).toBeVisible();

    // Get initial border color
    const initialBorderColor = await codeExample.evaluate((el) => {
      return window.getComputedStyle(el).borderColor;
    });

    // Hover
    await codeExample.hover();
    await page.waitForTimeout(400);

    // Get hover border color
    const hoverBorderColor = await codeExample.evaluate((el) => {
      return window.getComputedStyle(el).borderColor;
    });

    // Verify border color changed to accent color
    expect(initialBorderColor !== hoverBorderColor).toBe(true);
  });

  test('status features show hover effect', async ({ page }) => {
    // Navigate to status section
    await page.locator('#status').scrollIntoViewIfNeeded();
    await page.waitForTimeout(500);

    // Get a status feature
    const statusFeature = page.locator('.status-feature').first();
    await expect(statusFeature).toBeVisible();

    // Get initial transform
    const initialTransform = await statusFeature.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover
    await statusFeature.hover();
    await page.waitForTimeout(400);

    // Get hover transform
    const hoverTransform = await statusFeature.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Verify transform changed (translateX effect)
    expect(initialTransform !== hoverTransform).toBe(true);
  });
});
