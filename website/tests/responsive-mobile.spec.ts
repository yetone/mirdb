import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Responsive Design - Mobile
 * Scenario: Verify that the website displays correctly and is fully functional on mobile devices
 * Tests run at 375px width (iPhone SE size) to validate mobile responsiveness
 */

// Configure mobile viewport (375px width x 667px height - iPhone SE dimensions)
// Using chromium instead of webkit for compatibility
test.use({
  viewport: { width: 375, height: 667 },
  isMobile: true,
  hasTouch: true,
});

test.describe('Responsive Design - Mobile', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Load homepage at 375px width
   * Input: Load homepage at 375px width
   * Expected: Page loads without horizontal scroll, content fits viewport width
   */
  test('should load homepage without horizontal scroll at 375px width', async ({ page }) => {
    // Verify viewport is set to mobile width (375px for iPhone SE)
    const viewportSize = page.viewportSize();
    expect(viewportSize?.width).toBe(375);

    // Check that there is no horizontal scrollbar by verifying document width matches viewport
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify the main content container fits within viewport
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyWidth).toBeLessThanOrEqual(375);

    // Verify page loads successfully with main elements visible
    const mainContent = page.locator('main');
    await expect(mainContent).toBeVisible();
  });

  /**
   * Test Case 2: Check hero section on mobile
   * Input: Check hero section on mobile
   * Expected: Hero text is readable, CTA buttons are full-width or appropriately sized
   */
  test('should display hero section properly on mobile with readable text and appropriately sized CTA buttons', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify headline is visible and readable
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();

    // Check headline font size is appropriate for mobile (at least 20px)
    const headlineFontSize = await headline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(headlineFontSize).toBeGreaterThanOrEqual(20);

    // Verify subheadline is visible
    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible();

    // Verify CTA buttons are visible and appropriately sized
    const getStartedButton = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedButton).toBeVisible();

    const githubButton = page.locator('[data-testid="cta-github"]');
    await expect(githubButton).toBeVisible();

    // Check that CTA buttons have appropriate touch target size (at least 44px height)
    const getStartedBoundingBox = await getStartedButton.boundingBox();
    expect(getStartedBoundingBox).toBeTruthy();
    expect(getStartedBoundingBox!.height).toBeGreaterThanOrEqual(44);

    const githubBoundingBox = await githubButton.boundingBox();
    expect(githubBoundingBox).toBeTruthy();
    expect(githubBoundingBox!.height).toBeGreaterThanOrEqual(44);

    // Verify buttons fit within viewport width (with some margin)
    expect(getStartedBoundingBox!.width).toBeLessThanOrEqual(375 - 32); // Account for padding
    expect(githubBoundingBox!.width).toBeLessThanOrEqual(375 - 32);
  });

  /**
   * Test Case 3: Check features section on mobile
   * Input: Check features section on mobile
   * Expected: Feature cards stack vertically, each card is fully visible
   */
  test('should display features section with vertically stacked cards on mobile', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify features grid exists
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('[data-testid="features-grid"] > div');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Verify cards are stacked vertically by checking their positions
    const cardBoundingBoxes = [];
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();
      const boundingBox = await card.boundingBox();
      expect(boundingBox).toBeTruthy();
      cardBoundingBoxes.push(boundingBox!);
    }

    // On mobile (375px), cards should be stacked vertically
    // Check that each subsequent card is below the previous one (vertical stacking)
    // At mobile width, grid should collapse to single column
    for (let i = 1; i < cardBoundingBoxes.length; i++) {
      const currentCard = cardBoundingBoxes[i];
      const previousCard = cardBoundingBoxes[i - 1];
      // Current card should start below or at the same horizontal position as previous
      // (accounting for single column layout)
      expect(currentCard.y).toBeGreaterThanOrEqual(previousCard.y);
    }

    // Verify each card fits within the viewport width
    for (const box of cardBoundingBoxes) {
      expect(box.width).toBeLessThanOrEqual(375 - 16); // Account for container padding
      expect(box.x).toBeGreaterThanOrEqual(0);
      expect(box.x + box.width).toBeLessThanOrEqual(375 + 10); // Small tolerance
    }

    // Verify specific feature cards are visible
    const memcachedFeature = page.locator('[data-testid="feature-memcached"]');
    await memcachedFeature.scrollIntoViewIfNeeded();
    await expect(memcachedFeature).toBeVisible();

    const persistenceFeature = page.locator('[data-testid="feature-persistence"]');
    await persistenceFeature.scrollIntoViewIfNeeded();
    await expect(persistenceFeature).toBeVisible();
  });

  /**
   * Test Case 4: Check code blocks on mobile
   * Input: Check code blocks on mobile
   * Expected: Code blocks have horizontal scroll if needed, text is readable
   */
  test('should display code blocks with horizontal scroll capability and readable text on mobile', async ({ page }) => {
    // Scroll to getting started section which contains code blocks
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Check startup code block
    const startupCodeBlock = page.locator('[data-testid="startup-code-block"]');
    await startupCodeBlock.scrollIntoViewIfNeeded();
    await expect(startupCodeBlock).toBeVisible();

    // Verify code block has overflow-x set to auto or scroll for horizontal scrolling
    const startupOverflow = await startupCodeBlock.locator('pre').evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(['auto', 'scroll']).toContain(startupOverflow);

    // Check usage code block
    const usageCodeBlock = page.locator('[data-testid="usage-code-block"]');
    await usageCodeBlock.scrollIntoViewIfNeeded();
    await expect(usageCodeBlock).toBeVisible();

    // Verify usage code block has overflow-x property
    const usageOverflow = await usageCodeBlock.locator('pre').evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(['auto', 'scroll']).toContain(usageOverflow);

    // Check that code text is readable (font size at least 12px)
    const codeElements = page.locator('[data-testid="getting-started-section"] code');
    const codeCount = await codeElements.count();
    expect(codeCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(codeCount, 2); i++) {
      const codeElement = codeElements.nth(i);
      const fontSize = await codeElement.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Code should be readable (at least 12px, typical minimum for code)
      expect(fontSize).toBeGreaterThanOrEqual(12);
    }

    // Verify code blocks don't overflow the viewport
    const codeBlockBoundingBox = await startupCodeBlock.boundingBox();
    expect(codeBlockBoundingBox).toBeTruthy();
    expect(codeBlockBoundingBox!.width).toBeLessThanOrEqual(375 + 10); // Small tolerance
  });

  /**
   * Test Case 5: Verify font sizes on mobile
   * Input: Verify font sizes on mobile
   * Expected: Body text is at least 16px, headings are proportionally sized
   */
  test('should have appropriate font sizes on mobile with body text at least 16px', async ({ page }) => {
    // Check body text in hero subheadline (should be at least 16px for readability)
    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible();

    const subheadlineFontSize = await subheadline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Body/description text should be at least 16px for mobile readability
    expect(subheadlineFontSize).toBeGreaterThanOrEqual(16);

    // Check main headline (H1) is larger than body text
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();

    const headlineFontSize = await headline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Headline should be larger than body text
    expect(headlineFontSize).toBeGreaterThan(subheadlineFontSize);

    // Scroll to features section and check feature descriptions
    const featuresSection = page.locator('[data-testid="features-section"]');
    await featuresSection.scrollIntoViewIfNeeded();

    const featureDescription = page.locator('[data-testid="feature-memcached-description"]');
    await expect(featureDescription).toBeVisible();

    const featureDescFontSize = await featureDescription.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Feature descriptions should be at least 14px (readable on mobile)
    expect(featureDescFontSize).toBeGreaterThanOrEqual(14);

    // Check section headings
    const gettingStartedHeading = page.locator('[data-testid="getting-started-heading"]');
    await gettingStartedHeading.scrollIntoViewIfNeeded();
    await expect(gettingStartedHeading).toBeVisible();

    const sectionHeadingFontSize = await gettingStartedHeading.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Section headings should be proportionally larger
    expect(sectionHeadingFontSize).toBeGreaterThanOrEqual(24);

    // Verify paragraph text in content sections is readable
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    const paragraphs = gettingStartedSection.locator('p');
    const paragraphCount = await paragraphs.count();

    if (paragraphCount > 0) {
      const firstParagraph = paragraphs.first();
      const paragraphFontSize = await firstParagraph.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Paragraph text should be at least 14px for mobile readability
      expect(paragraphFontSize).toBeGreaterThanOrEqual(14);
    }
  });

  /**
   * Additional Test: Full page scroll verification
   * Verify all sections render correctly when scrolling through entire page
   */
  test('should render all sections correctly when scrolling through entire page', async ({ page }) => {
    // Verify hero section at top
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Scroll to and verify features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Scroll to and verify getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Scroll to and verify commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Scroll to and verify configuration section
    const configurationSection = page.locator('[data-testid="configuration-section"]');
    await configurationSection.scrollIntoViewIfNeeded();
    await expect(configurationSection).toBeVisible();

    // Final check: scroll back to top and verify hero is still accessible
    await page.evaluate(() => window.scrollTo(0, 0));
    await expect(heroSection).toBeVisible();
  });

  /**
   * Additional Test: Touch targets have appropriate size
   * Verify interactive elements have minimum 44x44px touch targets
   */
  test('should have appropriately sized touch targets for interactive elements', async ({ page }) => {
    // Check CTA buttons in hero section
    const getStartedButton = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedButton).toBeVisible();

    const getStartedBox = await getStartedButton.boundingBox();
    expect(getStartedBox).toBeTruthy();
    // Touch targets should be at least 44x44px per iOS/Android guidelines
    expect(getStartedBox!.height).toBeGreaterThanOrEqual(44);
    expect(getStartedBox!.width).toBeGreaterThanOrEqual(44);

    const githubButton = page.locator('[data-testid="cta-github"]');
    await expect(githubButton).toBeVisible();

    const githubBox = await githubButton.boundingBox();
    expect(githubBox).toBeTruthy();
    expect(githubBox!.height).toBeGreaterThanOrEqual(44);
    expect(githubBox!.width).toBeGreaterThanOrEqual(44);

    // Check command section links/buttons
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await commandsSection.scrollIntoViewIfNeeded();

    // Verify command cards have sufficient tap targets
    const setCommand = page.locator('[data-testid="command-set"]');
    await setCommand.scrollIntoViewIfNeeded();
    await expect(setCommand).toBeVisible();

    const setCommandBox = await setCommand.boundingBox();
    expect(setCommandBox).toBeTruthy();
    // Command cards should be easily tappable
    expect(setCommandBox!.height).toBeGreaterThanOrEqual(44);
  });
});
