import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Tablet View (768px - 1024px)', () => {
  test.describe('Test Case 1: Homepage at 768px viewport width', () => {
    test('layout adapts to tablet view with appropriate grid columns at 768px', async ({ page }) => {
      // Set tablet viewport (iPad portrait equivalent) BEFORE navigating
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Verify main homepage sections are visible
      const heroSection = page.getByTestId('hero-section');
      const featuresSection = page.getByTestId('features-section');
      const howItWorksSection = page.locator('section#how-it-works');
      const footerCTA = page.getByTestId('footer-cta');

      await expect(heroSection).toBeVisible();
      await expect(featuresSection).toBeVisible();
      await expect(howItWorksSection).toBeVisible();
      await expect(footerCTA).toBeVisible();

      // Verify hero section displays correctly
      const heroHeadline = page.getByTestId('hero-headline');
      const heroSubheadline = page.getByTestId('hero-subheadline');
      await expect(heroHeadline).toBeVisible();
      await expect(heroSubheadline).toBeVisible();

      // Verify CTA buttons are visible
      const getStartedBtn = page.getByTestId('cta-get-started');
      const loginBtn = page.getByTestId('cta-login');
      await expect(getStartedBtn).toBeVisible();
      await expect(loginBtn).toBeVisible();

      // Verify no horizontal scrollbar (content fits within viewport)
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);
    });

    test('hero section text is readable and centered at 768px', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      const heroSection = page.getByTestId('hero-section');
      const heroHeadline = page.getByTestId('hero-headline');

      await expect(heroSection).toBeVisible();

      // Verify headline is visible and positioned correctly
      const headlineBox = await heroHeadline.boundingBox();
      expect(headlineBox).not.toBeNull();

      // Headline should be centered (approximately in middle of viewport)
      const centerX = headlineBox!.x + headlineBox!.width / 2;
      expect(centerX).toBeGreaterThan(300);
      expect(centerX).toBeLessThan(500);
    });
  });

  test.describe('Test Case 2: Homepage at 1024px viewport width', () => {
    test('layout displays correctly at tablet/desktop transition point (1024px)', async ({ page }) => {
      // Set viewport to upper tablet/small desktop boundary
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');

      // Verify main homepage sections are visible
      const heroSection = page.getByTestId('hero-section');
      const featuresSection = page.getByTestId('features-section');
      const howItWorksSection = page.locator('section#how-it-works');
      const footerCTA = page.getByTestId('footer-cta');

      await expect(heroSection).toBeVisible();
      await expect(featuresSection).toBeVisible();
      await expect(howItWorksSection).toBeVisible();
      await expect(footerCTA).toBeVisible();

      // Verify feature grid layout at 1024px - should show appropriate columns
      const featuresGrid = page.getByTestId('features-grid');
      await expect(featuresGrid).toBeVisible();

      // Get all feature cards
      const featureCards = page.getByTestId('glassmorphism-card');
      await expect(featureCards).toHaveCount(4);

      // Verify no horizontal scrollbar
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);
    });

    test('How It Works section shows horizontal layout at 1024px', async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');

      const howItWorksSection = page.locator('section#how-it-works');
      await expect(howItWorksSection).toBeVisible();

      // Verify all 3 steps are visible
      const step1 = page.getByTestId('step-1');
      const step2 = page.getByTestId('step-2');
      const step3 = page.getByTestId('step-3');

      await expect(step1).toBeVisible();
      await expect(step2).toBeVisible();
      await expect(step3).toBeVisible();

      // Verify steps are in horizontal layout (same y position)
      const step1Box = await step1.boundingBox();
      const step2Box = await step2.boundingBox();
      const step3Box = await step3.boundingBox();

      expect(step1Box).not.toBeNull();
      expect(step2Box).not.toBeNull();
      expect(step3Box).not.toBeNull();

      // At 1024px (md breakpoint), steps should be in a horizontal row
      const yTolerance = 50;
      expect(Math.abs(step1Box!.y - step2Box!.y)).toBeLessThan(yTolerance);
      expect(Math.abs(step2Box!.y - step3Box!.y)).toBeLessThan(yTolerance);

      // Verify horizontal connectors are visible (md:block)
      const connector1 = page.getByTestId('connector-1');
      const connector2 = page.getByTestId('connector-2');
      await expect(connector1).toBeVisible();
      await expect(connector2).toBeVisible();
    });

    test('navigation links remain accessible at 1024px', async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');

      // Verify navigation links in hero section
      const navFeatures = page.getByTestId('nav-features');
      const navDemo = page.getByTestId('nav-demo');

      await expect(navFeatures).toBeVisible();
      await expect(navDemo).toBeVisible();
    });
  });

  test.describe('Test Case 3: Feature grid displays 2-column on tablet', () => {
    test('feature cards display in 2-column grid at 768px tablet width', async ({ page }) => {
      // Set tablet viewport
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      const featuresSection = page.getByTestId('features-section');
      await expect(featuresSection).toBeVisible();

      const featuresGrid = page.getByTestId('features-grid');
      await expect(featuresGrid).toBeVisible();

      // Get all 4 feature cards
      const cards = page.getByTestId('glassmorphism-card');
      await expect(cards).toHaveCount(4);

      // Get bounding boxes of all cards
      const card1 = cards.nth(0);
      const card2 = cards.nth(1);
      const card3 = cards.nth(2);
      const card4 = cards.nth(3);

      const box1 = await card1.boundingBox();
      const box2 = await card2.boundingBox();
      const box3 = await card3.boundingBox();
      const box4 = await card4.boundingBox();

      expect(box1).not.toBeNull();
      expect(box2).not.toBeNull();
      expect(box3).not.toBeNull();
      expect(box4).not.toBeNull();

      // At 768px (sm breakpoint: 640px+), grid should be sm:grid-cols-2
      // Cards 1 and 2 should be on the same row (similar y)
      // Cards 3 and 4 should be on a different row (similar y to each other, different from 1&2)
      const yTolerance = 10;

      // Row 1: cards 1 and 2 should have similar y coordinates
      expect(Math.abs(box1!.y - box2!.y)).toBeLessThan(yTolerance);

      // Row 2: cards 3 and 4 should have similar y coordinates
      expect(Math.abs(box3!.y - box4!.y)).toBeLessThan(yTolerance);

      // Card 3 should be below card 1 (different row)
      expect(box3!.y).toBeGreaterThan(box1!.y + box1!.height - 10);

      // Card 1 and 2 should be side by side (different x coordinates)
      expect(box2!.x).toBeGreaterThan(box1!.x + 50);
    });

    test('feature cards display in appropriate layout at 900px', async ({ page }) => {
      // Test at a mid-tablet size
      await page.setViewportSize({ width: 900, height: 1024 });
      await page.goto('/');

      const featuresGrid = page.getByTestId('features-grid');
      await expect(featuresGrid).toBeVisible();

      const cards = page.getByTestId('glassmorphism-card');
      await expect(cards).toHaveCount(4);

      // All cards should be visible
      for (let i = 0; i < 4; i++) {
        await expect(cards.nth(i)).toBeVisible();
      }

      // Verify the grid still shows 2 columns at 900px (before lg breakpoint at 1024px)
      const box1 = await cards.nth(0).boundingBox();
      const box2 = await cards.nth(1).boundingBox();

      expect(box1).not.toBeNull();
      expect(box2).not.toBeNull();

      // Cards should be side by side in 2-column layout
      expect(box2!.x).toBeGreaterThan(box1!.x + 50);
    });

    test('feature section has proper spacing and layout at tablet size', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      const featuresSection = page.getByTestId('features-section');
      const featuresHeading = page.getByRole('heading', { name: /powerful features/i });

      await expect(featuresSection).toBeVisible();
      await expect(featuresHeading).toBeVisible();

      // Verify section has proper padding
      const sectionBox = await featuresSection.boundingBox();
      expect(sectionBox).not.toBeNull();

      // Section should span full width
      expect(sectionBox!.width).toBeGreaterThanOrEqual(768);

      // Verify each feature card has all required elements
      for (let i = 1; i <= 4; i++) {
        const icon = page.getByTestId(`feature-icon-${i}`);
        const title = page.getByTestId(`feature-title-${i}`);
        const description = page.getByTestId(`feature-description-${i}`);

        await expect(icon).toBeVisible();
        await expect(title).toBeVisible();
        await expect(description).toBeVisible();
      }
    });
  });

  test.describe('Additional tablet responsive tests', () => {
    test('footer CTA section adapts correctly at tablet width', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      const footerCTA = page.getByTestId('footer-cta');
      await expect(footerCTA).toBeVisible();

      const footerHeadline = page.getByTestId('footer-headline');
      const footerSubheadline = page.getByTestId('footer-subheadline');
      const footerGetStarted = page.getByTestId('footer-cta-get-started');
      const footerCopyright = page.getByTestId('footer-copyright');

      await expect(footerHeadline).toBeVisible();
      await expect(footerSubheadline).toBeVisible();
      await expect(footerGetStarted).toBeVisible();
      await expect(footerCopyright).toBeVisible();
    });

    test('CTA buttons are clickable and properly sized at tablet width', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      const getStartedBtn = page.getByTestId('cta-get-started');
      const loginBtn = page.getByTestId('cta-login');

      await expect(getStartedBtn).toBeVisible();
      await expect(loginBtn).toBeVisible();

      // Verify buttons have proper touch target size (minimum 44x44px for accessibility)
      const getStartedBox = await getStartedBtn.boundingBox();
      const loginBox = await loginBtn.boundingBox();

      expect(getStartedBox).not.toBeNull();
      expect(loginBox).not.toBeNull();

      // Buttons should be at least 44px in height for touch targets
      expect(getStartedBox!.height).toBeGreaterThanOrEqual(44);
      expect(loginBox!.height).toBeGreaterThanOrEqual(44);
    });

    test('page content is fully accessible without horizontal scroll at all tablet widths', async ({ page }) => {
      // Test multiple tablet widths
      const tabletWidths = [768, 834, 900, 1024];

      for (const width of tabletWidths) {
        await page.setViewportSize({ width, height: 1024 });
        await page.goto('/');

        // Verify no horizontal scrollbar
        const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
        const windowWidth = await page.evaluate(() => window.innerWidth);

        expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth + 1); // +1 for potential rounding
      }
    });
  });
});
