import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Desktop (1024px-1440px)', () => {

  test.describe('TC1: Desktop layout at 1024px width', () => {
    test.beforeEach(async ({ page }) => {
      // Set viewport to 1024px width (lower bound of desktop)
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');
    });

    test('Navigation bar displays full horizontal navigation at 1024px', async ({ page }) => {
      const navbar = page.locator('nav.navbar');
      await expect(navbar).toBeVisible();

      // Check that all navigation links are visible in horizontal layout
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Verify nav links are displayed horizontally (flex row layout)
      const navLinksBox = await navLinks.boundingBox();
      expect(navLinksBox).not.toBeNull();

      // Get all individual nav link items
      const linkItems = page.locator('.nav-links a');
      await expect(linkItems).toHaveCount(5);

      // All links should be visible
      for (let i = 0; i < 5; i++) {
        await expect(linkItems.nth(i)).toBeVisible();
      }

      // Verify horizontal layout by checking that links are side by side
      const firstLink = await linkItems.nth(0).boundingBox();
      const lastLink = await linkItems.nth(4).boundingBox();
      expect(firstLink).not.toBeNull();
      expect(lastLink).not.toBeNull();

      // First and last links should be at similar Y positions (horizontal layout)
      expect(Math.abs(firstLink!.y - lastLink!.y)).toBeLessThan(50);
    });

    test('Hero section is fully visible at 1024px', async ({ page }) => {
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify hero content is centered and visible
      const heroContent = page.locator('.hero-content');
      await expect(heroContent).toBeVisible();

      // Check that title and tagline are visible
      await expect(page.locator('.hero h1')).toBeVisible();
      await expect(page.locator('.tagline')).toBeVisible();

      // Verify CTA buttons are displayed horizontally
      const heroButtons = page.locator('.hero-buttons');
      await expect(heroButtons).toBeVisible();

      const primaryBtn = page.locator('.btn-primary');
      const secondaryBtn = page.locator('.btn-secondary');
      await expect(primaryBtn).toBeVisible();
      await expect(secondaryBtn).toBeVisible();

      // Buttons should be side by side at desktop width
      const primaryBox = await primaryBtn.boundingBox();
      const secondaryBox = await secondaryBtn.boundingBox();
      expect(primaryBox).not.toBeNull();
      expect(secondaryBox).not.toBeNull();
      expect(Math.abs(primaryBox!.y - secondaryBox!.y)).toBeLessThan(20);
    });

    test('All main sections are accessible at 1024px', async ({ page }) => {
      // Verify all main sections exist and can be scrolled to
      const sections = ['#features', '#getting-started', '#commands', '#architecture'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await expect(section).toBeAttached();
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();
      }
    });
  });

  test.describe('TC2: Content centering and max-width at 1440px', () => {
    test.beforeEach(async ({ page }) => {
      // Set viewport to 1440px width (upper bound of standard desktop)
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto('/');
    });

    test('Container has appropriate max-width at 1440px', async ({ page }) => {
      // Check that container elements have max-width constraint
      const containers = page.locator('.container');
      const containerCount = await containers.count();
      expect(containerCount).toBeGreaterThan(0);

      // Check first container's width
      const firstContainer = containers.first();
      const containerBox = await firstContainer.boundingBox();
      expect(containerBox).not.toBeNull();

      // Container max-width is 1200px as per CSS
      expect(containerBox!.width).toBeLessThanOrEqual(1200);
    });

    test('Content is centered on the page at 1440px', async ({ page }) => {
      // Get viewport and container positions
      const viewportWidth = 1440;

      // Check hero content centering
      const heroContent = page.locator('.hero-content');
      const heroBox = await heroContent.boundingBox();
      expect(heroBox).not.toBeNull();

      // Hero content should be centered (left margin approximately equal to right margin)
      const heroLeftMargin = heroBox!.x;
      const heroRightMargin = viewportWidth - (heroBox!.x + heroBox!.width);
      expect(Math.abs(heroLeftMargin - heroRightMargin)).toBeLessThan(100);

      // Check container centering in features section
      await page.locator('#features').scrollIntoViewIfNeeded();
      const featuresContainer = page.locator('#features .container');
      const featuresBox = await featuresContainer.boundingBox();
      expect(featuresBox).not.toBeNull();

      // Container should be centered
      const leftMargin = featuresBox!.x;
      const rightMargin = viewportWidth - (featuresBox!.x + featuresBox!.width);
      expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(100);
    });

    test('Navigation container respects max-width at 1440px', async ({ page }) => {
      const navContainer = page.locator('.nav-container');
      const navBox = await navContainer.boundingBox();
      expect(navBox).not.toBeNull();

      // Nav container max-width is 1200px
      expect(navBox!.width).toBeLessThanOrEqual(1200);
    });

    test('Content does not stretch too wide at 1440px', async ({ page }) => {
      // Verify that getting-started content has max-width
      await page.locator('#getting-started').scrollIntoViewIfNeeded();
      const gettingStartedContent = page.locator('.getting-started-content');
      const gsBox = await gettingStartedContent.boundingBox();
      expect(gsBox).not.toBeNull();

      // getting-started-content has max-width: 900px
      expect(gsBox!.width).toBeLessThanOrEqual(900);

      // Verify architecture content has max-width
      await page.locator('#architecture').scrollIntoViewIfNeeded();
      const archContent = page.locator('.architecture-content');
      const archBox = await archContent.boundingBox();
      expect(archBox).not.toBeNull();

      // architecture-content has max-width: 900px
      expect(archBox!.width).toBeLessThanOrEqual(900);
    });
  });

  test.describe('TC3: Feature cards multi-column grid layout', () => {
    test.beforeEach(async ({ page }) => {
      // Set viewport to desktop size
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');
    });

    test('Feature cards display in multi-column grid at 1024px', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4);

      // Get bounding boxes for first two cards to verify multi-column layout
      const firstCard = await featureCards.nth(0).boundingBox();
      const secondCard = await featureCards.nth(1).boundingBox();
      expect(firstCard).not.toBeNull();
      expect(secondCard).not.toBeNull();

      // Cards should be side by side (on the same row) at desktop width
      // They should have similar Y positions
      expect(Math.abs(firstCard!.y - secondCard!.y)).toBeLessThan(20);

      // And different X positions (side by side)
      expect(secondCard!.x).toBeGreaterThan(firstCard!.x + firstCard!.width - 50);
    });

    test('Feature cards display in multi-column grid at 1440px', async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto('/');
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCards = page.locator('.feature-card');

      // Get positions of all 4 cards
      const cardBoxes = [];
      for (let i = 0; i < 4; i++) {
        const box = await featureCards.nth(i).boundingBox();
        expect(box).not.toBeNull();
        cardBoxes.push(box!);
      }

      // At 1440px, with grid-template-columns: repeat(auto-fit, minmax(250px, 1fr))
      // All 4 cards should fit in one row (1440px container can fit 4 x 250px cards)
      // Check that cards are arranged horizontally

      // First row cards should have same Y position
      const firstRowY = cardBoxes[0].y;
      const allSameRow = cardBoxes.every(box => Math.abs(box.y - firstRowY) < 50);

      if (allSameRow) {
        // All 4 cards in one row - verify horizontal arrangement
        for (let i = 1; i < cardBoxes.length; i++) {
          expect(cardBoxes[i].x).toBeGreaterThan(cardBoxes[i-1].x);
        }
      } else {
        // Cards might be in 2x2 grid - verify at least 2 columns
        // First two cards should be on the same row
        expect(Math.abs(cardBoxes[0].y - cardBoxes[1].y)).toBeLessThan(50);
        expect(cardBoxes[1].x).toBeGreaterThan(cardBoxes[0].x);
      }
    });

    test('Each feature card has icon, title, and description', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      for (let i = 0; i < cardCount; i++) {
        const card = featureCards.nth(i);

        // Check for icon
        const icon = card.locator('.feature-icon');
        await expect(icon).toBeVisible();

        // Check for title (h3)
        const title = card.locator('h3');
        await expect(title).toBeVisible();
        const titleText = await title.textContent();
        expect(titleText).toBeTruthy();

        // Check for description (p)
        const description = card.locator('p');
        await expect(description).toBeVisible();
        const descText = await description.textContent();
        expect(descText).toBeTruthy();
      }
    });

    test('Commands grid displays in multi-column layout at desktop', async ({ page }) => {
      await page.locator('#commands').scrollIntoViewIfNeeded();

      const commandsGrid = page.locator('.commands-grid');
      await expect(commandsGrid).toBeVisible();

      // Get command categories
      const commandCategories = page.locator('.command-category');
      const categoryCount = await commandCategories.count();
      expect(categoryCount).toBe(4);

      // Verify multi-column layout
      const firstCategory = await commandCategories.nth(0).boundingBox();
      const secondCategory = await commandCategories.nth(1).boundingBox();
      expect(firstCategory).not.toBeNull();
      expect(secondCategory).not.toBeNull();

      // Categories should be side by side at desktop width
      expect(Math.abs(firstCategory!.y - secondCategory!.y)).toBeLessThan(20);
      expect(secondCategory!.x).toBeGreaterThan(firstCategory!.x);
    });
  });

  test.describe('Footer displays correctly at desktop widths', () => {
    test('Footer content is in multi-column layout at 1024px', async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.locator('.footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Footer content grid should show multiple columns
      const footerContent = page.locator('.footer-content');
      await expect(footerContent).toBeVisible();

      // Get footer sections
      const footerSections = page.locator('.footer-section');
      const sectionCount = await footerSections.count();
      expect(sectionCount).toBe(3);

      // Verify multi-column layout
      const firstSection = await footerSections.nth(0).boundingBox();
      const secondSection = await footerSections.nth(1).boundingBox();
      expect(firstSection).not.toBeNull();
      expect(secondSection).not.toBeNull();

      // Sections should be side by side at desktop width
      expect(Math.abs(firstSection!.y - secondSection!.y)).toBeLessThan(50);
    });

    test('Footer content is in multi-column layout at 1440px', async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto('/');

      const footer = page.locator('.footer');
      await footer.scrollIntoViewIfNeeded();

      const footerSections = page.locator('.footer-section');

      // All three sections should be on the same row
      const boxes = [];
      for (let i = 0; i < 3; i++) {
        const box = await footerSections.nth(i).boundingBox();
        expect(box).not.toBeNull();
        boxes.push(box!);
      }

      // All sections should have similar Y positions
      expect(Math.abs(boxes[0].y - boxes[1].y)).toBeLessThan(50);
      expect(Math.abs(boxes[1].y - boxes[2].y)).toBeLessThan(50);
    });
  });
});
