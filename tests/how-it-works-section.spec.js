// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: How It Works / Architecture Section
 * Scenario: Verify the How It Works section displays data flow and LSM tree explanation
 */

test.describe('How It Works / Architecture Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for How It Works or Architecture section
   * Input: Check for How It Works or Architecture section
   * Expected: Section explaining the LSM tree approach and data flow is present
   */
  test('TC1: How It Works section explaining LSM tree approach is present', async ({ page }) => {
    // Locate the How It Works section
    const howItWorksSection = page.locator('#how-it-works.how-it-works-section');
    await expect(howItWorksSection).toBeVisible();

    // Verify section has a heading
    const sectionHeading = howItWorksSection.locator('h2');
    await expect(sectionHeading).toBeVisible();
    await expect(sectionHeading).toHaveText('How It Works');

    // Verify section has intro text explaining LSM tree
    const introText = howItWorksSection.locator('.section-intro');
    await expect(introText).toBeVisible();

    const introContent = await introText.textContent();
    expect(introContent).toBeTruthy();

    // Should mention LSM tree
    const lowerText = introContent.toLowerCase();
    const mentionsLSM = lowerText.includes('lsm') ||
                        lowerText.includes('log-structured') ||
                        lowerText.includes('merge-tree');
    expect(mentionsLSM).toBeTruthy();
  });

  /**
   * Test Case 2: Verify data flow explanation
   * Input: Verify data flow explanation
   * Expected: Content explains the flow from client request through WAL to SSTables
   */
  test('TC2: Data flow from client request through WAL to SSTables is explained', async ({ page }) => {
    // Locate the How It Works section
    const howItWorksSection = page.locator('#how-it-works.how-it-works-section');
    await expect(howItWorksSection).toBeVisible();

    // Verify architecture diagram exists
    const architectureDiagram = howItWorksSection.locator('.architecture-diagram');
    await expect(architectureDiagram).toBeVisible();

    // Verify data flow container exists
    const dataFlow = howItWorksSection.locator('.data-flow');
    await expect(dataFlow).toBeVisible();

    // Verify flow steps are present
    const flowSteps = howItWorksSection.locator('.flow-step');
    const stepCount = await flowSteps.count();
    expect(stepCount).toBeGreaterThanOrEqual(4); // At least 4 steps in the data flow

    // Get all step content and verify key components are mentioned
    const pageContent = await howItWorksSection.textContent();
    const lowerContent = pageContent.toLowerCase();

    // Verify Write Request step
    expect(lowerContent).toContain('write');

    // Verify WAL (Write-Ahead Log) step
    const hasWAL = lowerContent.includes('wal') ||
                   lowerContent.includes('write-ahead log') ||
                   lowerContent.includes('write-ahead');
    expect(hasWAL).toBeTruthy();

    // Verify Memtable step
    expect(lowerContent).toContain('memtable');

    // Verify SSTable step
    const hasSSTable = lowerContent.includes('sstable') ||
                       lowerContent.includes('sorted string');
    expect(hasSSTable).toBeTruthy();
  });

  /**
   * Test Case 3: Check for visual diagram (if present)
   * Input: Check for visual diagram (if present)
   * Expected: Architecture diagram or visual representation is displayed (if implemented)
   */
  test('TC3: Architecture diagram or visual representation is displayed', async ({ page }) => {
    // Locate the How It Works section
    const howItWorksSection = page.locator('#how-it-works.how-it-works-section');
    await expect(howItWorksSection).toBeVisible();

    // Verify architecture diagram container exists with proper ARIA role
    const architectureDiagram = howItWorksSection.locator('.architecture-diagram');
    await expect(architectureDiagram).toBeVisible();

    // Check for role="img" and aria-label for accessibility
    const role = await architectureDiagram.getAttribute('role');
    expect(role).toBe('img');

    const ariaLabel = await architectureDiagram.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('architecture');

    // Verify visual flow representation with steps
    const flowSteps = howItWorksSection.locator('.flow-step');
    const stepCount = await flowSteps.count();
    expect(stepCount).toBeGreaterThanOrEqual(4);

    // Verify each flow step has proper visual structure
    for (let i = 0; i < stepCount; i++) {
      const step = flowSteps.nth(i);

      // Each step should have an icon
      const icon = step.locator('.flow-icon');
      await expect(icon).toBeVisible();

      // Each step should have a heading
      const heading = step.locator('h3');
      await expect(heading).toBeVisible();

      // Each step should have a description
      const description = step.locator('p');
      await expect(description).toBeVisible();
    }

    // Verify flow arrows exist between steps (visual connectors)
    const flowArrows = howItWorksSection.locator('.flow-arrow');
    const arrowCount = await flowArrows.count();
    expect(arrowCount).toBeGreaterThanOrEqual(3); // At least 3 arrows between 4 steps
  });

  /**
   * Additional test: How It Works section is accessible via navigation
   */
  test('How It Works section is accessible via navigation link', async ({ page }) => {
    // Find and click the How It Works navigation link
    const howItWorksNavLink = page.locator('.nav-link', { hasText: 'How It Works' });
    await expect(howItWorksNavLink).toBeVisible();
    await howItWorksNavLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify How It Works section is now near the top of viewport
    const howItWorksSection = page.locator('#how-it-works');
    const sectionBox = await howItWorksSection.boundingBox();
    expect(sectionBox).not.toBeNull();
    expect(sectionBox.y).toBeLessThan(200);

    // Verify URL hash has changed
    const currentUrl = page.url();
    expect(currentUrl).toContain('#how-it-works');
  });

  /**
   * Additional test: How It Works section has proper semantic structure
   */
  test('How It Works section has proper semantic structure', async ({ page }) => {
    // Verify section exists within main
    const howItWorksInMain = page.locator('main #how-it-works');
    await expect(howItWorksInMain).toBeVisible();

    // Verify section has proper heading hierarchy
    const sectionH2 = page.locator('#how-it-works h2');
    await expect(sectionH2).toBeVisible();
    await expect(sectionH2).toHaveText('How It Works');

    // Verify flow steps have proper h3 headings
    const flowStepHeadings = page.locator('#how-it-works .flow-step h3');
    const headingCount = await flowStepHeadings.count();
    expect(headingCount).toBeGreaterThanOrEqual(4);

    // Verify detail cards have proper h3 headings
    const detailCardHeadings = page.locator('#how-it-works .detail-card h3');
    const detailHeadingCount = await detailCardHeadings.count();
    expect(detailHeadingCount).toBeGreaterThanOrEqual(2);
  });

  /**
   * Additional test: LSM Tree benefits and data integrity details are displayed
   */
  test('LSM Tree benefits and data integrity details are displayed', async ({ page }) => {
    // Locate the How It Works section
    const howItWorksSection = page.locator('#how-it-works.how-it-works-section');
    await expect(howItWorksSection).toBeVisible();

    // Verify architecture details section exists
    const architectureDetails = howItWorksSection.locator('.architecture-details');
    await expect(architectureDetails).toBeVisible();

    // Verify detail cards exist
    const detailCards = howItWorksSection.locator('.detail-card');
    const cardCount = await detailCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(2);

    // Verify LSM Tree Benefits card
    const lsmBenefitsCard = page.locator('.detail-card', { hasText: 'LSM Tree Benefits' });
    await expect(lsmBenefitsCard).toBeVisible();

    // Verify benefits list has items
    const benefitsList = lsmBenefitsCard.locator('li');
    const benefitsCount = await benefitsList.count();
    expect(benefitsCount).toBeGreaterThanOrEqual(2);

    // Verify Data Integrity card
    const dataIntegrityCard = page.locator('.detail-card', { hasText: 'Data Integrity' });
    await expect(dataIntegrityCard).toBeVisible();

    // Verify data integrity list has items
    const integrityList = dataIntegrityCard.locator('li');
    const integrityCount = await integrityList.count();
    expect(integrityCount).toBeGreaterThanOrEqual(2);
  });

  /**
   * Additional test: How It Works section layout is responsive
   */
  test('How It Works section responds to viewport changes', async ({ page }) => {
    // Test desktop layout
    await page.setViewportSize({ width: 1200, height: 800 });

    const howItWorksSection = page.locator('#how-it-works.how-it-works-section');
    await expect(howItWorksSection).toBeVisible();

    // Verify data flow is displayed in horizontal layout on desktop
    const dataFlow = page.locator('.data-flow');
    const flowDisplay = await dataFlow.evaluate(el => window.getComputedStyle(el).flexDirection);
    expect(flowDisplay).toBe('row');

    // Test mobile layout
    await page.setViewportSize({ width: 375, height: 667 });

    // Verify data flow changes to vertical layout on mobile
    const mobileFlowDisplay = await dataFlow.evaluate(el => window.getComputedStyle(el).flexDirection);
    expect(mobileFlowDisplay).toBe('column');
  });
});
