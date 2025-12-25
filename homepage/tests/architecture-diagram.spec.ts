import { test, expect } from '@playwright/test';

test.describe('Architecture Diagram Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: architecture diagram is displayed showing MirDB data flow', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for architecture diagram - could be SVG, img, or div-based diagram
    const diagram = architectureSection.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Verify diagram shows the data flow components
    const diagramContent = await architectureSection.textContent();

    // Check for Write -> WAL -> Memtable -> SSTable flow components
    expect(diagramContent?.toLowerCase()).toMatch(/write|request/i);
    expect(diagramContent?.toLowerCase()).toMatch(/wal|write.?ahead/i);
    expect(diagramContent?.toLowerCase()).toMatch(/memtable/i);
    expect(diagramContent?.toLowerCase()).toMatch(/sstable|sst/i);
  });

  test('TC2: architecture diagram has alt text for accessibility', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for diagram element
    const diagram = architectureSection.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Check if it's an img element with alt text
    const imgElement = diagram.locator('img');
    const imgCount = await imgElement.count();

    if (imgCount > 0) {
      // If it's an image, check for alt text
      const altText = await imgElement.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText!.length).toBeGreaterThan(10); // Alt text should be descriptive
    } else {
      // If it's an SVG or div-based diagram, check for aria-label or role
      const ariaLabel = await diagram.getAttribute('aria-label');
      const ariaLabelledBy = await diagram.getAttribute('aria-labelledby');
      const role = await diagram.getAttribute('role');

      // Should have either aria-label, aria-labelledby, or role="img" with title
      const hasAccessibility = ariaLabel || ariaLabelledBy || role === 'img';
      expect(hasAccessibility).toBeTruthy();

      // If using aria-label, it should be descriptive
      if (ariaLabel) {
        expect(ariaLabel.length).toBeGreaterThan(10);
      }
    }
  });

  test('TC3: LSM tree explanation text is provided near the diagram', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for explanation text container
    const explanationText = architectureSection.locator('[data-testid="lsm-explanation"]');
    await expect(explanationText).toBeVisible();

    // Get the text content
    const textContent = await explanationText.textContent();

    // Verify LSM tree is mentioned
    expect(textContent?.toLowerCase()).toContain('lsm');

    // Verify explanation includes benefits/purpose
    expect(textContent?.toLowerCase()).toMatch(/write|performance|optimized|log.?structured|merge/i);
  });

  test('architecture section has proper heading and structure', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check section has a heading
    const heading = architectureSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/architecture|how.*works|data.*flow/i);

    // Check that both diagram and explanation are present
    const diagram = architectureSection.locator('[data-testid="architecture-diagram"]');
    const explanation = architectureSection.locator('[data-testid="lsm-explanation"]');

    await expect(diagram).toBeVisible();
    await expect(explanation).toBeVisible();
  });

  test('architecture section is positioned correctly in page flow', async ({ page }) => {
    // Architecture should appear after features and before getting-started
    const featuresSection = page.locator('#features');
    const architectureSection = page.locator('#architecture');
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');

    // All sections should be visible
    await expect(featuresSection).toBeVisible();
    await expect(architectureSection).toBeVisible();
    await expect(gettingStartedSection).toBeVisible();

    // Get bounding boxes to verify order
    const featuresBox = await featuresSection.boundingBox();
    const architectureBox = await architectureSection.boundingBox();
    const gettingStartedBox = await gettingStartedSection.boundingBox();

    expect(featuresBox).toBeTruthy();
    expect(architectureBox).toBeTruthy();
    expect(gettingStartedBox).toBeTruthy();

    // Architecture should be below features and above getting-started
    expect(architectureBox!.y).toBeGreaterThan(featuresBox!.y);
    expect(gettingStartedBox!.y).toBeGreaterThan(architectureBox!.y);
  });
});
