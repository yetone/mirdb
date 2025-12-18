// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const pageUrl = 'file://' + path.join(__dirname, '..', 'index.html');

test.describe('Architecture Overview Section (REQ-6)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
  });

  test('TC1: Architecture section exists on page', async ({ page }) => {
    // Navigate to architecture section using the anchor
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify section has the correct heading
    const heading = architectureSection.locator('h2');
    await expect(heading).toHaveText('Architecture Overview');

    // Verify section has an introduction paragraph
    const intro = architectureSection.locator('.architecture-intro');
    await expect(intro).toBeVisible();
    await expect(intro).toContainText('LSM tree');
  });

  test('TC2: Visual diagram of LSM tree is present', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Find the diagram container
    const diagramContainer = architectureSection.locator('.architecture-diagram-container');
    await expect(diagramContainer).toBeVisible();

    // Verify SVG diagram is present
    const diagram = architectureSection.locator('.architecture-diagram');
    await expect(diagram).toBeVisible();

    // Verify diagram is an SVG element with role="img"
    await expect(diagram).toHaveAttribute('role', 'img');

    // Verify diagram contains LSM tree components
    const svgContent = await diagram.innerHTML();
    expect(svgContent).toContain('Write-Ahead Log');
    expect(svgContent).toContain('Memtable');
    expect(svgContent).toContain('Level 0 SSTables');
    expect(svgContent).toContain('Level 1+ SSTables');
    expect(svgContent).toContain('minor compaction');
    expect(svgContent).toContain('major compaction');
  });

  test('TC3: Architecture diagram has descriptive alt text for accessibility', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Find the SVG diagram
    const diagram = architectureSection.locator('.architecture-diagram');
    await expect(diagram).toBeVisible();

    // Verify SVG has proper accessibility attributes
    // SVG uses aria-labelledby to reference title and desc elements
    const ariaLabelledBy = await diagram.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBeTruthy();
    expect(ariaLabelledBy).toContain('architecture-diagram-title');
    expect(ariaLabelledBy).toContain('architecture-diagram-desc');

    // Verify title element exists with descriptive text
    // Note: SVG title elements are not visually rendered but provide accessibility info
    const title = diagram.locator('title');
    const titleText = await title.textContent();
    expect(titleText).toBeTruthy();
    expect(titleText).toContain('LSM Tree');
    expect(titleText).toContain('Architecture');

    // Verify desc element exists with detailed description
    // Note: SVG desc elements are not visually rendered but provide accessibility info
    const desc = diagram.locator('desc');
    const descText = await desc.textContent();
    expect(descText).toBeTruthy();
    expect(descText.toLowerCase()).toContain('lsm tree');
    expect(descText.toLowerCase()).toContain('data flow');
    expect(descText).toContain('Write-Ahead Log');
    expect(descText).toContain('Memtable');
    expect(descText.toLowerCase()).toContain('compaction');
  });

  test('Navigation link to architecture section exists', async ({ page }) => {
    // Find the navigation links
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Find the architecture navigation link
    const architectureLink = navLinks.locator('a[href="#architecture"]');
    await expect(architectureLink).toBeVisible();
    await expect(architectureLink).toHaveText('Architecture');
  });

  test('Architecture diagram has a figure caption', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Find the figure element
    const figure = architectureSection.locator('.architecture-figure');
    await expect(figure).toBeVisible();

    // Find the figcaption
    const caption = figure.locator('.architecture-caption');
    await expect(caption).toBeVisible();

    // Verify caption contains useful information
    const captionText = await caption.textContent();
    expect(captionText).toContain('LSM Tree');
    // Caption uses "Data Flow" (capitalized)
    expect(captionText.toLowerCase()).toContain('data flow');
  });

  test('Architecture section is scrollable via navigation', async ({ page }) => {
    // Click on the architecture navigation link
    const architectureLink = page.locator('.nav-links a[href="#architecture"]');
    await architectureLink.click();

    // Wait for scroll to complete (smooth scroll)
    await page.waitForTimeout(500);

    // Verify the architecture section is in view
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();
  });
});
