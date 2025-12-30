// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Accessibility - Screen Reader Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has proper heading hierarchy (h1 > h2 > h3) without skipping levels', async ({ page }) => {
    // Get all headings on the page
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map((h, index) => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent?.trim() || '',
        index
      }));
    });

    // Verify there is exactly one h1
    const h1Count = headings.filter(h => h.level === 1).length;
    expect(h1Count, 'Page should have exactly one h1 element').toBe(1);

    // Verify h1 is the first heading
    expect(headings[0].level, 'First heading should be h1').toBe(1);

    // Verify heading hierarchy - no skipping levels
    let previousLevel = 0;
    const hierarchyErrors = [];

    for (const heading of headings) {
      // When moving to a deeper level, we should only increase by 1
      if (heading.level > previousLevel && heading.level - previousLevel > 1) {
        hierarchyErrors.push(
          `Heading "${heading.text}" (h${heading.level}) skips levels from h${previousLevel}`
        );
      }
      previousLevel = heading.level;
    }

    expect(hierarchyErrors,
      `Heading hierarchy should not skip levels:\n${hierarchyErrors.join('\n')}`
    ).toHaveLength(0);

    // Verify we have h1 and h2 elements
    const h2Count = headings.filter(h => h.level === 2).length;
    expect(h2Count, 'Page should have h2 section headings').toBeGreaterThan(0);

    // Verify h1 contains product name
    const h1Text = headings.find(h => h.level === 1)?.text;
    expect(h1Text).toContain('MirDB');
  });

  test('TC2: Page uses semantic landmarks (header, main, nav, footer)', async ({ page }) => {
    // Check for header landmark
    const header = page.locator('header');
    await expect(header, 'Page should have a header element').toHaveCount(1);

    // Check for navigation within header
    const nav = page.locator('header nav, nav');
    await expect(nav, 'Page should have a nav element').toHaveCount(1);

    // Check for main content area
    const main = page.locator('main');
    await expect(main, 'Page should have a main element').toHaveCount(1);

    // Check that main has an id for skip link
    const mainId = await main.getAttribute('id');
    expect(mainId, 'Main element should have an id for skip link navigation').toBeTruthy();

    // Check for footer landmark
    const footer = page.locator('footer');
    await expect(footer, 'Page should have a footer element').toHaveCount(1);

    // Verify sections within main have appropriate structure
    const sections = page.locator('main section');
    const sectionCount = await sections.count();
    expect(sectionCount, 'Page should have multiple sections within main').toBeGreaterThan(0);

    // Verify sections have id attributes for navigation
    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const sectionId = await section.getAttribute('id');
      expect(sectionId, `Section ${i + 1} should have an id attribute`).toBeTruthy();
    }
  });

  test('TC3: All images have descriptive alt text (or empty alt for decorative)', async ({ page }) => {
    // Get all images on the page
    const images = await page.evaluate(() => {
      const allImages = document.querySelectorAll('img');
      return Array.from(allImages).map(img => ({
        src: img.getAttribute('src') || '',
        alt: img.getAttribute('alt'),
        hasAlt: img.hasAttribute('alt'),
        ariaHidden: img.getAttribute('aria-hidden'),
        role: img.getAttribute('role')
      }));
    });

    // Check each image
    const issues = [];
    for (const img of images) {
      if (!img.hasAlt) {
        issues.push(`Image with src="${img.src}" is missing alt attribute`);
      }
    }

    expect(issues, `All images should have alt attributes:\n${issues.join('\n')}`).toHaveLength(0);

    // Check for decorative images that use empty alt or aria-hidden
    const decorativeIcons = await page.evaluate(() => {
      // Feature icons using emoji spans should be marked as decorative
      const icons = document.querySelectorAll('.feature-icon, [class*="icon"]');
      return Array.from(icons).map(icon => ({
        text: icon.textContent?.trim() || '',
        ariaHidden: icon.getAttribute('aria-hidden'),
        role: icon.getAttribute('role')
      }));
    });

    // Verify decorative icons are properly hidden from screen readers
    for (const icon of decorativeIcons) {
      if (icon.text) {
        // Decorative icons should have aria-hidden="true"
        expect(icon.ariaHidden,
          `Decorative icon "${icon.text}" should have aria-hidden="true"`
        ).toBe('true');
      }
    }
  });

  test('TC4: Code blocks are readable by screen readers with appropriate labels', async ({ page }) => {
    // Find all code blocks
    const codeBlocks = page.locator('pre code, .code-block pre, .code-block code');
    const codeBlockCount = await codeBlocks.count();

    expect(codeBlockCount, 'Page should have code blocks').toBeGreaterThan(0);

    // Check code block accessibility
    const codeBlockInfo = await page.evaluate(() => {
      const blocks = document.querySelectorAll('pre code, .code-block pre, .code-block code');
      return Array.from(blocks).map((block, index) => {
        const preElement = block.closest('pre') || block;
        const container = block.closest('.code-block') || preElement.parentElement;

        return {
          index,
          hasContent: (block.textContent?.trim().length || 0) > 0,
          // Check if parent or container has aria-label
          containerAriaLabel: container?.getAttribute('aria-label'),
          preAriaLabel: preElement.getAttribute('aria-label'),
          // Check for role attribute
          containerRole: container?.getAttribute('role'),
          preRole: preElement.getAttribute('role'),
          // Check for associated label
          ariaLabelledBy: preElement.getAttribute('aria-labelledby'),
          // Check if there's a preceding heading
          precedingHeading: (() => {
            const section = block.closest('section');
            const heading = section?.querySelector('h1, h2, h3');
            return heading?.textContent?.trim() || null;
          })()
        };
      });
    });

    // Verify each code block is accessible
    for (const info of codeBlockInfo) {
      expect(info.hasContent, `Code block ${info.index + 1} should have content`).toBe(true);

      // Code blocks should either have an aria-label, aria-labelledby, or be in a section with a heading
      const hasAccessibleLabel =
        info.containerAriaLabel ||
        info.preAriaLabel ||
        info.ariaLabelledBy ||
        info.precedingHeading;

      expect(hasAccessibleLabel,
        `Code block ${info.index + 1} should have accessible context (aria-label, aria-labelledby, or section heading)`
      ).toBeTruthy();
    }

    // Verify code content is not hidden from screen readers
    const hiddenCodeBlocks = await page.evaluate(() => {
      const blocks = document.querySelectorAll('pre code, .code-block pre');
      return Array.from(blocks).filter(block => {
        return block.getAttribute('aria-hidden') === 'true';
      }).length;
    });

    expect(hiddenCodeBlocks, 'Code blocks should not be hidden from screen readers').toBe(0);
  });

  test('Additional: Interactive elements have appropriate ARIA labels', async ({ page }) => {
    // Check buttons have accessible names
    const buttons = page.locator('button');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      const accessibleName = await button.evaluate(el => {
        // Get accessible name from aria-label, aria-labelledby, or text content
        const ariaLabel = el.getAttribute('aria-label');
        const ariaLabelledBy = el.getAttribute('aria-labelledby');
        const textContent = el.textContent?.trim();

        if (ariaLabel) return ariaLabel;
        if (ariaLabelledBy) {
          const labelEl = document.getElementById(ariaLabelledBy);
          return labelEl?.textContent?.trim() || '';
        }
        return textContent || '';
      });

      expect(accessibleName.length, `Button ${i + 1} should have an accessible name`).toBeGreaterThan(0);
    }

    // Check links have accessible names
    const links = page.locator('a');
    const linkCount = await links.count();

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      const accessibleName = await link.evaluate(el => {
        const ariaLabel = el.getAttribute('aria-label');
        const textContent = el.textContent?.trim();
        return ariaLabel || textContent || '';
      });

      expect(accessibleName.length, `Link ${i + 1} should have an accessible name`).toBeGreaterThan(0);
    }
  });

  test('Additional: Page has proper document structure', async ({ page }) => {
    // Check html lang attribute
    const htmlLang = await page.getAttribute('html', 'lang');
    expect(htmlLang, 'HTML element should have lang attribute').toBeTruthy();
    expect(htmlLang, 'HTML lang should be a valid language code').toMatch(/^[a-z]{2}(-[A-Z]{2})?$/);

    // Check for page title
    const title = await page.title();
    expect(title.length, 'Page should have a title').toBeGreaterThan(0);
    expect(title, 'Title should contain product name').toContain('MirDB');

    // Check for meta description
    const metaDescription = await page.getAttribute('meta[name="description"]', 'content');
    expect(metaDescription, 'Page should have a meta description').toBeTruthy();
    expect(metaDescription?.length, 'Meta description should have content').toBeGreaterThan(0);
  });
});
