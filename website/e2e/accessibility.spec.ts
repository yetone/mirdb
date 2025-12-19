import { test, expect } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('accessibility score meets WCAG 2.1 AA standards (score 90+)', async ({ page }) => {
    // Run axe accessibility audit with WCAG 2.1 AA rules
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze()

    // Log any violations for debugging
    if (accessibilityScanResults.violations.length > 0) {
      console.log('Accessibility violations found:')
      accessibilityScanResults.violations.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`)
        violation.nodes.forEach((node) => {
          console.log(`  - ${node.target.join(', ')}: ${node.failureSummary}`)
        })
      })
    }

    // Calculate accessibility score
    // Score = (passed / (passed + violations)) * 100
    const totalRules = accessibilityScanResults.passes.length + accessibilityScanResults.violations.length
    const score = totalRules > 0 ? (accessibilityScanResults.passes.length / totalRules) * 100 : 100

    console.log(`Accessibility score: ${score.toFixed(2)}%`)
    console.log(`Passed rules: ${accessibilityScanResults.passes.length}`)
    console.log(`Violations: ${accessibilityScanResults.violations.length}`)

    // Expect at least 90% score and no critical/serious violations
    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    )

    expect(criticalViolations.length, 'No critical or serious accessibility violations').toBe(0)
    expect(score, 'Accessibility score should be 90+').toBeGreaterThanOrEqual(90)
  })

  test('page has single h1 and headings follow logical hierarchy', async ({ page }) => {
    // Check for single h1
    const h1Elements = page.locator('h1')
    const h1Count = await h1Elements.count()
    expect(h1Count, 'Page should have exactly one h1 element').toBe(1)

    // Verify h1 is visible and not empty
    const h1Text = await h1Elements.first().textContent()
    expect(h1Text?.trim().length, 'h1 should have content').toBeGreaterThan(0)

    // Get all heading elements
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all()
    const headingLevels: number[] = []

    for (const heading of headings) {
      const tagName = await heading.evaluate((el) => el.tagName.toLowerCase())
      const level = parseInt(tagName.replace('h', ''))
      headingLevels.push(level)
    }

    // Verify heading hierarchy - no heading should skip more than one level
    for (let i = 1; i < headingLevels.length; i++) {
      const currentLevel = headingLevels[i]
      const previousLevel = headingLevels[i - 1]

      // When going deeper, should not skip levels (e.g., h1 -> h3 is bad, h1 -> h2 is good)
      if (currentLevel > previousLevel) {
        expect(
          currentLevel - previousLevel,
          `Heading level should not skip from h${previousLevel} to h${currentLevel}`
        ).toBeLessThanOrEqual(1)
      }
    }

    console.log('Heading levels found:', headingLevels.join(', '))
  })

  test('all text meets 4.5:1 contrast ratio minimum', async ({ page }) => {
    // Run axe specifically for color contrast rules
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('*')
      .analyze()

    // Filter for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast' || v.id === 'color-contrast-enhanced'
    )

    if (contrastViolations.length > 0) {
      console.log('Color contrast violations:')
      contrastViolations.forEach((violation) => {
        violation.nodes.forEach((node) => {
          console.log(`- Element: ${node.target.join(', ')}`)
          console.log(`  ${node.failureSummary}`)
        })
      })
    }

    expect(
      contrastViolations.length,
      'All text should meet 4.5:1 contrast ratio minimum'
    ).toBe(0)
  })

  test('all links and buttons are reachable via Tab key', async ({ page }) => {
    // Get all interactive elements
    const interactiveElements = await page.locator('a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])').all()

    const focusableElements: string[] = []

    // Start from the beginning of the page
    await page.keyboard.press('Tab')

    // Track which elements receive focus
    let tabCount = 0
    const maxTabs = 50 // Prevent infinite loops
    const focusedSelectors = new Set<string>()

    while (tabCount < maxTabs) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement
        if (!el || el === document.body) return null

        // Create a unique identifier for the element
        const tagName = el.tagName.toLowerCase()
        const id = el.id ? `#${el.id}` : ''
        const classes = el.className ? `.${el.className.split(' ').join('.')}` : ''
        const href = el.getAttribute('href') ? `[href="${el.getAttribute('href')}"]` : ''

        return `${tagName}${id}${classes}${href}`
      })

      if (!activeElement || focusedSelectors.has(activeElement)) {
        break // We've cycled through or reached the end
      }

      focusedSelectors.add(activeElement)
      focusableElements.push(activeElement)

      await page.keyboard.press('Tab')
      tabCount++
    }

    console.log(`Found ${focusableElements.length} focusable elements via Tab key`)
    console.log('Focusable elements:', focusableElements)

    // Verify we can reach at least the expected interactive elements
    // Every link and button should be focusable
    const links = await page.locator('a[href]').count()
    const buttons = await page.locator('button').count()
    const expectedFocusable = links + buttons

    expect(
      focusableElements.length,
      `Should be able to tab through at least ${expectedFocusable} interactive elements (links and buttons)`
    ).toBeGreaterThanOrEqual(expectedFocusable)
  })

  test('focused elements have visible focus indicator', async ({ page }) => {
    // Get all focusable elements
    const focusableSelectors = 'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
    const focusableElements = await page.locator(focusableSelectors).all()

    expect(focusableElements.length, 'Should have focusable elements to test').toBeGreaterThan(0)

    for (const element of focusableElements) {
      // Focus the element
      await element.focus()

      // Check that focus styles are applied
      const hasVisibleFocus = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el)
        const focusStyles = window.getComputedStyle(el, ':focus')

        // Check for common focus indicators:
        // 1. outline property
        const hasOutline = styles.outline !== 'none' &&
                          styles.outline !== '' &&
                          styles.outline !== 'rgb(0, 0, 0) none 0px' &&
                          styles.outlineWidth !== '0px'

        // 2. box-shadow (often used for custom focus styles)
        const hasBoxShadow = styles.boxShadow !== 'none' && styles.boxShadow !== ''

        // 3. border change (comparing to check if there's a visible border)
        const hasBorder = styles.borderColor !== 'transparent' &&
                         parseFloat(styles.borderWidth) > 0

        // 4. Check outline-offset (sometimes used with outline)
        const hasOutlineOffset = parseFloat(styles.outlineOffset) !== 0

        return hasOutline || hasBoxShadow || (hasBorder && hasOutlineOffset)
      })

      // Get element description for error message
      const elementDesc = await element.evaluate((el) => {
        const tagName = el.tagName.toLowerCase()
        const text = el.textContent?.trim().substring(0, 30) || ''
        return `${tagName}: "${text}"`
      })

      // Note: Browser default focus styles count as visible focus
      // We're checking that focus is not explicitly suppressed with outline: none
      const focusSuppressed = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el)
        return styles.outline === 'none' ||
               styles.outlineStyle === 'none' ||
               styles.outlineWidth === '0px'
      })

      // If custom focus styles exist or browser defaults are not suppressed
      expect(
        hasVisibleFocus || !focusSuppressed,
        `Element ${elementDesc} should have visible focus indicator`
      ).toBeTruthy()
    }
  })

  test('all images and icons have appropriate alt text', async ({ page }) => {
    // Check all img elements for alt text
    const images = await page.locator('img').all()

    for (const img of images) {
      const alt = await img.getAttribute('alt')
      const src = await img.getAttribute('src')

      // alt attribute should exist (even if empty for decorative images)
      expect(
        alt !== null,
        `Image ${src} should have alt attribute`
      ).toBeTruthy()
    }

    // Check SVG elements used as icons
    const svgIcons = await page.locator('svg').all()

    for (const svg of svgIcons) {
      // SVGs should have either:
      // 1. aria-hidden="true" (if decorative)
      // 2. aria-label or aria-labelledby (if meaningful)
      // 3. <title> element (if meaningful)
      const ariaHidden = await svg.getAttribute('aria-hidden')
      const ariaLabel = await svg.getAttribute('aria-label')
      const ariaLabelledBy = await svg.getAttribute('aria-labelledby')
      const hasTitle = await svg.locator('title').count()
      const role = await svg.getAttribute('role')

      const isAccessible =
        ariaHidden === 'true' || // Decorative
        ariaLabel !== null ||    // Has label
        ariaLabelledBy !== null || // Referenced label
        hasTitle > 0 ||          // Has title element
        role === 'img'           // Has img role (should also have aria-label)

      // Skip if SVG is nested inside a button or link that provides context
      const parentButton = await svg.locator('xpath=ancestor::button').count()
      const parentLink = await svg.locator('xpath=ancestor::a').count()
      const hasParentContext = parentButton > 0 || parentLink > 0

      if (!hasParentContext) {
        expect(
          isAccessible,
          `SVG icon should have appropriate accessibility attributes`
        ).toBeTruthy()
      }
    }

    // Check for icon fonts or emoji used as icons (spans with single characters or icon classes)
    const iconSpans = await page.locator('span.icon, i.icon, span[aria-label]').all()

    for (const icon of iconSpans) {
      const ariaLabel = await icon.getAttribute('aria-label')
      const ariaHidden = await icon.getAttribute('aria-hidden')
      const role = await icon.getAttribute('role')

      const isAccessible =
        ariaHidden === 'true' || // Decorative
        ariaLabel !== null ||    // Has label
        role === 'img'           // Has img role

      expect(
        isAccessible,
        `Icon span should have aria-label or be marked as decorative`
      ).toBeTruthy()
    }

    // Run axe to check for image alt text issues
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a'])
      .analyze()

    const imageViolations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'image-alt' || v.id === 'svg-img-alt'
    )

    expect(
      imageViolations.length,
      'All images should have appropriate alt text'
    ).toBe(0)
  })
})
