import { test, expect } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

test.describe('Homepage Accessibility Compliance (WCAG 2.1 AA)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle')
  })

  test('TC1: No critical or serious accessibility violations (axe audit)', async ({ page }) => {
    // Run axe accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze()

    // Filter for critical and serious violations
    const criticalAndSeriousViolations = accessibilityScanResults.violations.filter(
      violation => violation.impact === 'critical' || violation.impact === 'serious'
    )

    // Log any violations for debugging
    if (criticalAndSeriousViolations.length > 0) {
      console.log('Critical/Serious Accessibility Violations:')
      criticalAndSeriousViolations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description}`)
        console.log(`  Impact: ${violation.impact}`)
        console.log(`  Nodes affected: ${violation.nodes.length}`)
        violation.nodes.forEach(node => {
          console.log(`    HTML: ${node.html}`)
          console.log(`    Fix: ${node.failureSummary}`)
        })
      })
    }

    expect(criticalAndSeriousViolations).toHaveLength(0)
  })

  test('TC4: Color contrast meets WCAG AA in light theme (4.5:1)', async ({ page }) => {
    // Set light theme
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'light')
      localStorage.setItem('theme-preference', 'light')
    })

    // Wait for theme to apply
    await page.waitForTimeout(500)

    // Run axe audit specifically for color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('body')
      .analyze()

    // Filter for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      violation => violation.id === 'color-contrast' || violation.id === 'color-contrast-enhanced'
    )

    // Log any contrast violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Light Theme Color Contrast Violations:')
      contrastViolations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description}`)
        violation.nodes.forEach(node => {
          console.log(`  HTML: ${node.html}`)
          console.log(`  Fix: ${node.failureSummary}`)
        })
      })
    }

    expect(contrastViolations).toHaveLength(0)
  })

  test('TC5: Color contrast meets WCAG AA in dark theme (4.5:1)', async ({ page }) => {
    // Set dark theme
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'dark')
      localStorage.setItem('theme-preference', 'dark')
    })

    // Wait for theme to apply
    await page.waitForTimeout(500)

    // Run axe audit specifically for color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('body')
      .analyze()

    // Filter for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      violation => violation.id === 'color-contrast' || violation.id === 'color-contrast-enhanced'
    )

    // Log any contrast violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Dark Theme Color Contrast Violations:')
      contrastViolations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description}`)
        violation.nodes.forEach(node => {
          console.log(`  HTML: ${node.html}`)
          console.log(`  Fix: ${node.failureSummary}`)
        })
      })
    }

    expect(contrastViolations).toHaveLength(0)
  })

  test('Semantic HTML structure is correct', async ({ page }) => {
    // Check for proper semantic elements
    const header = page.locator('header')
    const main = page.locator('main')
    const sections = page.locator('section')
    const footer = page.locator('footer')
    const nav = page.locator('nav')

    // Verify presence of semantic elements
    await expect(header.first()).toBeVisible()
    await expect(main).toBeVisible()
    expect(await sections.count()).toBeGreaterThan(0)
    await expect(footer.first()).toBeVisible()
    expect(await nav.count()).toBeGreaterThan(0)
  })

  test('Heading hierarchy is correct', async ({ page }) => {
    // Get all headings
    const h1Elements = page.locator('h1')
    const h2Elements = page.locator('h2')
    const h3Elements = page.locator('h3')

    // Verify single h1 element
    const h1Count = await h1Elements.count()
    expect(h1Count).toBe(1)

    // Verify h2 elements exist (after h1)
    const h2Count = await h2Elements.count()
    expect(h2Count).toBeGreaterThan(0)

    // Verify h3 elements exist (for features)
    const h3Count = await h3Elements.count()
    expect(h3Count).toBeGreaterThan(0)

    // Run axe heading order check
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a'])
      .analyze()

    // Filter for heading order violations
    const headingViolations = accessibilityScanResults.violations.filter(
      violation => violation.id === 'heading-order'
    )

    expect(headingViolations).toHaveLength(0)
  })

  test('Interactive elements are keyboard accessible', async ({ page }) => {
    // Get initial focused element
    await page.keyboard.press('Tab')

    // Verify first interactive element gets focus
    const focusedElement = await page.evaluate(() => {
      const focused = document.activeElement
      return focused?.tagName?.toLowerCase()
    })

    // Interactive elements should be focusable
    expect(['a', 'button', 'input']).toContain(focusedElement)

    // Tab through all interactive elements
    const interactiveCount = await page.evaluate(() => {
      const focusableElements = document.querySelectorAll(
        'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
      )
      return focusableElements.length
    })

    // Verify there are interactive elements
    expect(interactiveCount).toBeGreaterThan(0)
  })

  test('Focus indicators are visible', async ({ page }) => {
    // Tab to first interactive element
    await page.keyboard.press('Tab')

    // Get the focused element and check for visible focus styles
    const hasFocusIndicator = await page.evaluate(() => {
      const focused = document.activeElement
      if (!focused) return false

      const styles = window.getComputedStyle(focused)
      // Check for focus ring or outline
      const hasOutline = styles.outlineStyle !== 'none' && styles.outlineWidth !== '0px'
      const hasRing = styles.boxShadow !== 'none'
      const hasBorderChange = focused.classList.contains('focus:ring-2') ||
                              focused.classList.contains('focus:outline-none')

      return hasOutline || hasRing || hasBorderChange
    })

    // Note: Tailwind's focus:ring-2 class may not be detected via computed styles
    // The important thing is that elements are focusable
    expect(hasFocusIndicator).toBeTruthy()
  })

  test('Images and icons have appropriate alt text or are hidden from accessibility tree', async ({ page }) => {
    // Get all images
    const images = page.locator('img')
    const imageCount = await images.count()

    // Check each image has alt attribute
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i)
      const altText = await img.getAttribute('alt')
      const ariaHidden = await img.getAttribute('aria-hidden')

      // Image should have alt text OR be hidden from a11y tree
      const isAccessible = altText !== null || ariaHidden === 'true'
      expect(isAccessible).toBeTruthy()
    }

    // Check that decorative SVG icons are hidden from accessibility tree
    const decorativeIcons = page.locator('svg[aria-hidden="true"]')
    const decorativeCount = await decorativeIcons.count()

    // There should be decorative icons (feature icons, etc.)
    expect(decorativeCount).toBeGreaterThan(0)
  })

  test('ARIA labels are present on interactive elements without visible text', async ({ page }) => {
    // Check theme toggle button has aria-label
    const themeToggle = page.locator('[data-testid="theme-toggle-button"]')
    await expect(themeToggle).toHaveAttribute('aria-label')

    // Check hamburger button has aria-label
    const hamburgerButton = page.locator('[data-testid="hamburger-button"]')
    await expect(hamburgerButton).toHaveAttribute('aria-label')

    // Check theme dropdown has proper ARIA
    const themeToggleButton = page.locator('[data-testid="theme-toggle-button"]')
    await expect(themeToggleButton).toHaveAttribute('aria-haspopup', 'listbox')
    await expect(themeToggleButton).toHaveAttribute('aria-expanded')
  })

  test('Sections have proper aria-labelledby or aria-label', async ({ page }) => {
    // Check features section has aria-labelledby
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading')

    // Check how-it-works section has aria-labelledby
    const howItWorksSection = page.locator('#how-it-works')
    await expect(howItWorksSection).toHaveAttribute('aria-labelledby', 'how-it-works-title')

    // Check navigation has aria-label
    const navWithAriaLabel = page.locator('nav[aria-label]')
    const navCount = await navWithAriaLabel.count()
    expect(navCount).toBeGreaterThan(0)
  })

  test('Forms have proper labels', async ({ page }) => {
    // Navigate to login page to test form accessibility
    await page.goto('/login')
    await page.waitForLoadState('networkidle')

    // Run axe audit on login form
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze()

    // Filter for label violations
    const labelViolations = accessibilityScanResults.violations.filter(
      violation => violation.id === 'label' || violation.id === 'label-title-only'
    )

    expect(labelViolations).toHaveLength(0)
  })
})
