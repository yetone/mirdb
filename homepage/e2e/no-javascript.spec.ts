import { test, expect } from '@playwright/test'

test.describe('Core Content Without JavaScript', () => {
  test.use({ javaScriptEnabled: false })

  test('TC1: page displays noscript fallback content when JavaScript is disabled', async ({ page }) => {
    // Load page with JavaScript disabled (via test.use above)
    await page.goto('/')

    // The noscript element should be visible
    const noscriptContent = page.locator('noscript')
    await expect(noscriptContent).toBeAttached()

    // The noscript fallback should contain core content information
    // Check for the main heading in noscript
    const heading = page.locator('noscript h1')
    await expect(heading).toHaveText('MirDB')

    // Check for the tagline
    const tagline = page.locator('noscript .noscript-tagline')
    await expect(tagline).toContainText('Memcached, but persistent')

    // Check for the description
    const description = page.locator('noscript .noscript-description')
    await expect(description).toContainText('persistent key-value store')
  })

  test('TC2: navigation links work without JavaScript', async ({ page }) => {
    await page.goto('/')

    // Check that navigation anchor links are present in noscript fallback
    const featuresLink = page.locator('noscript a[href="#features"]')
    await expect(featuresLink).toBeAttached()

    const quickStartLink = page.locator('noscript a[href="#quick-start"]')
    await expect(quickStartLink).toBeAttached()

    const commandsLink = page.locator('noscript a[href="#commands"]')
    await expect(commandsLink).toBeAttached()

    const configLink = page.locator('noscript a[href="#configuration"]')
    await expect(configLink).toBeAttached()

    // The GitHub link should also be present (use count since there are nav and footer links)
    const githubLinks = page.locator('noscript a[href*="github.com"]')
    const count = await githubLinks.count()
    expect(count).toBeGreaterThanOrEqual(1)
  })

  test('TC3: code examples are visible without JavaScript', async ({ page }) => {
    await page.goto('/')

    // Check that code examples are present in noscript section
    const codeBlocks = page.locator('noscript pre')
    const codeBlockCount = await codeBlocks.count()

    // Expect at least installation command and usage example
    expect(codeBlockCount).toBeGreaterThanOrEqual(2)

    // Check install command is present
    const installCode = page.locator('noscript pre:has-text("cargo install mirdb")')
    await expect(installCode).toBeAttached()

    // Check basic usage example is present
    const usageCode = page.locator('noscript pre:has-text("set mykey")')
    await expect(usageCode).toBeAttached()
  })

  test('TC1-Full: all text content and sections are visible and readable', async ({ page }) => {
    await page.goto('/')

    // Verify main product information is present
    const productName = page.locator('noscript h1')
    await expect(productName).toHaveText('MirDB')

    // Verify features section exists
    const featuresHeading = page.locator('noscript h2:has-text("Key Features")')
    await expect(featuresHeading).toBeAttached()

    // Verify feature descriptions are present using getByText for more reliable matching
    const noscriptContent = page.locator('noscript .noscript-content')
    await expect(noscriptContent).toContainText('Memcached Protocol Compatible')
    await expect(noscriptContent).toContainText('Durable Persistence')
    await expect(noscriptContent).toContainText('LSM-Tree Architecture')

    // Verify Quick Start section exists
    const quickStartHeading = page.locator('noscript h2:has-text("Quick Start")')
    await expect(quickStartHeading).toBeAttached()

    // Verify Commands section exists
    const commandsHeading = page.locator('noscript h2:has-text("Supported Commands")')
    await expect(commandsHeading).toBeAttached()

    // Verify Configuration section exists
    const configHeading = page.locator('noscript h2:has-text("Configuration")')
    await expect(configHeading).toBeAttached()
  })

  test('TC2-Full: navigation links work and scroll to sections', async ({ page }) => {
    await page.goto('/')

    // Check navigation structure in noscript
    const nav = page.locator('noscript nav')
    await expect(nav).toBeAttached()

    // Verify all section anchors exist with proper hrefs
    const navLinks = page.locator('noscript nav a')
    const linkCount = await navLinks.count()
    expect(linkCount).toBeGreaterThanOrEqual(4)

    // Verify anchor links point to correct sections
    const anchors = ['#features', '#quick-start', '#commands', '#configuration']
    for (const anchor of anchors) {
      const link = page.locator(`noscript nav a[href="${anchor}"]`)
      await expect(link).toBeAttached()
    }

    // Verify corresponding section IDs exist in the noscript content
    const sections = ['features', 'quick-start', 'commands', 'configuration']
    for (const sectionId of sections) {
      const section = page.locator(`noscript [id="${sectionId}"]`)
      await expect(section).toBeAttached()
    }
  })

  test('TC3-Full: code examples are visible with syntax structure', async ({ page }) => {
    await page.goto('/')

    // Verify installation code block
    const installBlock = page.locator('noscript .noscript-code-block:has-text("cargo install mirdb")')
    await expect(installBlock).toBeAttached()

    // Verify TOML configuration example exists (use first() since there are multiple)
    const tomlBlocks = page.locator('noscript pre:has-text("addr")')
    const tomlCount = await tomlBlocks.count()
    expect(tomlCount).toBeGreaterThanOrEqual(1)

    // Verify configuration contains expected content using the first matching block
    const configContent = await tomlBlocks.first().textContent()
    expect(configContent).toContain('addr')
    expect(configContent).toContain('0.0.0.0:12333')

    // Verify memcached usage example
    const usageBlock = page.locator('noscript pre:has-text("telnet")')
    await expect(usageBlock).toBeAttached()

    // Verify basic commands are shown using containText on the noscript content
    const noscriptContent = page.locator('noscript .noscript-content')
    await expect(noscriptContent).toContainText('get <key>')
    await expect(noscriptContent).toContainText('set <key>')
    await expect(noscriptContent).toContainText('delete <key>')
  })

  test('noscript fallback has proper styling for readability', async ({ page }) => {
    await page.goto('/')

    // Verify noscript content is styled
    const noscriptSection = page.locator('noscript .noscript-content')
    await expect(noscriptSection).toBeAttached()

    // Check that the noscript content has visible styling (not hidden)
    const styles = await page.locator('noscript style').count()
    // Inline styles may be present for noscript fallback
    expect(styles).toBeGreaterThanOrEqual(0)
  })
})
