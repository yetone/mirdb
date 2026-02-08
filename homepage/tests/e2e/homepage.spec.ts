/**
 * Homepage E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 */
import { test, expect } from '@playwright/test'

test.describe('Homepage - Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('loads successfully with correct title', async ({ page }) => {
    await expect(page).toHaveTitle(/MirDB/)
  })

  test('displays MirDB product name in hero section', async ({ page }) => {
    const productName = page.locator('.hero h1')
    await expect(productName).toBeVisible()
    await expect(productName).toHaveText('MirDB')
  })

  test('displays tagline with Persistent Key-Value Store and Memcached', async ({ page }) => {
    const tagline = page.locator('.hero-tagline')
    await expect(tagline).toBeVisible()
    await expect(tagline).toContainText('Persistent Key-Value Store')
    await expect(tagline).toContainText('Memcached')
  })

  test('displays logo.gif image', async ({ page }) => {
    const logo = page.locator('.hero-logo-image')
    await expect(logo).toBeVisible()
    const src = await logo.getAttribute('src')
    expect(src).toContain('logo.gif')
  })

  test('hero section CTA button navigates to GitHub', async ({ page }) => {
    const ctaButton = page.locator('.hero-cta')
    await expect(ctaButton).toBeVisible()
    const href = await ctaButton.getAttribute('href')
    expect(href).toBe('https://github.com/yetone/mirdb')
    // Verify it opens in new tab
    const target = await ctaButton.getAttribute('target')
    expect(target).toBe('_blank')
  })

  test('header contains navigation to GitHub', async ({ page }) => {
    const githubNav = page.getByRole('link', { name: 'GitHub' }).first()
    await expect(githubNav).toBeVisible()
    const href = await githubNav.getAttribute('href')
    expect(href).toContain('github.com/yetone/mirdb')
  })
})
