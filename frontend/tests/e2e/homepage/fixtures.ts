/**
 * E2E Test Fixtures and Page Objects
 * Owner: First builder
 *
 * Playwright page objects and fixtures for homepage E2E tests.
 */

import { Page, Locator } from '@playwright/test'

export class HomePage {
  readonly page: Page
  readonly heroSection: Locator
  readonly heroHeadline: Locator
  readonly heroSubheadline: Locator
  readonly getStartedButton: Locator
  readonly signInButton: Locator
  readonly navbar: Locator

  constructor(page: Page) {
    this.page = page
    this.heroSection = page.getByTestId('hero-section')
    this.heroHeadline = page.getByTestId('hero-headline')
    this.heroSubheadline = page.getByTestId('hero-subheadline')
    this.getStartedButton = page.getByTestId('get-started-button')
    this.signInButton = page.getByTestId('sign-in-button')
    this.navbar = page.getByRole('navigation')
  }

  async goto() {
    await this.page.goto('/')
  }

  async clickGetStarted() {
    await this.getStartedButton.click()
  }

  async clickSignIn() {
    await this.signInButton.click()
  }
}

export const viewports = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 720 },
  desktopLarge: { width: 1920, height: 1080 },
}

export const testUrls = {
  home: '/',
  register: '/register',
  login: '/login',
  dashboard: '/dashboard',
}
