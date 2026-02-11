/**
 * Unit Tests for Image Accessibility
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test Case 5: All img elements have non-empty alt text or role='presentation'
 */

import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import App from '../../../src/App'

// Mock CSS imports
vi.mock('../../../src/styles/globals.css', () => ({}))
vi.mock('../../../src/components/layout/Header.css', () => ({}))
vi.mock('../../../src/components/layout/Footer.css', () => ({}))
vi.mock('../../../src/components/layout/Navigation.css', () => ({}))
vi.mock('../../../src/components/layout/MobileMenu.css', () => ({}))
vi.mock('../../../src/components/sections/Hero.css', () => ({}))
vi.mock('../../../src/components/sections/Demo.css', () => ({}))
vi.mock('../../../src/components/sections/Features.css', () => ({}))
vi.mock('../../../src/components/sections/QuickStart.css', () => ({}))
vi.mock('../../../src/components/ui/Button.css', () => ({}))
vi.mock('../../../src/components/ui/Badge.css', () => ({}))
vi.mock('../../../src/components/ui/FeatureCard.css', () => ({}))
vi.mock('../../../src/components/ui/CodeBlock.css', () => ({}))
vi.mock('../../../src/components/ui/ThemeToggle.css', () => ({}))

describe('Test Case 5: Image Alt Text', () => {
  beforeEach(() => {
    render(<App />)
  })

  it('all images have alt attributes', () => {
    const images = document.querySelectorAll('img')

    images.forEach((img) => {
      const alt = img.getAttribute('alt')
      const role = img.getAttribute('role')
      const ariaHidden = img.getAttribute('aria-hidden')

      // Image should have alt text OR role="presentation" OR be aria-hidden
      const hasAccessibleAlternative =
        (alt !== null && alt.trim() !== '') || role === 'presentation' || ariaHidden === 'true'

      const src = img.getAttribute('src')

      expect(
        hasAccessibleAlternative,
        `Image with src "${src}" should have non-empty alt text, role="presentation", or aria-hidden="true"`
      ).toBe(true)
    })
  })

  it('logo image has appropriate alt text', () => {
    const logoImg = document.querySelector('img[src*="logo"]') as HTMLImageElement

    if (logoImg) {
      const alt = logoImg.getAttribute('alt')
      expect(alt).not.toBeNull()
      expect(alt).not.toBe('')
      expect(alt?.toLowerCase()).toContain('mirdb')
    }
  })

  it('demo GIF has descriptive alt text', () => {
    const demoImg = document.querySelector('img[src*="usage"]') as HTMLImageElement

    if (demoImg) {
      const alt = demoImg.getAttribute('alt')
      expect(alt).not.toBeNull()
      expect(alt).not.toBe('')
      // Alt text should describe what the demo shows (should be reasonably descriptive)
      expect(alt!.length).toBeGreaterThan(20)
    }
  })

  it('informative images have meaningful alt text (not generic)', () => {
    const images = document.querySelectorAll('img')

    images.forEach((img) => {
      const alt = img.getAttribute('alt')
      const role = img.getAttribute('role')
      const ariaHidden = img.getAttribute('aria-hidden')

      // Skip decorative images
      if (role === 'presentation' || ariaHidden === 'true' || alt === '') {
        return
      }

      if (alt) {
        // Alt text should not be just generic placeholder text
        const genericAltPatterns = [/^image$/i, /^photo$/i, /^picture$/i, /^img$/i, /\.(?:gif|jpg|jpeg|png|webp)$/i]

        const isGeneric = genericAltPatterns.some((pattern) => pattern.test(alt.trim()))

        expect(isGeneric, `Image alt text "${alt}" appears to be generic placeholder text`).toBe(false)
      }
    })
  })
})
