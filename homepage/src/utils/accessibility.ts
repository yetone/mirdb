/**
 * Accessibility utility functions.
 * Owner: Scenario 8 - Accessibility Compliance
 *
 * Requirements:
 * - WCAG 2.1 AA compliance helpers (REQ-8, NFR-6)
 * - Color contrast checking
 * - Focus management utilities
 * - Screen reader helpers
 */

/**
 * Parses a hex color string to RGB components
 */
export function hexToRgb(hex: string): { r: number; g: number; b: number } | null {
  // Remove # if present
  const cleanHex = hex.replace(/^#/, '')

  // Handle shorthand hex
  let fullHex = cleanHex
  if (cleanHex.length === 3) {
    fullHex = cleanHex
      .split('')
      .map((char) => char + char)
      .join('')
  }

  if (fullHex.length !== 6) {
    return null
  }

  const result = /^([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(fullHex)
  return result
    ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16),
      }
    : null
}

/**
 * Calculates the relative luminance of a color according to WCAG 2.1
 * https://www.w3.org/TR/WCAG21/#dfn-relative-luminance
 */
export function getLuminance(r: number, g: number, b: number): number {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const sRGB = c / 255
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4)
  })
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs
}

/**
 * Calculates the contrast ratio between two colors
 * Returns the ratio as a number (e.g., 4.5 for 4.5:1)
 */
export function getContrastRatio(
  color1: { r: number; g: number; b: number },
  color2: { r: number; g: number; b: number }
): number {
  const l1 = getLuminance(color1.r, color1.g, color1.b)
  const l2 = getLuminance(color2.r, color2.g, color2.b)
  const lighter = Math.max(l1, l2)
  const darker = Math.min(l1, l2)
  return (lighter + 0.05) / (darker + 0.05)
}

/**
 * Checks if two colors meet WCAG 2.1 AA contrast requirements
 * Normal text requires 4.5:1, large text requires 3:1
 */
export function checkContrast(
  fg: string,
  bg: string,
  isLargeText = false
): { ratio: number; passes: boolean; level: 'AA' | 'AAA' | 'fail' } {
  const fgRgb = hexToRgb(fg)
  const bgRgb = hexToRgb(bg)

  if (!fgRgb || !bgRgb) {
    return { ratio: 0, passes: false, level: 'fail' }
  }

  const ratio = getContrastRatio(fgRgb, bgRgb)
  const aaThreshold = isLargeText ? 3 : 4.5
  const aaaThreshold = isLargeText ? 4.5 : 7

  let level: 'AA' | 'AAA' | 'fail' = 'fail'
  if (ratio >= aaaThreshold) {
    level = 'AAA'
  } else if (ratio >= aaThreshold) {
    level = 'AA'
  }

  return {
    ratio: Math.round(ratio * 100) / 100,
    passes: ratio >= aaThreshold,
    level,
  }
}

/**
 * Traps focus within a container element
 * Returns a cleanup function to restore normal focus behavior
 */
export function trapFocus(container: HTMLElement): () => void {
  const focusableSelectors = [
    'a[href]:not([tabindex="-1"])',
    'button:not([disabled]):not([tabindex="-1"])',
    'textarea:not([disabled]):not([tabindex="-1"])',
    'input:not([disabled]):not([tabindex="-1"])',
    'select:not([disabled]):not([tabindex="-1"])',
    '[tabindex]:not([tabindex="-1"])',
  ].join(', ')

  const focusableElements = container.querySelectorAll<HTMLElement>(focusableSelectors)
  const firstFocusable = focusableElements[0]
  const lastFocusable = focusableElements[focusableElements.length - 1]

  function handleKeyDown(e: KeyboardEvent) {
    if (e.key !== 'Tab') return

    if (e.shiftKey) {
      // Shift + Tab
      if (document.activeElement === firstFocusable) {
        e.preventDefault()
        lastFocusable?.focus()
      }
    } else {
      // Tab
      if (document.activeElement === lastFocusable) {
        e.preventDefault()
        firstFocusable?.focus()
      }
    }
  }

  container.addEventListener('keydown', handleKeyDown)
  firstFocusable?.focus()

  return () => {
    container.removeEventListener('keydown', handleKeyDown)
  }
}

/**
 * Announces a message to screen readers using an ARIA live region
 */
export function announceToScreenReader(
  message: string,
  priority: 'polite' | 'assertive' = 'polite'
): void {
  const announcer = document.createElement('div')
  announcer.setAttribute('role', 'status')
  announcer.setAttribute('aria-live', priority)
  announcer.setAttribute('aria-atomic', 'true')
  announcer.className = 'sr-only'
  announcer.style.cssText = `
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
  `

  document.body.appendChild(announcer)

  // Delay setting the message to ensure screen readers pick it up
  setTimeout(() => {
    announcer.textContent = message
  }, 100)

  // Remove the announcer after the message is read
  setTimeout(() => {
    document.body.removeChild(announcer)
  }, 1000)
}

/**
 * Generates an appropriate ARIA label for an element
 */
export function generateAriaLabel(element: string, context?: string): string {
  if (!context) {
    return element
  }
  return `${element}, ${context}`
}

/**
 * Checks if an element has a visible focus indicator
 * This is a utility for testing purposes
 */
export function hasFocusIndicator(element: HTMLElement): boolean {
  const styles = window.getComputedStyle(element)

  // Check for common focus indicators
  const hasOutline = styles.outlineStyle !== 'none' && styles.outlineWidth !== '0px'
  const hasBoxShadow = styles.boxShadow !== 'none'
  const hasBorderChange = styles.borderStyle !== 'none'
  const hasRingUtility =
    styles.boxShadow.includes('ring') || styles.boxShadow.includes('0 0 0')

  return hasOutline || hasBoxShadow || hasBorderChange || hasRingUtility
}

/**
 * Validates heading hierarchy on a page
 * Returns an array of issues found
 */
export function validateHeadingHierarchy(container: HTMLElement = document.body): {
  isValid: boolean
  issues: string[]
  headings: { level: number; text: string }[]
} {
  const headings = container.querySelectorAll<HTMLHeadingElement>('h1, h2, h3, h4, h5, h6')
  const issues: string[] = []
  const headingList: { level: number; text: string }[] = []

  let previousLevel = 0
  let h1Count = 0

  headings.forEach((heading) => {
    const level = parseInt(heading.tagName.charAt(1), 10)
    const text = heading.textContent?.trim() || ''

    headingList.push({ level, text })

    if (level === 1) {
      h1Count++
    }

    // Check for skipped heading levels (e.g., h1 to h3)
    if (previousLevel > 0 && level > previousLevel + 1) {
      issues.push(
        `Heading level skipped: h${previousLevel} to h${level} ("${text}")`
      )
    }

    previousLevel = level
  })

  // Check for multiple h1s or missing h1
  if (h1Count === 0) {
    issues.push('No h1 heading found on the page')
  } else if (h1Count > 1) {
    issues.push(`Multiple h1 headings found (${h1Count})`)
  }

  return {
    isValid: issues.length === 0,
    issues,
    headings: headingList,
  }
}

/**
 * Validates that all images have appropriate alt text
 */
export function validateImageAltText(container: HTMLElement = document.body): {
  isValid: boolean
  issues: string[]
  images: { src: string; alt: string | null; hasAlt: boolean }[]
} {
  const images = container.querySelectorAll<HTMLImageElement>('img')
  const issues: string[] = []
  const imageList: { src: string; alt: string | null; hasAlt: boolean }[] = []

  images.forEach((img, index) => {
    const hasAltAttribute = img.hasAttribute('alt')
    const alt = img.getAttribute('alt')
    const src = img.src || img.getAttribute('src') || `[image ${index + 1}]`

    imageList.push({
      src,
      alt,
      hasAlt: hasAltAttribute,
    })

    if (!hasAltAttribute) {
      issues.push(`Image missing alt attribute: ${src}`)
    }
  })

  return {
    isValid: issues.length === 0,
    issues,
    images: imageList,
  }
}

/**
 * Validates that icon-only buttons have proper accessible names
 */
export function validateButtonAccessibility(container: HTMLElement = document.body): {
  isValid: boolean
  issues: string[]
} {
  const buttons = container.querySelectorAll<HTMLElement>('button, [role="button"]')
  const issues: string[] = []

  buttons.forEach((button, index) => {
    const hasAriaLabel = button.hasAttribute('aria-label')
    const hasAriaLabelledby = button.hasAttribute('aria-labelledby')
    const textContent = button.textContent?.trim()
    const srOnlyText = button.querySelector('.sr-only')?.textContent?.trim()

    const hasAccessibleName =
      hasAriaLabel ||
      hasAriaLabelledby ||
      (textContent && textContent.length > 0) ||
      (srOnlyText && srOnlyText.length > 0)

    if (!hasAccessibleName) {
      const identifier = button.id || button.className || `button ${index + 1}`
      issues.push(`Button without accessible name: ${identifier}`)
    }
  })

  return {
    isValid: issues.length === 0,
    issues,
  }
}

/**
 * Gets all focusable elements within a container
 */
export function getFocusableElements(container: HTMLElement = document.body): HTMLElement[] {
  const focusableSelectors = [
    'a[href]:not([tabindex="-1"])',
    'button:not([disabled]):not([tabindex="-1"])',
    'textarea:not([disabled]):not([tabindex="-1"])',
    'input:not([disabled]):not([tabindex="-1"])',
    'select:not([disabled]):not([tabindex="-1"])',
    '[tabindex]:not([tabindex="-1"])',
  ].join(', ')

  return Array.from(container.querySelectorAll<HTMLElement>(focusableSelectors))
}

/**
 * Checks if focus order is logical (follows DOM order by default)
 */
export function validateFocusOrder(container: HTMLElement = document.body): {
  isValid: boolean
  elements: HTMLElement[]
  issues: string[]
} {
  const elements = getFocusableElements(container)
  const issues: string[] = []

  // Check for positive tabindex values which can disrupt natural focus order
  elements.forEach((element) => {
    const tabindex = element.getAttribute('tabindex')
    if (tabindex && parseInt(tabindex, 10) > 0) {
      const identifier = element.id || element.textContent?.slice(0, 20) || element.tagName
      issues.push(`Positive tabindex found on element: ${identifier} (tabindex="${tabindex}")`)
    }
  })

  return {
    isValid: issues.length === 0,
    elements,
    issues,
  }
}
