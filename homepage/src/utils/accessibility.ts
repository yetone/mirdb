/**
 * Accessibility utility functions.
 * Owner: Scenario 13 - Accessibility - Screen Reader Compatibility
 *
 * Provides utilities for accessibility features:
 * - Focus management
 * - ARIA attribute helpers
 * - Screen reader announcements
 */

/**
 * Announces a message to screen readers using an ARIA live region.
 * Creates a temporary element that is read by assistive technology.
 *
 * @param message - The message to announce to screen readers
 * @param politeness - The urgency level: 'polite' (default) or 'assertive'
 */
export function announceToScreenReader(
  message: string,
  politeness: 'polite' | 'assertive' = 'polite'
): void {
  // Create a live region element
  const announcement = document.createElement('div')
  announcement.setAttribute('role', 'status')
  announcement.setAttribute('aria-live', politeness)
  announcement.setAttribute('aria-atomic', 'true')

  // Make it visually hidden but accessible to screen readers
  announcement.style.cssText = `
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

  document.body.appendChild(announcement)

  // Set the message after a brief delay to ensure it's announced
  requestAnimationFrame(() => {
    announcement.textContent = message
  })

  // Clean up after announcement is made
  setTimeout(() => {
    announcement.remove()
  }, 1000)
}

/**
 * Traps focus within a specific element, useful for modals and dialogs.
 * Returns a cleanup function to restore normal tab behavior.
 *
 * @param element - The container element to trap focus within
 * @returns A cleanup function that removes the focus trap
 */
export function trapFocus(element: HTMLElement): () => void {
  const focusableSelectors = [
    'a[href]',
    'button:not([disabled])',
    'input:not([disabled])',
    'select:not([disabled])',
    'textarea:not([disabled])',
    '[tabindex]:not([tabindex="-1"])',
  ].join(', ')

  const focusableElements = element.querySelectorAll<HTMLElement>(focusableSelectors)
  const firstFocusable = focusableElements[0]
  const lastFocusable = focusableElements[focusableElements.length - 1]

  function handleKeyDown(event: KeyboardEvent): void {
    if (event.key !== 'Tab') return

    if (event.shiftKey) {
      // Shift + Tab: if on first element, go to last
      if (document.activeElement === firstFocusable) {
        event.preventDefault()
        lastFocusable?.focus()
      }
    } else {
      // Tab: if on last element, go to first
      if (document.activeElement === lastFocusable) {
        event.preventDefault()
        firstFocusable?.focus()
      }
    }
  }

  element.addEventListener('keydown', handleKeyDown)

  // Focus the first element when trap is activated
  firstFocusable?.focus()

  // Return cleanup function
  return () => {
    element.removeEventListener('keydown', handleKeyDown)
  }
}

/**
 * Validates that the page has proper heading hierarchy (no skipped levels).
 * Used for accessibility auditing.
 *
 * @returns An object with validation result and any issues found
 */
export function validateHeadingHierarchy(): {
  valid: boolean
  issues: string[]
} {
  const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6')
  const issues: string[] = []
  let lastLevel = 0
  let h1Count = 0

  headings.forEach((heading) => {
    const level = parseInt(heading.tagName.charAt(1), 10)

    if (level === 1) {
      h1Count++
      if (h1Count > 1) {
        issues.push(`Multiple h1 elements found (found ${h1Count})`)
      }
    }

    // Check for skipped levels (e.g., h1 directly to h3)
    if (lastLevel > 0 && level > lastLevel + 1) {
      issues.push(`Skipped heading level: h${lastLevel} to h${level}`)
    }

    lastLevel = level
  })

  if (h1Count === 0) {
    issues.push('No h1 element found on page')
  }

  return {
    valid: issues.length === 0,
    issues,
  }
}

/**
 * Checks if all images on the page have alt attributes.
 *
 * @returns An object with validation result and any images missing alt text
 */
export function validateImageAltText(): {
  valid: boolean
  imagesWithoutAlt: HTMLImageElement[]
} {
  const images = document.querySelectorAll('img')
  const imagesWithoutAlt: HTMLImageElement[] = []

  images.forEach((img) => {
    if (!img.hasAttribute('alt')) {
      imagesWithoutAlt.push(img)
    }
  })

  return {
    valid: imagesWithoutAlt.length === 0,
    imagesWithoutAlt,
  }
}

/**
 * Checks if all icon-only buttons have accessible names (aria-label or text content).
 *
 * @returns An object with validation result and any buttons missing accessible names
 */
export function validateIconButtonLabels(): {
  valid: boolean
  buttonsWithoutLabels: HTMLButtonElement[]
} {
  const buttons = document.querySelectorAll('button')
  const buttonsWithoutLabels: HTMLButtonElement[] = []

  buttons.forEach((button) => {
    const hasTextContent = button.textContent?.trim()
    const hasAriaLabel = button.getAttribute('aria-label')
    const hasAriaLabelledBy = button.getAttribute('aria-labelledby')
    const hasTitle = button.getAttribute('title')

    // Check if button has only icon (svg, img, or icon class) and no text
    const hasOnlyIcon =
      !hasTextContent &&
      (button.querySelector('svg') !== null || button.querySelector('img') !== null)

    if (hasOnlyIcon && !hasAriaLabel && !hasAriaLabelledBy && !hasTitle) {
      buttonsWithoutLabels.push(button)
    }
  })

  return {
    valid: buttonsWithoutLabels.length === 0,
    buttonsWithoutLabels,
  }
}

/**
 * Validates that the page has required landmark regions.
 *
 * @returns An object with validation result and details about landmarks
 */
export function validateLandmarks(): {
  hasMain: boolean
  hasHeader: boolean
  hasNav: boolean
  hasFooter: boolean
} {
  const main = document.querySelector('main, [role="main"]')
  const header = document.querySelector('header, [role="banner"]')
  const nav = document.querySelector('nav, [role="navigation"]')
  const footer = document.querySelector('footer, [role="contentinfo"]')

  return {
    hasMain: main !== null,
    hasHeader: header !== null,
    hasNav: nav !== null,
    hasFooter: footer !== null,
  }
}
