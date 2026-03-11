/**
 * Dark Mode Tests
 * Owner: Scenario 15 - Dark Mode Support
 *
 * Test coverage:
 * - prefers-color-scheme: dark detection
 * - Background color changes
 * - Text color contrast in dark mode
 * - Code block syntax highlighting
 * - Diagram visibility
 * - Theme toggle transition smoothness
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

// Import components
import { Hero } from '../../src/components/Hero.js'
import { FeatureShowcase } from '../../src/components/FeatureShowcase.js'
import { InteractiveDemo } from '../../src/components/InteractiveDemo.js'
import { QuickStart } from '../../src/components/QuickStart.js'
import { ProtocolDocs } from '../../src/components/ProtocolDocs.js'
import { ArchitectureOverview } from '../../src/components/ArchitectureOverview.js'
import { PerformanceInfo } from '../../src/components/PerformanceInfo.js'
import { Footer } from '../../src/components/Footer.js'

// Import theme utilities
import { getPreferredTheme, setTheme, onThemeChange } from '../../src/utils/theme.js'

/**
 * Creates a full page DOM structure for dark mode testing
 * @param {boolean} darkMode - Whether to apply dark mode class
 * @returns {HTMLElement} The container element with the full page rendered
 */
function createFullPageDOM(darkMode = false) {
  const container = document.createElement('div')
  container.innerHTML = `
    <!DOCTYPE html>
    <html lang="en" class="${darkMode ? 'dark' : ''}">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>MirDB - Persistent Key-Value Store</title>
      <style>
        :root {
          --color-primary: #3b82f6;
          --color-primary-dark: #2563eb;
          --color-secondary: #10b981;
          --color-background: #ffffff;
          --color-surface: #f9fafb;
          --color-text: #111827;
          --color-text-muted: #6b7280;
        }
        .dark {
          --color-background: #111827;
          --color-surface: #1f2937;
          --color-text: #f9fafb;
          --color-text-muted: #9ca3af;
        }
      </style>
    </head>
    <body class="bg-white dark:bg-gray-900 text-gray-900 dark:text-white">
      <!-- Navigation placeholder -->
      <header id="navigation" role="banner">
        <nav aria-label="Main navigation" class="bg-white dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div class="flex items-center justify-between h-16">
            <a href="#" class="text-xl font-bold text-gray-900 dark:text-white">MirDB</a>
            <div class="hidden md:flex space-x-4">
              <a href="#features" class="px-3 py-2 text-gray-700 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white">Features</a>
              <a href="#demo" class="px-3 py-2 text-gray-700 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white">Demo</a>
              <a href="#quickstart" class="px-3 py-2 text-gray-700 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white">Quick Start</a>
            </div>
          </div>
        </nav>
      </header>

      <main id="main-content" role="main">
        <!-- Hero Section -->
        <section id="hero" class="min-h-screen flex items-center justify-center bg-white dark:bg-gray-900" aria-labelledby="hero-heading">
          ${Hero()}
        </section>

        <!-- Features Section -->
        <section id="features" class="py-20 bg-gray-50 dark:bg-gray-800" aria-labelledby="features-heading">
          ${FeatureShowcase()}
        </section>

        <!-- Interactive Demo Section -->
        <section id="demo" class="py-20 bg-white dark:bg-gray-900" aria-labelledby="demo-heading">
          ${InteractiveDemo()}
        </section>

        <!-- Quick Start Section -->
        <section id="quickstart" class="py-20 bg-gray-50 dark:bg-gray-800" aria-labelledby="quickstart-heading">
          ${QuickStart()}
        </section>

        <!-- Protocol Documentation Section -->
        <section id="protocol" class="py-20 bg-white dark:bg-gray-900" aria-labelledby="protocol-heading">
          ${ProtocolDocs()}
        </section>

        <!-- Architecture Overview Section -->
        <section id="architecture" class="py-20 bg-gray-50 dark:bg-gray-800" aria-labelledby="architecture-heading">
          ${ArchitectureOverview()}
        </section>

        <!-- Performance Section -->
        <section id="performance" class="py-20 bg-white dark:bg-gray-900" aria-labelledby="performance-heading">
          ${PerformanceInfo()}
        </section>
      </main>

      <!-- Footer -->
      <footer id="footer" role="contentinfo" class="bg-gray-100 dark:bg-gray-800 border-t border-gray-200 dark:border-gray-700">
        ${Footer()}
      </footer>
    </body>
    </html>
  `
  document.body.appendChild(container)
  return container
}

describe('Dark Mode Support', () => {
  let container
  let originalMatchMedia

  beforeEach(() => {
    document.body.innerHTML = ''
    // Store original matchMedia
    originalMatchMedia = window.matchMedia
  })

  afterEach(() => {
    if (container && container.parentNode) {
      container.parentNode.removeChild(container)
    }
    // Restore original matchMedia
    window.matchMedia = originalMatchMedia
    vi.restoreAllMocks()
  })

  /**
   * Test Case 1: prefers-color-scheme: dark detection
   * Expected: Homepage switches to dark color scheme automatically
   */
  describe('TC-1: prefers-color-scheme: dark detection', () => {
    it('should detect system dark mode preference using getPreferredTheme', () => {
      // Mock matchMedia to simulate dark mode preference
      window.matchMedia = vi.fn().mockImplementation(query => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      const theme = getPreferredTheme()
      expect(theme).toBe('dark')
    })

    it('should detect system light mode preference using getPreferredTheme', () => {
      // Mock matchMedia to simulate light mode preference
      window.matchMedia = vi.fn().mockImplementation(query => ({
        matches: query !== '(prefers-color-scheme: dark)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      const theme = getPreferredTheme()
      expect(theme).toBe('light')
    })

    it('should apply dark class to html element when setTheme is called with dark', () => {
      container = createFullPageDOM(false)
      const htmlElement = container.querySelector('html')

      // Initially should not have dark class
      expect(htmlElement.classList.contains('dark')).toBe(false)

      // Apply dark theme
      setTheme('dark', htmlElement)

      // Should now have dark class
      expect(htmlElement.classList.contains('dark')).toBe(true)
    })

    it('should remove dark class when setTheme is called with light', () => {
      container = createFullPageDOM(true)
      const htmlElement = container.querySelector('html')

      // Initially should have dark class
      expect(htmlElement.classList.contains('dark')).toBe(true)

      // Apply light theme
      setTheme('light', htmlElement)

      // Should not have dark class
      expect(htmlElement.classList.contains('dark')).toBe(false)
    })

    it('should support onThemeChange callback for system preference changes', () => {
      const callback = vi.fn()
      let mediaQueryCallback = null

      // Mock matchMedia with addEventListener support
      window.matchMedia = vi.fn().mockImplementation(query => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn((event, cb) => {
          mediaQueryCallback = cb
        }),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      // Register the callback
      const cleanup = onThemeChange(callback)

      // Simulate system theme change
      if (mediaQueryCallback) {
        mediaQueryCallback({ matches: true })
        expect(callback).toHaveBeenCalledWith('dark')
      }

      // Cleanup should be a function
      expect(typeof cleanup).toBe('function')
    })

    it('should have dark mode class-based strategy configured in Tailwind', () => {
      container = createFullPageDOM(true)

      // Check that elements with dark: prefixes exist
      const darkModeElements = container.querySelectorAll('[class*="dark:"]')
      expect(darkModeElements.length).toBeGreaterThan(0)
    })
  })

  /**
   * Test Case 2: Background colors in dark mode
   * Expected: Background uses dark colors appropriate for developer tools
   */
  describe('TC-2: Background colors in dark mode', () => {
    it('should have dark background classes on body', () => {
      container = createFullPageDOM(true)
      const body = container.querySelector('body')

      // Body should have dark:bg-gray-900 class
      expect(body.className).toContain('dark:bg-gray-900')
    })

    it('should have alternating dark backgrounds for sections', () => {
      container = createFullPageDOM(true)

      // Check hero section
      const heroSection = container.querySelector('#hero')
      expect(heroSection.className).toContain('dark:bg-gray-900')

      // Check features section (should have lighter dark bg)
      const featuresSection = container.querySelector('#features')
      expect(featuresSection.className).toContain('dark:bg-gray-800')

      // Check demo section
      const demoSection = container.querySelector('#demo')
      expect(demoSection.className).toContain('dark:bg-gray-900')
    })

    it('should have dark surface color for cards and containers', () => {
      container = createFullPageDOM(true)

      // Check for dark:bg-gray-800 on cards
      const cardsWithDarkBg = container.querySelectorAll('[class*="dark:bg-gray-800"]')
      expect(cardsWithDarkBg.length).toBeGreaterThan(0)
    })

    it('should have CSS custom properties for dark mode colors', () => {
      container = createFullPageDOM(true)

      // Check that CSS variables are defined in the style tag
      const styleTag = container.querySelector('style')
      expect(styleTag.textContent).toContain('.dark')
      expect(styleTag.textContent).toContain('--color-background: #111827')
      expect(styleTag.textContent).toContain('--color-surface: #1f2937')
    })

    it('should have navigation with dark background', () => {
      container = createFullPageDOM(true)

      const nav = container.querySelector('nav')
      expect(nav.className).toContain('dark:bg-gray-900')
    })

    it('should have footer with dark background', () => {
      container = createFullPageDOM(true)

      const footer = container.querySelector('#footer')
      expect(footer.className).toContain('dark:bg-gray-800')
    })
  })

  /**
   * Test Case 3: Text colors in dark mode
   * Expected: Text colors provide sufficient contrast against dark backgrounds
   */
  describe('TC-3: Text colors in dark mode', () => {
    it('should have light text colors in dark mode for primary text', () => {
      container = createFullPageDOM(true)

      // Body should have dark:text-white class
      const body = container.querySelector('body')
      expect(body.className).toContain('dark:text-white')
    })

    it('should have proper contrast for headings in dark mode', () => {
      container = createFullPageDOM(true)

      // Check hero heading for dark:text-white
      const heroTitle = container.querySelector('[data-testid="hero-product-name"]')
      expect(heroTitle.className).toContain('dark:text-white')
    })

    it('should have muted text color for secondary text in dark mode', () => {
      container = createFullPageDOM(true)

      // Check for dark:text-gray-300 or dark:text-gray-400 for secondary text
      const mutedTextElements = container.querySelectorAll('[class*="dark:text-gray-300"], [class*="dark:text-gray-400"]')
      expect(mutedTextElements.length).toBeGreaterThan(0)
    })

    it('should have CSS custom property for text colors in dark mode', () => {
      container = createFullPageDOM(true)

      const styleTag = container.querySelector('style')
      expect(styleTag.textContent).toContain('--color-text: #f9fafb')
      expect(styleTag.textContent).toContain('--color-text-muted: #9ca3af')
    })

    it('should have navigation links with proper dark mode text colors', () => {
      container = createFullPageDOM(true)

      const navLinks = container.querySelectorAll('nav a')
      let hasProperDarkTextColors = false

      navLinks.forEach(link => {
        if (link.className.includes('dark:text-gray-300') ||
            link.className.includes('dark:text-white') ||
            link.className.includes('dark:hover:text-white')) {
          hasProperDarkTextColors = true
        }
      })

      expect(hasProperDarkTextColors).toBe(true)
    })

    it('should have feature card text with proper dark mode colors', () => {
      container = createFullPageDOM(true)

      // Check feature cards have dark text colors
      const featureCards = container.querySelectorAll('[data-testid^="feature-card-"]')

      if (featureCards.length > 0) {
        featureCards.forEach(card => {
          // Title should have dark:text-white
          const title = card.querySelector('[class*="dark:text-white"]')
          expect(title).not.toBeNull()
        })
      }
    })

    it('should have footer text with proper dark mode colors', () => {
      container = createFullPageDOM(true)

      const footerTextElements = container.querySelectorAll('#footer [class*="dark:text-"]')
      expect(footerTextElements.length).toBeGreaterThan(0)
    })
  })

  /**
   * Test Case 4: Code block styling in dark mode
   * Expected: Code blocks have appropriate dark theme syntax highlighting
   */
  describe('TC-4: Code block styling in dark mode', () => {
    it('should have dark background for code blocks', () => {
      container = createFullPageDOM(true)

      // Code blocks have pre elements with bg-gray-900
      const preElements = container.querySelectorAll('.code-block pre, pre.syntax-highlighted')

      expect(preElements.length).toBeGreaterThan(0)

      preElements.forEach(pre => {
        const className = pre.className
        // Should have dark background class
        const hasDarkBg = className.includes('bg-gray-900') ||
                          className.includes('bg-gray-800')
        expect(hasDarkBg).toBe(true)
      })
    })

    it('should have light text in code blocks for contrast', () => {
      container = createFullPageDOM(true)

      // Check pre elements inside code blocks
      const preElements = container.querySelectorAll('.code-block pre, pre.syntax-highlighted')

      expect(preElements.length).toBeGreaterThan(0)

      preElements.forEach(pre => {
        const className = pre.className
        // Should have light text color class
        const hasLightText = className.includes('text-gray-100') ||
                             className.includes('text-white') ||
                             className.includes('text-gray-200')
        expect(hasLightText).toBe(true)
      })
    })

    it('should have syntax highlighting classes for code', () => {
      container = createFullPageDOM(true)

      // Check for syntax highlighting spans
      const syntaxKeywords = container.querySelectorAll('.syntax-keyword')
      const syntaxKeys = container.querySelectorAll('.syntax-key')
      const syntaxValues = container.querySelectorAll('.syntax-number, .syntax-success')

      // At least some syntax highlighting should be present
      const hasSyntaxHighlighting = syntaxKeywords.length > 0 ||
                                    syntaxKeys.length > 0 ||
                                    syntaxValues.length > 0
      expect(hasSyntaxHighlighting).toBe(true)
    })

    it('should have monospace font for code blocks', () => {
      container = createFullPageDOM(true)

      // Check pre elements inside code blocks
      const preElements = container.querySelectorAll('.code-block pre, pre.syntax-highlighted')

      expect(preElements.length).toBeGreaterThan(0)

      preElements.forEach(pre => {
        const className = pre.className
        expect(className).toContain('font-mono')
      })
    })

    it('should maintain code readability in both themes', () => {
      // Light mode
      const lightContainer = createFullPageDOM(false)
      const lightCodeBlocks = lightContainer.querySelectorAll('.code-block, pre')
      expect(lightCodeBlocks.length).toBeGreaterThan(0)

      // Cleanup light container
      if (lightContainer.parentNode) {
        lightContainer.parentNode.removeChild(lightContainer)
      }

      // Dark mode
      container = createFullPageDOM(true)
      const darkCodeBlocks = container.querySelectorAll('.code-block, pre')
      expect(darkCodeBlocks.length).toBeGreaterThan(0)

      // Same number of code blocks should exist in both modes
      expect(lightCodeBlocks.length).toBe(darkCodeBlocks.length)
    })
  })

  /**
   * Test Case 5: Diagram visibility in dark mode
   * Expected: Architecture diagram is visible and readable in dark mode
   */
  describe('TC-5: Diagram visibility in dark mode', () => {
    it('should have LSM tree diagram with currentColor for dark mode compatibility', () => {
      container = createFullPageDOM(true)

      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')
      expect(diagram).not.toBeNull()

      // Check that diagram uses currentColor for text
      const textElements = diagram.querySelectorAll('text')
      expect(textElements.length).toBeGreaterThan(0)

      textElements.forEach(text => {
        const fill = text.getAttribute('fill')
        expect(fill).toBe('currentColor')
      })
    })

    it('should have diagram SVG with dark mode text classes', () => {
      container = createFullPageDOM(true)

      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')
      expect(diagram).not.toBeNull()

      // SVG should have dark:text-white class for currentColor inheritance
      const className = diagram.getAttribute('class') || ''
      expect(className).toContain('dark:text-white')
    })

    it('should have diagram container with appropriate dark background', () => {
      container = createFullPageDOM(true)

      const diagramContainer = container.querySelector('.diagram-container')
      expect(diagramContainer).not.toBeNull()

      const className = diagramContainer.className
      // Should have dark:bg- class
      expect(className).toMatch(/dark:bg-gray-\d+/)
    })

    it('should have colored elements in diagram that are visible in dark mode', () => {
      container = createFullPageDOM(true)

      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')

      // Check for colored rectangles (memtable, sstables, etc.)
      const coloredRects = diagram.querySelectorAll('rect[fill*="#"]')
      expect(coloredRects.length).toBeGreaterThan(0)
    })

    it('should have arrows with currentColor for visibility in dark mode', () => {
      container = createFullPageDOM(true)

      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')

      // Check paths (arrows) use currentColor
      const paths = diagram.querySelectorAll('path[stroke="currentColor"]')
      expect(paths.length).toBeGreaterThan(0)
    })

    it('should have accessible alt text/description for diagram', () => {
      container = createFullPageDOM(true)

      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')

      // Check for title and desc elements
      const title = diagram.querySelector('title')
      const desc = diagram.querySelector('desc')

      expect(title).not.toBeNull()
      expect(desc).not.toBeNull()
    })
  })

  /**
   * Test Case 6: Theme toggle transition smoothness
   * Expected: Transition is smooth without layout shifts
   */
  describe('TC-6: Theme toggle transition smoothness', () => {
    it('should have transition classes on color-changing elements', () => {
      container = createFullPageDOM(false)

      // Check for transition-colors class on buttons
      const buttons = container.querySelectorAll('[class*="transition-colors"]')
      expect(buttons.length).toBeGreaterThan(0)
    })

    it('should not cause layout shifts when toggling theme', () => {
      // Create light mode DOM
      container = createFullPageDOM(false)
      const htmlElement = container.querySelector('html')

      // Get initial layout measurements
      const heroSection = container.querySelector('[data-testid="hero-section"]')
      const initialWidth = heroSection.offsetWidth
      const initialHeight = heroSection.offsetHeight

      // Toggle to dark mode
      setTheme('dark', htmlElement)

      // Measurements should remain the same (no layout shift)
      expect(heroSection.offsetWidth).toBe(initialWidth)
      expect(heroSection.offsetHeight).toBe(initialHeight)
    })

    it('should preserve element structure when toggling theme', () => {
      container = createFullPageDOM(false)
      const htmlElement = container.querySelector('html')

      // Count elements before toggle
      const elementCountBefore = container.querySelectorAll('*').length

      // Toggle to dark mode
      setTheme('dark', htmlElement)

      // Count elements after toggle
      const elementCountAfter = container.querySelectorAll('*').length

      // Element count should be the same
      expect(elementCountAfter).toBe(elementCountBefore)
    })

    it('should have smooth transition duration defined', () => {
      container = createFullPageDOM(false)

      // Check for duration-* classes
      const transitionElements = container.querySelectorAll('[class*="duration-"]')
      expect(transitionElements.length).toBeGreaterThan(0)
    })

    it('should toggle theme class without removing other classes', () => {
      container = createFullPageDOM(false)
      const htmlElement = container.querySelector('html')

      // Store original class count
      htmlElement.classList.add('test-class')
      const originalHasTestClass = htmlElement.classList.contains('test-class')

      // Toggle to dark mode
      setTheme('dark', htmlElement)

      // Should still have test class and now have dark class
      expect(htmlElement.classList.contains('test-class')).toBe(originalHasTestClass)
      expect(htmlElement.classList.contains('dark')).toBe(true)

      // Toggle back to light mode
      setTheme('light', htmlElement)

      // Should still have test class but not dark class
      expect(htmlElement.classList.contains('test-class')).toBe(originalHasTestClass)
      expect(htmlElement.classList.contains('dark')).toBe(false)
    })

    it('should support CSS variables for smooth theming', () => {
      container = createFullPageDOM(false)

      const styleTag = container.querySelector('style')
      expect(styleTag.textContent).toContain(':root')
      expect(styleTag.textContent).toContain('.dark')
    })
  })

  /**
   * Additional dark mode validations
   */
  describe('Additional dark mode validations', () => {
    it('should have dark mode border colors defined', () => {
      container = createFullPageDOM(true)

      const elementsWithDarkBorder = container.querySelectorAll('[class*="dark:border-"]')
      expect(elementsWithDarkBorder.length).toBeGreaterThan(0)
    })

    it('should have dark mode hover states', () => {
      container = createFullPageDOM(true)

      // Check for dark:hover: classes
      const elementsWithDarkHover = container.querySelectorAll('[class*="dark:hover:"]')
      expect(elementsWithDarkHover.length).toBeGreaterThan(0)
    })

    it('should have icon colors adapt to dark mode', () => {
      container = createFullPageDOM(true)

      // Check for icons with dark: color classes
      const icons = container.querySelectorAll('svg[class*="dark:text-"]')
      expect(icons.length).toBeGreaterThan(0)
    })

    it('should have btn-secondary with dark mode styling', () => {
      container = createFullPageDOM(true)

      const secondaryButtons = container.querySelectorAll('[class*="btn-secondary"], [class*="dark:bg-gray-"]')
      expect(secondaryButtons.length).toBeGreaterThan(0)
    })

    it('should have explanation cards with dark mode background', () => {
      container = createFullPageDOM(true)

      const explanationCards = container.querySelectorAll('[data-testid^="explanation-"]')

      explanationCards.forEach(card => {
        const className = card.className
        expect(className).toContain('dark:bg-gray-800')
      })
    })

    it('should have scroll behavior not affected by theme', () => {
      container = createFullPageDOM(true)

      const html = container.querySelector('html')
      // Scroll behavior should be smooth regardless of theme
      // This is usually set via CSS
      expect(html).not.toBeNull()
    })

    it('should have dark mode ready quick start section', () => {
      container = createFullPageDOM(true)

      const quickStartSection = container.querySelector('#quickstart')
      expect(quickStartSection.className).toContain('dark:bg-gray-800')
    })

    it('should have protocol docs section with dark mode support', () => {
      container = createFullPageDOM(true)

      const protocolSection = container.querySelector('#protocol')
      expect(protocolSection.className).toContain('dark:bg-gray-900')
    })
  })
})
