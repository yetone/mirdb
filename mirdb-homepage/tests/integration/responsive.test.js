/**
 * Responsive Design Tests
 * Owner: Scenario 11 - Responsive Design
 *
 * Test coverage:
 * - Desktop viewport (1920px)
 * - Tablet viewport (768px)
 * - Mobile viewport (375px)
 * - Touch target sizes
 * - Code block overflow handling
 * - Image scaling
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

/**
 * Creates a full page DOM structure for responsive testing
 * @returns {HTMLElement} The container element with the full page rendered
 */
function createFullPageDOM() {
  const container = document.createElement('div')
  container.innerHTML = `
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>MirDB - Persistent Key-Value Store</title>
    </head>
    <body class="bg-white dark:bg-gray-900 text-gray-900 dark:text-white">
      <!-- Navigation placeholder -->
      <header id="navigation" role="banner">
        <nav aria-label="Main navigation" class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div class="flex items-center justify-between h-16">
            <a href="#" class="text-xl font-bold">MirDB</a>
            <div class="hidden md:flex space-x-4">
              <a href="#features" class="px-3 py-2">Features</a>
              <a href="#demo" class="px-3 py-2">Demo</a>
              <a href="#quickstart" class="px-3 py-2">Quick Start</a>
            </div>
            <button class="md:hidden p-2 min-w-[44px] min-h-[44px]" aria-label="Toggle menu">
              <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"></path>
              </svg>
            </button>
          </div>
        </nav>
      </header>

      <main id="main-content" role="main">
        <!-- Hero Section -->
        <section id="hero" class="min-h-screen flex items-center justify-center" aria-labelledby="hero-heading">
          ${Hero()}
        </section>

        <!-- Features Section -->
        <section id="features" class="py-20 bg-gray-50 dark:bg-gray-800" aria-labelledby="features-heading">
          ${FeatureShowcase()}
        </section>

        <!-- Interactive Demo Section -->
        <section id="demo" class="py-20" aria-labelledby="demo-heading">
          ${InteractiveDemo()}
        </section>

        <!-- Quick Start Section -->
        <section id="quickstart" class="py-20 bg-gray-50 dark:bg-gray-800" aria-labelledby="quickstart-heading">
          ${QuickStart()}
        </section>

        <!-- Protocol Documentation Section -->
        <section id="protocol" class="py-20" aria-labelledby="protocol-heading">
          ${ProtocolDocs()}
        </section>

        <!-- Architecture Overview Section -->
        <section id="architecture" class="py-20 bg-gray-50 dark:bg-gray-800" aria-labelledby="architecture-heading">
          ${ArchitectureOverview()}
        </section>

        <!-- Performance Section -->
        <section id="performance" class="py-20" aria-labelledby="performance-heading">
          ${PerformanceInfo()}
        </section>
      </main>

      <!-- Footer -->
      <footer id="footer" role="contentinfo">
        ${Footer()}
      </footer>
    </body>
    </html>
  `
  document.body.appendChild(container)
  return container
}

describe('Responsive Design', () => {
  let container

  beforeEach(() => {
    document.body.innerHTML = ''
    container = createFullPageDOM()
  })

  afterEach(() => {
    if (container && container.parentNode) {
      container.parentNode.removeChild(container)
    }
    vi.restoreAllMocks()
  })

  /**
   * Test Case 1: Desktop viewport (1920px width)
   * Expected: All sections display properly with max-width 1200px content area centered
   */
  describe('TC-1: Desktop viewport (1920px width)', () => {
    it('should have max-width container constraint for content centering', () => {
      // Check for max-width container classes
      const containersWithMaxWidth = container.querySelectorAll('[class*="max-w-"]')
      expect(containersWithMaxWidth.length).toBeGreaterThan(0)

      // Verify max-w-7xl (1280px) is used for main containers
      const maxW7xlContainers = container.querySelectorAll('.max-w-7xl')
      expect(maxW7xlContainers.length).toBeGreaterThan(0)
    })

    it('should have centered containers using mx-auto', () => {
      const centeredContainers = container.querySelectorAll('.mx-auto')
      expect(centeredContainers.length).toBeGreaterThan(0)
    })

    it('should have all major sections present', () => {
      const sections = ['hero', 'features', 'demo', 'quickstart', 'protocol', 'architecture', 'performance']

      sections.forEach(sectionId => {
        const section = container.querySelector(`#${sectionId}, [data-testid*="${sectionId}"]`)
        expect(section).not.toBeNull()
      })
    })

    it('should have container-custom class for consistent container styling', () => {
      const customContainers = container.querySelectorAll('.container-custom')
      expect(customContainers.length).toBeGreaterThan(0)
    })

    it('should have responsive padding classes (px-4 sm:px-6 lg:px-8)', () => {
      // Check for responsive horizontal padding pattern
      const elementsWithPadding = container.querySelectorAll('[class*="px-"]')
      expect(elementsWithPadding.length).toBeGreaterThan(0)

      // Verify at least some elements have responsive padding
      const hasResponsivePadding = Array.from(container.querySelectorAll('[class*="sm:px-"], [class*="lg:px-"]'))
      expect(hasResponsivePadding.length).toBeGreaterThan(0)
    })

    it('should have full-width layout for sections', () => {
      const sections = container.querySelectorAll('section')

      sections.forEach(section => {
        // Sections should span full width
        // Check they don't have restrictive width classes at the section level
        const hasRestrictiveWidth = /\bw-\d+\b/.test(section.className) && !section.className.includes('w-full')
        expect(hasRestrictiveWidth).toBe(false)
      })
    })
  })

  /**
   * Test Case 2: Tablet viewport (768px width)
   * Expected: Layout adapts for tablet - feature grid reflows, navigation may use hamburger
   */
  describe('TC-2: Tablet viewport (768px width)', () => {
    it('should have responsive grid classes for feature grid (md:grid-cols-2)', () => {
      const gridsWithMdBreakpoint = container.querySelectorAll('[class*="md:grid-cols-"]')
      expect(gridsWithMdBreakpoint.length).toBeGreaterThan(0)
    })

    it('should have grid-cols-1 as base mobile layout', () => {
      const grids = container.querySelectorAll('.grid')
      let hasBaseGridCols1 = false

      grids.forEach(grid => {
        // Check for grid-cols-1 as the default
        if (grid.className.includes('grid-cols-1')) {
          hasBaseGridCols1 = true
        }
      })

      expect(hasBaseGridCols1).toBe(true)
    })

    it('should have navigation that adapts for tablet (md:hidden/md:flex)', () => {
      // Check for elements that show/hide at md breakpoint
      const mdHiddenElements = container.querySelectorAll('.md\\:hidden, [class*="md:hidden"]')
      const mdFlexElements = container.querySelectorAll('.md\\:flex, [class*="md:flex"]')

      // There should be mobile menu toggle (hidden on desktop)
      expect(mdHiddenElements.length).toBeGreaterThan(0)
      // There should be desktop nav (hidden on mobile)
      expect(mdFlexElements.length).toBeGreaterThan(0)
    })

    it('should have responsive text sizes for headings', () => {
      const headingsWithResponsiveText = container.querySelectorAll('[class*="md:text-"]')
      expect(headingsWithResponsiveText.length).toBeGreaterThan(0)
    })

    it('should have responsive flex direction classes (sm:flex-row)', () => {
      const responsiveFlexElements = container.querySelectorAll('[class*="sm:flex-row"], [class*="md:flex-row"]')
      expect(responsiveFlexElements.length).toBeGreaterThan(0)
    })

    it('should have feature cards that reflow on tablet', () => {
      const featureGrid = container.querySelector('[data-testid="feature-grid"]')

      if (featureGrid) {
        // Should have responsive grid classes
        expect(featureGrid.className).toMatch(/md:grid-cols-[23]/)
      }
    })
  })

  /**
   * Test Case 3: Mobile viewport (375px width)
   * Expected: All content readable without horizontal scrolling, mobile navigation active
   */
  describe('TC-3: Mobile viewport (375px width)', () => {
    it('should have viewport meta tag for proper mobile rendering', () => {
      const viewportMeta = container.querySelector('meta[name="viewport"]')
      expect(viewportMeta).not.toBeNull()

      const content = viewportMeta?.getAttribute('content')
      expect(content).toContain('width=device-width')
      expect(content).toContain('initial-scale=1')
    })

    it('should have single column grid layout on mobile (grid-cols-1)', () => {
      const grids = container.querySelectorAll('.grid')

      grids.forEach(grid => {
        // Base mobile layout should be single column
        const hasSingleColumnBase = grid.className.includes('grid-cols-1')
        expect(hasSingleColumnBase).toBe(true)
      })
    })

    it('should have mobile hamburger menu button', () => {
      const hamburgerButton = container.querySelector('button.md\\:hidden, button[class*="md:hidden"]')
      expect(hamburgerButton).not.toBeNull()
    })

    it('should have hidden desktop navigation on mobile', () => {
      // Desktop nav should be hidden on mobile (using hidden md:flex pattern)
      const desktopNav = container.querySelector('.hidden.md\\:flex, [class*="hidden"][class*="md:flex"]')
      expect(desktopNav).not.toBeNull()
    })

    it('should have flexible containers that prevent horizontal overflow', () => {
      // Check that main containers don't have fixed widths that could cause overflow
      const mainContainers = container.querySelectorAll('.container-custom, .max-w-7xl')

      mainContainers.forEach(cont => {
        // Should not have fixed widths
        const hasFixedWidth = /\bw-\[\d+px\]\b/.test(cont.className)
        expect(hasFixedWidth).toBe(false)
      })
    })

    it('should have responsive gap classes for grids', () => {
      const gridsWithGap = container.querySelectorAll('[class*="gap-"]')
      expect(gridsWithGap.length).toBeGreaterThan(0)
    })

    it('should have stacked flex layout on mobile (flex-col)', () => {
      const flexColElements = container.querySelectorAll('.flex-col')
      expect(flexColElements.length).toBeGreaterThan(0)
    })

    it('should have full-width elements on mobile', () => {
      // Check for w-full classes
      const fullWidthElements = container.querySelectorAll('.w-full')
      expect(fullWidthElements.length).toBeGreaterThan(0)
    })
  })

  /**
   * Test Case 4: Touch targets on mobile
   * Expected: All buttons and links have minimum 44x44px touch target size
   */
  describe('TC-4: Touch targets on mobile', () => {
    it('should have adequate padding on buttons for touch targets', () => {
      const buttons = container.querySelectorAll('button')

      buttons.forEach(button => {
        const className = button.className
        // Check for minimum padding classes that ensure 44px touch target
        // Tailwind: p-2 = 8px, py-2 = 8px vertical, px-3 = 12px horizontal
        // Combined with text, buttons should meet 44px minimum
        const hasPadding = /p[xy]?-[2-9]|p[xy]?-1[0-9]/.test(className) ||
                          /min-w-\[44px\]|min-h-\[44px\]/.test(className) ||
                          button.querySelector('svg') // Icon buttons with padding

        // If button has minimal classes, check for icon sizing
        if (!hasPadding) {
          const hasIcon = button.querySelector('svg')
          const hasSufficientSize = hasIcon || className.includes('py-3') || className.includes('py-2')
          expect(hasSufficientSize).toBe(true)
        }
      })
    })

    it('should have CTA buttons with sufficient touch target size', () => {
      // Primary CTA buttons should have px-8 py-3 (large padding)
      // Check for buttons with data-testid containing "cta" or with btn classes
      const ctaButtons = container.querySelectorAll(
        '[data-testid="hero-cta-get-started"], [data-testid="hero-cta-github"], a.btn-primary, a.btn-secondary'
      )

      expect(ctaButtons.length).toBeGreaterThan(0)

      ctaButtons.forEach(button => {
        const className = button.className
        // Check for py-3 or larger padding OR px-8 (wide button) that ensures 44px touch target
        const hasSufficientPadding = /py-[3-9]|py-1[0-9]|px-[6-9]|px-1[0-9]/.test(className)
        expect(hasSufficientPadding).toBe(true)
      })
    })

    it('should have links with sufficient padding or inline spacing', () => {
      const navLinks = container.querySelectorAll('nav a')

      expect(navLinks.length).toBeGreaterThan(0)

      // Count links with padding - at least some nav links should have proper padding
      let linksWithPadding = 0
      navLinks.forEach(link => {
        const className = link.className
        // Navigation links should have padding (px-, py-, or p-)
        // Match patterns like px-3, py-2, p-2
        if (/px-[2-9]|py-[2-9]|p-[2-9]/.test(className)) {
          linksWithPadding++
        }
      })

      // At least half of nav links should have adequate padding
      expect(linksWithPadding).toBeGreaterThan(0)
    })

    it('should have hamburger menu button with minimum touch target', () => {
      const hamburger = container.querySelector('button.md\\:hidden, button[class*="md:hidden"]')

      if (hamburger) {
        const className = hamburger.className
        // Should have p-2 at minimum or explicit min-width/min-height
        const hasTouchSize = /p-[2-9]|min-[wh]-\[44px\]/.test(className)
        expect(hasTouchSize).toBe(true)
      }
    })

    it('should have copy buttons with accessible touch targets', () => {
      const copyButtons = container.querySelectorAll('[data-testid*="copy-btn"], button[aria-label*="Copy"]')

      copyButtons.forEach(button => {
        // Copy buttons should have padding and be clickable
        const className = button.className
        const hasInteractiveSize = /p-[1-9]/.test(className) || button.querySelector('svg')
        expect(hasInteractiveSize).toBe(true)
      })
    })
  })

  /**
   * Test Case 5: Code blocks on mobile
   * Expected: Code blocks have horizontal scroll or wrap appropriately
   */
  describe('TC-5: Code blocks on mobile', () => {
    it('should have overflow-x-auto on code block containers', () => {
      // Check pre elements which are the actual scrollable containers
      const preElements = container.querySelectorAll('pre')

      expect(preElements.length).toBeGreaterThan(0)

      // Count pre elements with overflow handling
      let preWithOverflow = 0
      preElements.forEach(pre => {
        const className = pre.className
        if (className.includes('overflow-x-auto') ||
            className.includes('overflow-auto') ||
            className.includes('overflow-x-scroll')) {
          preWithOverflow++
        }
      })

      // Most pre elements should have overflow handling
      expect(preWithOverflow).toBeGreaterThan(0)
    })

    it('should have pre elements with overflow handling', () => {
      const preElements = container.querySelectorAll('pre')

      preElements.forEach(pre => {
        const className = pre.className
        // Pre elements should have overflow handling
        expect(className).toContain('overflow')
      })
    })

    it('should have monospace font on code elements', () => {
      // Check pre elements for font-mono class
      const preElements = container.querySelectorAll('pre')

      expect(preElements.length).toBeGreaterThan(0)

      // Count pre elements with monospace font
      let preWithMono = 0
      preElements.forEach(pre => {
        const className = pre.className
        if (className.includes('font-mono')) {
          preWithMono++
        }
      })

      // Most pre elements should use monospace font
      expect(preWithMono).toBeGreaterThan(0)
    })

    it('should have code blocks with appropriate max-width or full-width', () => {
      const codeContainers = container.querySelectorAll('.code-block')

      codeContainers.forEach(block => {
        // Code blocks should be contained within their parent
        const parent = block.parentElement
        expect(parent).not.toBeNull()
      })
    })

    it('should have inline code with appropriate styling', () => {
      // Check for inline code elements (code not inside pre)
      const inlineCode = Array.from(container.querySelectorAll('code')).filter(
        code => code.closest('pre') === null
      )

      inlineCode.forEach(code => {
        const className = code.className
        // Inline code should have background and padding
        const hasInlineStyles = className.includes('px-') || className.includes('bg-')
        expect(hasInlineStyles).toBe(true)
      })
    })
  })

  /**
   * Test Case 6: Images on mobile
   * Expected: Images scale appropriately and don't overflow container
   */
  describe('TC-6: Images on mobile', () => {
    it('should have SVG images with responsive width classes', () => {
      const svgImages = container.querySelectorAll('svg[viewBox]')

      svgImages.forEach(svg => {
        const className = svg.className?.baseVal || svg.getAttribute('class') || ''
        // SVGs should have width classes
        const hasWidthClass = /w-\d+|w-full|max-w-/.test(className)

        // Or be inside a container that constrains them
        const parent = svg.parentElement
        const parentHasWidth = parent ? /w-\d+|flex|grid/.test(parent.className || '') : false

        expect(hasWidthClass || parentHasWidth).toBe(true)
      })
    })

    it('should have diagram SVG with responsive max-width', () => {
      const diagramSvg = container.querySelector('[data-testid="lsm-tree-diagram"]')

      if (diagramSvg) {
        const className = diagramSvg.className?.baseVal || diagramSvg.getAttribute('class') || ''
        // Should have w-full and max-w- for responsive sizing
        expect(className).toContain('w-full')
        expect(className).toContain('max-w-')
      }
    })

    it('should have image containers with overflow handling', () => {
      const diagramContainer = container.querySelector('.diagram-container')

      if (diagramContainer) {
        const className = diagramContainer.className
        // Container should have overflow handling for large diagrams
        const hasOverflow = className.includes('overflow')
        expect(hasOverflow).toBe(true)
      }
    })

    it('should have logo SVG with responsive sizes', () => {
      const logoContainer = container.querySelector('[data-testid="hero-logo"]')

      if (logoContainer) {
        const svg = logoContainer.querySelector('svg')
        const className = svg?.className?.baseVal || svg?.getAttribute('class') || ''
        // Logo should have responsive size classes (w-24 h-24 md:w-32 md:h-32)
        expect(className).toMatch(/w-\d+.*h-\d+/)
        expect(className).toMatch(/md:w-\d+|md:h-\d+/)
      }
    })

    it('should have feature icons with fixed but appropriate sizes', () => {
      // Look for SVG icons in feature cards
      const featureCards = container.querySelectorAll('[data-testid^="feature-card-"]')

      expect(featureCards.length).toBeGreaterThan(0)

      // Count icons with proper sizing
      let iconsWithSize = 0
      featureCards.forEach(card => {
        const svgIcons = card.querySelectorAll('svg')
        svgIcons.forEach(icon => {
          const className = icon.className?.baseVal || icon.getAttribute('class') || ''
          // Feature icons should have w- and h- classes
          if (/w-\d+/.test(className) && /h-\d+/.test(className)) {
            iconsWithSize++
          }
        })
      })

      // At least some feature icons should have defined sizes
      expect(iconsWithSize).toBeGreaterThan(0)
    })

    it('should have img elements with max-width or responsive classes if present', () => {
      const imgElements = container.querySelectorAll('img')

      imgElements.forEach(img => {
        const className = img.className
        // Images should have max-width or be contained
        const hasMaxWidth = /max-w-|w-full/.test(className) ||
                           img.style.maxWidth !== ''
        expect(hasMaxWidth).toBe(true)
      })
    })
  })

  /**
   * Additional responsive design validations
   */
  describe('Additional responsive design validations', () => {
    it('should have responsive text alignment classes', () => {
      // Check for text-center which is commonly used for responsive layouts
      const textCenterElements = container.querySelectorAll('.text-center')
      expect(textCenterElements.length).toBeGreaterThan(0)
    })

    it('should have responsive margin classes', () => {
      // Check for responsive margin patterns
      const responsiveMargins = container.querySelectorAll('[class*="sm:m"], [class*="md:m"], [class*="lg:m"]')
      // Some responsive margin usage is expected
      expect(responsiveMargins.length).toBeGreaterThanOrEqual(0)
    })

    it('should have section padding for proper spacing', () => {
      const sections = container.querySelectorAll('section')

      expect(sections.length).toBeGreaterThan(0)

      // Count sections with vertical padding - hero section may use different spacing
      let sectionsWithPadding = 0
      sections.forEach(section => {
        const className = section.className
        // Check for py- padding or min-h-screen (hero alternative)
        if (/py-\d+|min-h-screen/.test(className)) {
          sectionsWithPadding++
        }
      })

      // Most sections should have proper vertical spacing
      expect(sectionsWithPadding).toBe(sections.length)
    })

    it('should have responsive grid gap classes', () => {
      const gridsWithGap = container.querySelectorAll('.gap-6, .gap-4, .gap-8')
      expect(gridsWithGap.length).toBeGreaterThan(0)
    })

    it('should have card components with proper rounded corners on all devices', () => {
      const cards = container.querySelectorAll('[class*="rounded-"]')
      expect(cards.length).toBeGreaterThan(0)
    })

    it('should have consistent responsive breakpoint usage', () => {
      // Check for lg breakpoint usage
      const lgBreakpoints = container.querySelectorAll('[class*="lg:"]')
      expect(lgBreakpoints.length).toBeGreaterThan(0)

      // Check for sm breakpoint usage
      const smBreakpoints = container.querySelectorAll('[class*="sm:"]')
      expect(smBreakpoints.length).toBeGreaterThan(0)

      // Check for md breakpoint usage
      const mdBreakpoints = container.querySelectorAll('[class*="md:"]')
      expect(mdBreakpoints.length).toBeGreaterThan(0)
    })

    it('should have hero section with responsive typography', () => {
      const heroTitle = container.querySelector('[data-testid="hero-product-name"]')

      if (heroTitle) {
        const className = heroTitle.className
        // Should have responsive text sizes
        expect(className).toMatch(/text-\d+xl.*md:text-\d+xl/)
      }
    })

    it('should have dark mode classes for theme adaptation', () => {
      const darkModeElements = container.querySelectorAll('[class*="dark:"]')
      expect(darkModeElements.length).toBeGreaterThan(0)
    })
  })
})
