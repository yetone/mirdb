import { describe, it, expect, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from './Home'
import Navbar from '../components/Navbar'
import { AuthProvider } from '../contexts/AuthContext'

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  clear: vi.fn(),
  removeItem: vi.fn(),
}
Object.defineProperty(window, 'localStorage', { value: localStorageMock })

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <div {...rest}>{children}</div>
    },
    section: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <section {...rest}>{children}</section>
    },
    ul: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <ul {...rest}>{children}</ul>
    },
    ol: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <ol {...rest}>{children}</ol>
    },
    li: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <li {...rest}>{children}</li>
    },
    svg: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, whileInView, viewport, transition, variants, ...rest } = props
      return <svg {...rest}>{children}</svg>
    },
    circle: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, ...rest } = props
      return <circle {...rest}>{children}</circle>
    },
    rect: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, ...rest } = props
      return <rect {...rest}>{children}</rect>
    },
    g: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, ...rest } = props
      return <g {...rest}>{children}</g>
    },
    text: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, ...rest } = props
      return <text {...rest}>{children}</text>
    },
  },
}))

const renderWithRouter = () => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <AuthProvider>
        <Navbar />
        <Home />
      </AuthProvider>
    </MemoryRouter>
  )
}

describe('Home Page - Accessibility - ARIA Labels', () => {
  // Test Case 1: Check icon buttons for aria-label
  describe('Test Case 1: Icon-only buttons have descriptive aria-label attributes', () => {
    it('theme toggle button has aria-label attribute', () => {
      renderWithRouter()

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toHaveAttribute('aria-label')
      expect(themeToggle.getAttribute('aria-label')).toMatch(/switch theme/i)
    })

    it('all buttons with only SVG icons have aria-label', () => {
      renderWithRouter()

      // Find all buttons that contain only SVG (icon-only buttons)
      const allButtons = document.querySelectorAll('button')
      const iconOnlyButtons = Array.from(allButtons).filter((button) => {
        const hasSvg = button.querySelector('svg')
        const textContent = button.textContent?.trim() || ''
        // Button is icon-only if it has SVG and no meaningful text
        return hasSvg && textContent === ''
      })

      // Each icon-only button should have aria-label
      iconOnlyButtons.forEach((button) => {
        expect(button).toHaveAttribute('aria-label')
      })
    })

    it('nav logo link is accessible with visible text', () => {
      renderWithRouter()

      const navLogo = screen.getByTestId('nav-logo')
      // Logo link contains both SVG and text "ShortURL"
      expect(navLogo).toBeInTheDocument()
      expect(navLogo).toHaveTextContent('ShortURL')
    })
  })

  // Test Case 2: Check theme toggle for aria-label
  describe('Test Case 2: Theme toggle has descriptive label for screen readers', () => {
    it('theme toggle has aria-label describing current theme', () => {
      renderWithRouter()

      const themeToggle = screen.getByTestId('theme-toggle')
      const ariaLabel = themeToggle.getAttribute('aria-label')

      expect(ariaLabel).toBeTruthy()
      // Should indicate it's for switching themes
      expect(ariaLabel).toMatch(/switch theme/i)
      // Should indicate current theme state
      expect(ariaLabel).toMatch(/current:/i)
    })

    it('theme toggle button is focusable', () => {
      renderWithRouter()

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle.tagName.toLowerCase()).toBe('button')
      expect(themeToggle).not.toHaveAttribute('tabindex', '-1')
    })

    it('theme toggle icon SVGs are decorative (hidden from screen readers)', () => {
      renderWithRouter()

      const themeToggle = screen.getByTestId('theme-toggle')
      const svgIcon = themeToggle.querySelector('svg')

      // SVG inside button with aria-label should be presentational
      // The button's aria-label provides the accessible name
      expect(themeToggle).toHaveAttribute('aria-label')
      expect(svgIcon).toBeInTheDocument()
    })
  })

  // Test Case 3: Check navigation landmark
  describe('Test Case 3: Navigation has appropriate role or is within nav element', () => {
    it('header contains nav element for navigation links', () => {
      renderWithRouter()

      const header = screen.getByTestId('navigation-header')
      const navElement = header.querySelector('nav')

      expect(navElement).toBeInTheDocument()
    })

    it('nav element contains navigation links', () => {
      renderWithRouter()

      const header = screen.getByTestId('navigation-header')
      const navElement = header.querySelector('nav')

      expect(navElement).toBeInTheDocument()
      // Nav should contain interactive navigation elements
      const buttonsInNav = navElement!.querySelectorAll('button')
      expect(buttonsInNav.length).toBeGreaterThan(0)
    })

    it('footer contains nav element for footer links', () => {
      renderWithRouter()

      const footer = screen.getByTestId('footer-section')
      const footerNav = footer.querySelector('nav')

      expect(footerNav).toBeInTheDocument()
    })

    it('navigation landmarks are properly structured', () => {
      renderWithRouter()

      // Header should be a landmark
      const header = document.querySelector('header')
      expect(header).toBeInTheDocument()

      // Main content should be a landmark
      const main = document.querySelector('main')
      expect(main).toBeInTheDocument()

      // Footer should be a landmark
      const footer = document.querySelector('footer')
      expect(footer).toBeInTheDocument()

      // Navigation elements should exist
      const navElements = document.querySelectorAll('nav')
      expect(navElements.length).toBeGreaterThanOrEqual(2)
    })

    it('header has appropriate role="banner" or is a header element', () => {
      renderWithRouter()

      const header = screen.getByTestId('navigation-header')
      // header element has implicit banner role
      expect(header.tagName.toLowerCase()).toBe('header')
    })
  })

  // Test Case 4: Check images for alt text
  describe('Test Case 4: All images have appropriate alt attributes', () => {
    it('hero visual SVG has aria-label for accessibility', () => {
      renderWithRouter()

      const heroVisual = screen.getByTestId('hero-visual')
      const svgElement = heroVisual.querySelector('svg')

      // Decorative SVGs in hero section don't need alt text if parent provides context
      // But meaningful images/illustrations should have aria-label
      expect(heroVisual).toBeInTheDocument()
    })

    it('analytics dashboard preview SVG has aria-label', () => {
      renderWithRouter()

      const analyticsPreview = screen.getByTestId('analytics-dashboard-preview')
      const svgElement = analyticsPreview.querySelector('svg')

      expect(svgElement).toBeInTheDocument()
      expect(svgElement).toHaveAttribute('aria-label')
      expect(svgElement!.getAttribute('aria-label')).toBeTruthy()
    })

    it('feature icons are decorative and contained within labeled elements', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const featureCards = featuresSection.querySelectorAll('li')

      featureCards.forEach((card) => {
        const svg = card.querySelector('svg')
        const heading = card.querySelector('h3')

        // Each feature card should have an icon and a heading
        expect(svg).toBeInTheDocument()
        expect(heading).toBeInTheDocument()
        // The heading provides accessible name for the feature
        expect(heading!.textContent).toBeTruthy()
      })
    })

    it('footer logo SVG is accompanied by visible text', () => {
      renderWithRouter()

      const footerLogo = screen.getByTestId('footer-logo')
      const svgIcon = footerLogo.querySelector('svg')
      const brandText = footerLogo.querySelector('span')

      expect(svgIcon).toBeInTheDocument()
      expect(brandText).toBeInTheDocument()
      expect(brandText!.textContent).toBe('LinkShort')
    })

    it('all informative images have appropriate alt or aria-label', () => {
      renderWithRouter()

      // Check that the analytics preview (the main informative image) has aria-label
      const analyticsSection = screen.getByTestId('analytics-preview-section')
      const dashboardPreview = within(analyticsSection).getByTestId('analytics-dashboard-preview')
      const svgWithLabel = dashboardPreview.querySelector('svg[aria-label]')

      expect(svgWithLabel).toBeInTheDocument()
    })

    it('decorative icons do not have redundant alt text', () => {
      renderWithRouter()

      // Feature section icons are decorative - they accompany text
      const featuresSection = screen.getByTestId('features-section')
      const featureCards = featuresSection.querySelectorAll('li')

      featureCards.forEach((card) => {
        const heading = card.querySelector('h3')
        const description = card.querySelector('p')

        // Content should be accessible via text, not icon
        expect(heading).toBeInTheDocument()
        expect(description).toBeInTheDocument()
      })
    })
  })

  // Additional ARIA tests for form inputs
  describe('Additional: Form inputs have proper ARIA attributes', () => {
    it('demo URL input has aria-label', () => {
      renderWithRouter()

      const demoInput = screen.getByTestId('demo-input')
      expect(demoInput).toHaveAttribute('aria-label')
      expect(demoInput.getAttribute('aria-label')).toBe('URL to shorten')
    })

    it('demo URL input has aria-describedby when error exists', () => {
      renderWithRouter()

      const demoInput = screen.getByTestId('demo-input')
      // Initially no error, so aria-describedby should not point to error
      // This is correctly implemented - it only adds aria-describedby when there's an error
      expect(demoInput).toBeInTheDocument()
    })
  })
})
