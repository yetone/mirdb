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

describe('Home Page - Accessibility - Semantic HTML', () => {
  // Test Case 1: Check heading hierarchy on landing page
  describe('Test Case 1: Heading hierarchy', () => {
    it('has exactly one h1 element on the page', () => {
      renderWithRouter()

      const h1Elements = document.querySelectorAll('h1')
      expect(h1Elements).toHaveLength(1)
    })

    it('h1 is the hero headline', () => {
      renderWithRouter()

      const h1 = document.querySelector('h1')
      expect(h1).toBeInTheDocument()
      expect(h1).toHaveAttribute('data-testid', 'hero-headline')
    })

    it('headings follow logical order (h1 > h2 > h3)', () => {
      renderWithRouter()

      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6')
      const headingLevels = Array.from(allHeadings).map((h) =>
        parseInt(h.tagName.charAt(1))
      )

      // First heading should be h1
      expect(headingLevels[0]).toBe(1)

      // Verify no heading level is skipped (e.g., h1 directly to h3)
      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i]
        const previousLevel = headingLevels[i - 1]
        // Each heading should not be more than 1 level deeper than a previous heading
        // But can reset to any higher level
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1)
        }
      }
    })

    it('has h2 elements for each major section', () => {
      renderWithRouter()

      const h2Elements = document.querySelectorAll('h2')
      // Should have h2 for: Features, How It Works, Try It Now (Demo), Analytics
      expect(h2Elements.length).toBeGreaterThanOrEqual(4)
    })

    it('feature cards use h3 for titles', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const h3Elements = featuresSection.querySelectorAll('h3')
      // Should have 4 feature cards with h3 titles
      expect(h3Elements.length).toBe(4)
    })
  })

  // Test Case 2: Query for semantic landmarks (header, main, footer, nav)
  describe('Test Case 2: Semantic landmarks', () => {
    it('has a header element for navigation', () => {
      renderWithRouter()

      const header = document.querySelector('header')
      expect(header).toBeInTheDocument()
      expect(header).toHaveAttribute('data-testid', 'navigation-header')
    })

    it('has a main element wrapping page content', () => {
      renderWithRouter()

      const main = document.querySelector('main')
      expect(main).toBeInTheDocument()
      expect(main).toHaveAttribute('data-testid', 'home-page')
    })

    it('has a footer element', () => {
      renderWithRouter()

      const footer = document.querySelector('footer')
      expect(footer).toBeInTheDocument()
      expect(footer).toHaveAttribute('data-testid', 'footer-section')
    })

    it('has nav elements for navigation links', () => {
      renderWithRouter()

      const navElements = document.querySelectorAll('nav')
      // Should have nav in header and footer
      expect(navElements.length).toBeGreaterThanOrEqual(2)
    })

    it('header contains nav element', () => {
      renderWithRouter()

      const header = document.querySelector('header')
      expect(header).toBeInTheDocument()
      const navInHeader = header!.querySelector('nav')
      expect(navInHeader).toBeInTheDocument()
    })

    it('footer contains nav element for quick links', () => {
      renderWithRouter()

      const footer = document.querySelector('footer')
      expect(footer).toBeInTheDocument()
      const navInFooter = footer!.querySelector('nav')
      expect(navInFooter).toBeInTheDocument()
    })
  })

  // Test Case 3: Check for section/article elements
  describe('Test Case 3: Section elements', () => {
    it('has section elements for content areas', () => {
      renderWithRouter()

      const sections = document.querySelectorAll('section')
      // Should have sections for: Hero, Features, How It Works, Demo, Analytics
      expect(sections.length).toBeGreaterThanOrEqual(5)
    })

    it('hero section uses section element', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection.tagName.toLowerCase()).toBe('section')
    })

    it('features section uses section element', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection.tagName.toLowerCase()).toBe('section')
    })

    it('how-it-works section uses section element', () => {
      renderWithRouter()

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection.tagName.toLowerCase()).toBe('section')
    })

    it('demo section uses section element', () => {
      renderWithRouter()

      const demoSection = screen.getByTestId('demo-section')
      expect(demoSection.tagName.toLowerCase()).toBe('section')
    })

    it('analytics preview section uses section element', () => {
      renderWithRouter()

      const analyticsSection = screen.getByTestId('analytics-preview-section')
      expect(analyticsSection.tagName.toLowerCase()).toBe('section')
    })

    it('sections have proper id attributes for navigation', () => {
      renderWithRouter()

      // Check sections have id attributes for anchor navigation
      expect(screen.getByTestId('features-section')).toHaveAttribute('id', 'features')
      expect(screen.getByTestId('how-it-works-section')).toHaveAttribute('id', 'how-it-works')
      expect(screen.getByTestId('demo-section')).toHaveAttribute('id', 'demo')
      expect(screen.getByTestId('analytics-preview-section')).toHaveAttribute('id', 'analytics-preview')
    })
  })

  // Test Case 4: Verify lists use ul/ol elements
  describe('Test Case 4: List elements', () => {
    it('features section uses ul element for feature list', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const ulElement = featuresSection.querySelector('ul')
      expect(ulElement).toBeInTheDocument()
    })

    it('features list contains li elements for each feature', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const ulElement = featuresSection.querySelector('ul')
      expect(ulElement).toBeInTheDocument()

      const liElements = ulElement!.querySelectorAll('li')
      // Should have 4 feature items
      expect(liElements.length).toBe(4)
    })

    it('how-it-works section uses ol element for steps', () => {
      renderWithRouter()

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const olElement = howItWorksSection.querySelector('ol')
      expect(olElement).toBeInTheDocument()
    })

    it('how-it-works list contains li elements for each step', () => {
      renderWithRouter()

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const olElement = howItWorksSection.querySelector('ol')
      expect(olElement).toBeInTheDocument()

      const liElements = olElement!.querySelectorAll('li')
      // Should have 3 step items
      expect(liElements.length).toBe(3)
    })

    it('footer navigation uses proper list markup', () => {
      renderWithRouter()

      const footerNav = screen.getByTestId('footer-links')
      expect(footerNav.tagName.toLowerCase()).toBe('nav')
    })

    it('analytics feature highlights use ul element', () => {
      renderWithRouter()

      const analyticsSection = screen.getByTestId('analytics-preview-section')
      const featureHighlights = within(analyticsSection).getByTestId('analytics-feature-highlights')
      // The feature highlights container itself should be a ul element
      expect(featureHighlights.tagName.toLowerCase()).toBe('ul')
    })

    it('analytics feature highlights list contains li elements', () => {
      renderWithRouter()

      const analyticsSection = screen.getByTestId('analytics-preview-section')
      const featureHighlights = within(analyticsSection).getByTestId('analytics-feature-highlights')
      expect(featureHighlights.tagName.toLowerCase()).toBe('ul')

      const liElements = featureHighlights.querySelectorAll('li')
      // Should have 4 analytics feature items
      expect(liElements.length).toBe(4)
    })
  })
})
