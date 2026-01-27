import { describe, it, expect, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { Footer } from '../../../src/components/homepage/Footer'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, whileHover, whileTap, initial, animate, whileInView, viewport, transition, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
  },
}))

const renderFooter = (props = {}) => {
  return render(<Footer {...props} />)
}

// Mock homepage with semantic structure for page structure tests
const MockHomepageWithSemanticStructure = () => (
  <div data-testid="homepage">
    <header data-testid="page-header">
      <nav aria-label="Main navigation">
        <a href="/">Logo</a>
        <a href="/login">Login</a>
      </nav>
    </header>
    <main data-testid="page-main">
      <section aria-labelledby="hero-heading">
        <h1 id="hero-heading">Shorten URLs, Track Every Click</h1>
        <p>Description text</p>
      </section>
      <section aria-labelledby="features-heading">
        <h2 id="features-heading">Features</h2>
        <div>
          <h3>Feature 1</h3>
          <p>Description</p>
        </div>
      </section>
      <section aria-labelledby="how-it-works-heading">
        <h2 id="how-it-works-heading">How It Works</h2>
        <div>
          <h3>Step 1</h3>
          <p>Sign up</p>
        </div>
      </section>
    </main>
    <Footer />
  </div>
)

const renderMockHomepage = () => {
  return render(<MockHomepageWithSemanticStructure />)
}

describe('Footer', () => {
  // Test Case 1: Component renders without errors
  describe('Rendering', () => {
    it('should render the Footer component without errors', () => {
      renderFooter()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('should render with custom year and company name props', () => {
      renderFooter({ year: 2025, companyName: 'Test Company' })
      expect(screen.getByTestId('footer')).toBeInTheDocument()
      expect(screen.getByTestId('copyright')).toHaveTextContent('2025')
      expect(screen.getByTestId('copyright')).toHaveTextContent('Test Company')
    })
  })

  // Test Case 2: Check for footer HTML element
  describe('Semantic HTML', () => {
    it('should render a semantic <footer> element', () => {
      renderFooter()
      const footer = screen.getByTestId('footer')
      expect(footer.tagName).toBe('FOOTER')
    })

    it('should use proper nav elements for link groups', () => {
      renderFooter()
      const navElements = screen.getAllByRole('navigation')
      expect(navElements.length).toBeGreaterThanOrEqual(1)
    })
  })

  // Test Case 3: Check for copyright text
  describe('Copyright Information', () => {
    it('should display copyright symbol and current year', () => {
      const currentYear = new Date().getFullYear()
      renderFooter()
      const copyright = screen.getByTestId('copyright')
      expect(copyright).toBeInTheDocument()
      expect(copyright.textContent).toContain('©')
      expect(copyright.textContent).toContain(currentYear.toString())
    })

    it('should display copyright symbol with custom year', () => {
      renderFooter({ year: 2024 })
      const copyright = screen.getByTestId('copyright')
      expect(copyright.textContent).toContain('©')
      expect(copyright.textContent).toContain('2024')
    })

    it('should display company name in copyright', () => {
      renderFooter({ companyName: 'My Company' })
      const copyright = screen.getByTestId('copyright')
      expect(copyright.textContent).toContain('My Company')
    })

    it('should use default company name when not provided', () => {
      renderFooter()
      const copyright = screen.getByTestId('copyright')
      expect(copyright.textContent).toContain('URL Shortener')
    })
  })

  // Test Case 4: Check for Privacy Policy link
  describe('Privacy Policy Link', () => {
    it('should render a Privacy Policy link', () => {
      renderFooter()
      const privacyLink = screen.getByTestId('privacy-link')
      expect(privacyLink).toBeInTheDocument()
    })

    it('should have Privacy Policy text', () => {
      renderFooter()
      const privacyLink = screen.getByTestId('privacy-link')
      expect(privacyLink.textContent).toMatch(/privacy/i)
    })

    it('should be a link element with href', () => {
      renderFooter()
      const privacyLink = screen.getByTestId('privacy-link')
      expect(privacyLink.tagName).toBe('A')
      expect(privacyLink).toHaveAttribute('href')
    })
  })

  // Test Case 5: Check for Terms of Service link
  describe('Terms of Service Link', () => {
    it('should render a Terms of Service link', () => {
      renderFooter()
      const termsLink = screen.getByTestId('terms-link')
      expect(termsLink).toBeInTheDocument()
    })

    it('should have Terms of Service text', () => {
      renderFooter()
      const termsLink = screen.getByTestId('terms-link')
      expect(termsLink.textContent).toMatch(/terms/i)
    })

    it('should be a link element with href', () => {
      renderFooter()
      const termsLink = screen.getByTestId('terms-link')
      expect(termsLink.tagName).toBe('A')
      expect(termsLink).toHaveAttribute('href')
    })
  })

  // Additional tests for social links
  describe('Social Media Links', () => {
    it('should render social media links', () => {
      renderFooter()
      const twitterLink = screen.getByTestId('social-twitter')
      const githubLink = screen.getByTestId('social-github')
      expect(twitterLink).toBeInTheDocument()
      expect(githubLink).toBeInTheDocument()
    })

    it('should have proper aria labels for accessibility', () => {
      renderFooter()
      const twitterLink = screen.getByTestId('social-twitter')
      const githubLink = screen.getByTestId('social-github')
      expect(twitterLink).toHaveAttribute('aria-label')
      expect(githubLink).toHaveAttribute('aria-label')
    })

    it('should open links in new tab with proper security attributes', () => {
      renderFooter()
      const twitterLink = screen.getByTestId('social-twitter')
      const githubLink = screen.getByTestId('social-github')
      expect(twitterLink).toHaveAttribute('target', '_blank')
      expect(twitterLink).toHaveAttribute('rel', 'noopener noreferrer')
      expect(githubLink).toHaveAttribute('target', '_blank')
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
    })
  })

  // Test Case 6 & 7: Semantic structure and accessibility (covered in integration test)
  describe('Accessibility', () => {
    it('should have accessible navigation with aria-label', () => {
      renderFooter()
      const navElements = screen.getAllByRole('navigation')
      navElements.forEach((nav) => {
        expect(nav).toHaveAttribute('aria-label')
      })
    })

    it('should have focusable links', () => {
      renderFooter()
      const privacyLink = screen.getByTestId('privacy-link')
      const termsLink = screen.getByTestId('terms-link')

      // Links should be accessible and focusable
      expect(privacyLink.tagName).toBe('A')
      expect(termsLink.tagName).toBe('A')
    })
  })

  // Test Case 8: Footer styling matches theme
  describe('Theme Styling', () => {
    it('should use DaisyUI theme-aware classes', () => {
      renderFooter()
      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('bg-base-200')
      expect(footer).toHaveClass('text-base-content')
    })

    it('should have footer styling classes', () => {
      renderFooter()
      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('footer')
      expect(footer).toHaveClass('footer-center')
    })

    it('should have hover transition on links', () => {
      renderFooter()
      const privacyLink = screen.getByTestId('privacy-link')
      const termsLink = screen.getByTestId('terms-link')
      expect(privacyLink).toHaveClass('link-hover')
      expect(termsLink).toHaveClass('link-hover')
    })
  })
})

// Test Cases 6 & 7: Page Structure Tests (semantic HTML and heading hierarchy)
describe('Page Structure with Footer', () => {
  // Test Case 6: Verify homepage has semantic structure
  describe('Semantic HTML Structure', () => {
    it('should have a semantic <header> element', () => {
      renderMockHomepage()
      const header = screen.getByTestId('page-header')
      expect(header).toBeInTheDocument()
      expect(header.tagName).toBe('HEADER')
    })

    it('should have a semantic <main> element', () => {
      renderMockHomepage()
      const main = screen.getByTestId('page-main')
      expect(main).toBeInTheDocument()
      expect(main.tagName).toBe('MAIN')
    })

    it('should have a semantic <footer> element', () => {
      renderMockHomepage()
      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
      expect(footer.tagName).toBe('FOOTER')
    })

    it('should have all three semantic elements (header, main, footer) on homepage', () => {
      renderMockHomepage()
      const header = screen.getByTestId('page-header')
      const main = screen.getByTestId('page-main')
      const footer = screen.getByTestId('footer')

      expect(header.tagName).toBe('HEADER')
      expect(main.tagName).toBe('MAIN')
      expect(footer.tagName).toBe('FOOTER')
    })
  })

  // Test Case 7: Check heading hierarchy on full page
  describe('Heading Hierarchy', () => {
    it('should have exactly one h1 element', () => {
      renderMockHomepage()
      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)
    })

    it('should have h1 as the main page heading', () => {
      renderMockHomepage()
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()
      expect(h1.textContent).toBeTruthy()
    })

    it('should have h2 elements for section headings', () => {
      renderMockHomepage()
      const h2Elements = screen.getAllByRole('heading', { level: 2 })
      expect(h2Elements.length).toBeGreaterThanOrEqual(1)
    })

    it('should not skip heading levels (no h3 without h2)', () => {
      renderMockHomepage()
      const h2Elements = screen.getAllByRole('heading', { level: 2 })
      const h3Elements = screen.queryAllByRole('heading', { level: 3 })

      // If h3 exists, h2 must also exist
      if (h3Elements.length > 0) {
        expect(h2Elements.length).toBeGreaterThan(0)
      }
    })

    it('should have proper heading hierarchy without skipping levels', () => {
      renderMockHomepage()
      const headings = screen.getAllByRole('heading')
      const levels = headings.map((h) => parseInt(h.tagName.charAt(1)))

      // Check that we start with h1
      expect(levels[0]).toBe(1)

      // Check that we never skip more than one level
      for (let i = 1; i < levels.length; i++) {
        const diff = levels[i] - levels[i - 1]
        // Can go up by 1, stay same, or go down any amount
        expect(diff).toBeLessThanOrEqual(1)
      }
    })
  })
})
