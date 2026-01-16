import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import FooterCTA from './FooterCTA'
import { ThemeProvider } from '../contexts/ThemeContext'

// Mock framer-motion to avoid animation-related issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    h2: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
      <h2 {...props}>{children}</h2>
    ),
    p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => (
      <p {...props}>{children}</p>
    ),
  },
}))

const renderWithProviders = (component: React.ReactNode) => {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        {component}
      </ThemeProvider>
    </BrowserRouter>
  )
}

describe('FooterCTA', () => {
  describe('Component Rendering', () => {
    it('renders without errors', () => {
      renderWithProviders(<FooterCTA />)
      expect(screen.getByTestId('footer-cta')).toBeInTheDocument()
    })

    it('accepts custom data-testid prop', () => {
      renderWithProviders(<FooterCTA data-testid="custom-footer" />)
      expect(screen.getByTestId('custom-footer')).toBeInTheDocument()
    })

    it('renders headline text', () => {
      renderWithProviders(<FooterCTA />)
      const headline = screen.getByTestId('footer-headline')
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('Ready to Get Started?')
    })

    it('renders subheadline text', () => {
      renderWithProviders(<FooterCTA />)
      const subheadline = screen.getByTestId('footer-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline.textContent).toContain('Join thousands of users')
    })

    it('renders copyright text', () => {
      renderWithProviders(<FooterCTA />)
      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toBeInTheDocument()
      expect(copyright.textContent).toContain('URL Shortener')
    })
  })

  describe('CTA Button', () => {
    it('renders Get Started Free button', () => {
      renderWithProviders(<FooterCTA />)
      const ctaButton = screen.getByTestId('footer-cta-get-started')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toHaveTextContent('Get Started Free')
    })

    it('CTA button links to /register route', () => {
      renderWithProviders(<FooterCTA />)
      const ctaButton = screen.getByTestId('footer-cta-get-started')
      expect(ctaButton).toHaveAttribute('href', '/register')
    })

    it('CTA button has primary styling', () => {
      renderWithProviders(<FooterCTA />)
      const ctaButton = screen.getByTestId('footer-cta-get-started')
      expect(ctaButton).toHaveClass('bg-primary')
    })

    it('CTA button is a Link element for SPA navigation', () => {
      renderWithProviders(<FooterCTA />)
      const ctaButton = screen.getByTestId('footer-cta-get-started')
      expect(ctaButton.tagName.toLowerCase()).toBe('a')
    })
  })

  describe('Layout and Styling', () => {
    it('footer has proper background styling', () => {
      renderWithProviders(<FooterCTA />)
      const footer = screen.getByTestId('footer-cta')
      expect(footer).toHaveClass('bg-base-200')
    })

    it('footer uses semantic footer element', () => {
      renderWithProviders(<FooterCTA />)
      const footer = screen.getByTestId('footer-cta')
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })
  })

  describe('Accessibility', () => {
    it('has proper heading element', () => {
      renderWithProviders(<FooterCTA />)
      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toBeInTheDocument()
    })
  })
})
