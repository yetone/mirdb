/**
 * Responsive design integration tests for homepage.
 * Owner: Scenario 9 - Responsive Design - Tablet and Desktop
 *
 * These tests verify the responsive CSS classes and component structure
 * that enable proper tablet and desktop layouts.
 *
 * Note: Since jsdom doesn't fully support CSS media queries, we verify
 * that the correct responsive classes are applied to elements.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import Home from '@/pages/Home'
import FeaturesSection from '@/components/homepage/FeaturesSection'
import Navbar from '@/components/Navbar'
import { AuthProvider } from '@/contexts/AuthContext'
import { ThemeProvider } from '@/contexts/ThemeContext'

// Mock modules
vi.mock('@/contexts/AuthContext', async () => {
  const actual = await vi.importActual<typeof import('@/contexts/AuthContext')>(
    '@/contexts/AuthContext'
  )
  return {
    ...actual,
    useAuth: vi.fn(() => ({
      isAuthenticated: false,
      user: null,
      login: vi.fn(),
      logout: vi.fn(),
      register: vi.fn(),
    })),
    AuthProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  }
})

vi.mock('@/contexts/ThemeContext', async () => {
  const actual = await vi.importActual<typeof import('@/contexts/ThemeContext')>(
    '@/contexts/ThemeContext'
  )
  return {
    ...actual,
    useTheme: vi.fn(() => ({
      theme: 'light',
      setTheme: vi.fn(),
      toggleTheme: vi.fn(),
    })),
    ThemeProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  }
})

const renderWithRouter = (ui: React.ReactNode) => {
  return render(
    <MemoryRouter>
      <AuthProvider>
        <ThemeProvider>{ui}</ThemeProvider>
      </AuthProvider>
    </MemoryRouter>
  )
}

describe('Responsive Design - Tablet and Desktop', () => {
  describe('Navigation at Tablet/Desktop Width (768px+)', () => {
    it('renders navigation links visible by default (no hamburger menu)', () => {
      renderWithRouter(<Navbar />)

      // Navigation should always show Login and Sign Up links
      // There's no hamburger menu - links are always visible
      const loginLink = screen.getByRole('link', { name: /login/i })
      const signUpLink = screen.getByRole('link', { name: /sign up/i })

      expect(loginLink).toBeInTheDocument()
      expect(signUpLink).toBeInTheDocument()

      // Verify links are not conditionally hidden (no hidden/invisible classes)
      expect(loginLink).not.toHaveClass('hidden')
      expect(loginLink).not.toHaveClass('invisible')
      expect(signUpLink).not.toHaveClass('hidden')
      expect(signUpLink).not.toHaveClass('invisible')
    })

    it('renders brand link in navigation', () => {
      renderWithRouter(<Navbar />)

      const brandLink = screen.getByRole('link', { name: /url shortener/i })
      expect(brandLink).toBeInTheDocument()
      expect(brandLink).toHaveAttribute('href', '/')
    })

    it('renders theme toggle in navigation', () => {
      renderWithRouter(<Navbar />)

      // Theme toggle should be visible as a button with aria-label
      const themeToggle = screen.getByRole('button', { name: /toggle theme/i })
      expect(themeToggle).toBeInTheDocument()
    })
  })

  describe('Features Grid at Desktop Width (1024px+)', () => {
    it('features grid has responsive classes for multi-column layout', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Verify grid has responsive column classes
      // grid-cols-1 for mobile, md:grid-cols-2 for tablet, lg:grid-cols-4 for desktop
      expect(featuresGrid).toHaveClass('grid')
      expect(featuresGrid).toHaveClass('grid-cols-1')
      expect(featuresGrid).toHaveClass('md:grid-cols-2')
      expect(featuresGrid).toHaveClass('lg:grid-cols-4')
    })

    it('renders exactly 4 feature cards for 4-column grid', () => {
      renderWithRouter(<FeaturesSection />)

      const featureCards = screen.getAllByTestId(/^feature-card-/)
      expect(featureCards).toHaveLength(4)

      // Verify each expected feature card exists
      expect(screen.getByTestId('feature-card-url-shortening')).toBeInTheDocument()
      expect(screen.getByTestId('feature-card-analytics-dashboard')).toBeInTheDocument()
      expect(screen.getByTestId('feature-card-click-tracking')).toBeInTheDocument()
      expect(screen.getByTestId('feature-card-secure-reliable')).toBeInTheDocument()
    })

    it('features section has proper spacing classes', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('py-20')
      expect(featuresSection).toHaveClass('px-4')
    })
  })

  describe('Content Max-Width at Large Desktop (1440px+)', () => {
    it('features container has max-width constraint for centered content', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')

      // The inner container should have max-w-6xl and mx-auto for centering
      const container = featuresSection.querySelector('.max-w-6xl')
      expect(container).toBeInTheDocument()
      expect(container).toHaveClass('mx-auto')
    })

    it('homepage renders with min-height for proper layout', () => {
      renderWithRouter(<Home />)

      const homepage = screen.getByTestId('homepage')
      expect(homepage).toHaveClass('min-h-screen')
    })

    it('hero section has max-width constraint', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')

      // Hero content should be constrained
      const heroContent = heroSection.querySelector('.max-w-4xl')
      expect(heroContent).toBeInTheDocument()
      expect(heroContent).toHaveClass('mx-auto')
    })

    it('hero headline has responsive font sizes', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')

      // Should have base size text-4xl and responsive md:text-6xl
      expect(headline).toHaveClass('text-4xl')
      expect(headline).toHaveClass('md:text-6xl')
    })
  })

  describe('Full Homepage Responsive Layout', () => {
    it('renders all major sections', () => {
      renderWithRouter(<Home />)

      expect(screen.getByTestId('homepage')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('features section is accessible via anchor link', () => {
      renderWithRouter(<Home />)

      // The features section should have the id="features" for anchor navigation
      const featuresWrapper = document.querySelector('#features')
      expect(featuresWrapper).toBeInTheDocument()
    })

    it('grid gap is consistent for card spacing', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toHaveClass('gap-6')
    })
  })
})
