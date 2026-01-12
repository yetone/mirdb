import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import Home from './Home'
import Register from './Register'
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

const renderWithRouter = (initialEntries = ['/']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <AuthProvider>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/register" element={<Register />} />
        </Routes>
      </AuthProvider>
    </MemoryRouter>
  )
}

describe('Home Page - Hero Section Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.getItem.mockReturnValue(null)
  })

  // Test Case 1: Hero section is rendered on landing page
  it('renders hero section on landing page', () => {
    renderWithRouter()

    expect(screen.getByTestId('home-page')).toBeInTheDocument()
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()
  })

  // Test Case 2: Hero headline contains URL shortening tagline
  it('displays hero headline with URL shortening value proposition', () => {
    renderWithRouter()

    const headline = screen.getByTestId('hero-headline')
    expect(headline).toBeInTheDocument()
    expect(headline.textContent).toMatch(/shorten.*share.*track/i)
  })

  // Test Case 3: Get Started Free button navigates to /register
  it('navigates to register page when clicking Get Started Free', async () => {
    renderWithRouter()

    const primaryCTA = screen.getByTestId('hero-cta-primary')
    fireEvent.click(primaryCTA)

    await waitFor(() => {
      expect(screen.getByTestId('register-page')).toBeInTheDocument()
    })
  })

  // Test Case 4: Learn More button scrolls to features section
  it('scrolls to features section when clicking Learn More', () => {
    renderWithRouter()

    const secondaryCTA = screen.getByTestId('hero-cta-secondary')
    const featuresSection = screen.getByTestId('features-section')

    expect(featuresSection).toBeInTheDocument()

    fireEvent.click(secondaryCTA)
    expect(Element.prototype.scrollIntoView).toHaveBeenCalled()
  })

  // Test Case 5: Visual element is visible in hero section
  it('displays visual element in hero section', () => {
    renderWithRouter()

    const visual = screen.getByTestId('hero-visual')
    expect(visual).toBeInTheDocument()

    // Verify there's an SVG illustration
    const svg = visual.querySelector('svg')
    expect(svg).toBeInTheDocument()
  })
})
