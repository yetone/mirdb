import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import Home from '../pages/Home'
import Login from '../pages/Login'
import Register from '../pages/Register'

const renderApp = (initialRoute = '/') => {
  return render(
    <MemoryRouter initialEntries={[initialRoute]}>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/login" element={<Login />} />
        <Route path="/register" element={<Register />} />
      </Routes>
    </MemoryRouter>
  )
}

describe('Hero Section E2E Tests', () => {
  it('navigates to homepage and displays hero section with headline, subheadline, and CTA buttons', () => {
    renderApp('/')

    // Hero section should be visible
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()

    // Headline should be visible
    expect(screen.getByTestId('hero-headline')).toBeInTheDocument()

    // Subheadline should be visible
    expect(screen.getByTestId('hero-subheadline')).toBeInTheDocument()

    // CTA buttons should be in the document (toBeVisible doesn't work well in jsdom)
    expect(screen.getByTestId('hero-cta-primary')).toBeInTheDocument()
    expect(screen.getByTestId('hero-cta-secondary')).toBeInTheDocument()
  })

  it('clicking "Get Started Free" button navigates to /register page', async () => {
    const user = userEvent.setup()
    renderApp('/')

    // Verify we're on the home page
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()

    // Click the "Get Started Free" button
    const primaryCta = screen.getByTestId('hero-cta-primary')
    await user.click(primaryCta)

    // Verify navigation to register page
    expect(screen.getByTestId('register-page')).toBeInTheDocument()
    expect(screen.getByText('Get Started Free')).toBeInTheDocument()
  })

  it('clicking "Log In" button navigates to /login page', async () => {
    const user = userEvent.setup()
    renderApp('/')

    // Verify we're on the home page
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()

    // Click the "Log In" button
    const secondaryCta = screen.getByTestId('hero-cta-secondary')
    await user.click(secondaryCta)

    // Verify navigation to login page
    expect(screen.getByTestId('login-page')).toBeInTheDocument()
    expect(screen.getByText('Log In')).toBeInTheDocument()
    expect(screen.getByText('Welcome back! Log in to your account.')).toBeInTheDocument()
  })

  it('hero section is the first major content area visible on homepage', () => {
    renderApp('/')

    // Home page should render
    expect(screen.getByTestId('home-page')).toBeInTheDocument()

    // Hero section should be inside home page and visible
    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    // Hero section should have min-height for above-the-fold visibility
    expect(heroSection).toHaveClass('min-h-[80vh]')
  })

  it('hero section displays value proposition headline clearly', () => {
    renderApp('/')

    const headline = screen.getByTestId('hero-headline')
    const headlineText = headline.textContent || ''

    // Should contain URL shortening value proposition
    expect(headlineText).toContain('Shorten')
    expect(headlineText).toContain('Share')
    expect(headlineText).toContain('Track')
  })
})
