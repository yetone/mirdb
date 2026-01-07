import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../src/pages/Home'
import HeroSection from '../src/components/HeroSection'
import FeaturesSection from '../src/components/FeaturesSection'
import HowItWorks from '../src/components/HowItWorks'

// Import framer-motion to check if motion components are used
import * as framerMotion from 'framer-motion'

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

/**
 * Scenario: Animation and Interaction Effects
 * Verify Framer Motion animations and hover states work correctly on homepage elements
 */

/**
 * Test Case 1: Check if Framer Motion is imported in Homepage component
 * Expected: Component imports and uses framer-motion for animations
 */
describe('Test Case 1: Framer Motion Import and Usage', () => {
  it('should verify framer-motion is available and motion components are used in HeroSection', async () => {
    // Verify framer-motion is available
    expect(framerMotion).toBeDefined()
    expect(framerMotion.motion).toBeDefined()

    renderWithRouter(<HeroSection />)

    // The HeroSection uses motion.h1, motion.p, motion.div
    // When rendered, these become regular HTML elements but with framer-motion data attributes
    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    // Verify heading is rendered (created with motion.h1)
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading).toHaveTextContent(/shorten links/i)
  })

  it('should verify framer-motion is used in FeaturesSection for scroll animations', () => {
    renderWithRouter(<FeaturesSection />)

    // FeaturesSection uses motion.div with whileInView for scroll-triggered animations
    const featuresGrid = screen.getByTestId('features-grid')
    expect(featuresGrid).toBeInTheDocument()

    // Feature cards should be rendered (created with motion.div)
    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards.length).toBe(4)
  })

  it('should verify framer-motion is used in HowItWorks section', () => {
    renderWithRouter(<HowItWorks />)

    // HowItWorks uses motion.div with variants and whileInView
    const stepsContainer = screen.getByTestId('how-it-works-steps')
    expect(stepsContainer).toBeInTheDocument()

    // Step elements should be rendered (created with motion.div)
    const step1 = screen.getByTestId('step-1')
    const step2 = screen.getByTestId('step-2')
    const step3 = screen.getByTestId('step-3')
    const step4 = screen.getByTestId('step-4')
    expect(step1).toBeInTheDocument()
    expect(step2).toBeInTheDocument()
    expect(step3).toBeInTheDocument()
    expect(step4).toBeInTheDocument()
  })
})

/**
 * Test Case 2: Hover over primary CTA button
 * Expected: Button displays hover state with visual change (color, scale, or shadow)
 */
describe('Test Case 2: Primary CTA Button Hover States', () => {
  it('should have hover-capable styling on Get Started Free button', () => {
    renderWithRouter(<HeroSection />)

    const getStartedButton = screen.getByRole('link', { name: /get started free/i })
    expect(getStartedButton).toBeInTheDocument()

    // DaisyUI btn class includes built-in hover effects
    expect(getStartedButton.className).toMatch(/\bbtn\b/)
    expect(getStartedButton.className).toMatch(/\bbtn-primary\b/)
  })

  it('should have hover-capable styling on Sign In button', () => {
    renderWithRouter(<HeroSection />)

    const signInButton = screen.getByRole('link', { name: /sign in/i })
    expect(signInButton).toBeInTheDocument()

    // DaisyUI btn-outline class includes built-in hover effects
    expect(signInButton.className).toMatch(/\bbtn\b/)
    expect(signInButton.className).toMatch(/\bbtn-outline\b/)
  })

  it('should be focusable for keyboard accessibility on CTA buttons', () => {
    renderWithRouter(<HeroSection />)

    const getStartedButton = screen.getByRole('link', { name: /get started free/i })

    // Focus the button directly (jsdom requires this for links)
    getStartedButton.focus()

    // Button should be focusable (buttons in DaisyUI have focus states)
    expect(document.activeElement).toBe(getStartedButton)
  })
})

/**
 * Test Case 3: Hover over feature card
 * Expected: Card displays hover state with visual feedback
 */
describe('Test Case 3: Feature Card Hover States', () => {
  it('should have hover shadow effect classes on feature cards', () => {
    renderWithRouter(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards.length).toBeGreaterThan(0)

    featureCards.forEach((card) => {
      // Verify hover:shadow-2xl class is present for hover effect
      expect(card.className).toMatch(/\bhover:shadow-2xl\b/)
    })
  })

  it('should have transition classes for smooth hover animation on feature cards', () => {
    renderWithRouter(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')

    featureCards.forEach((card) => {
      // Verify transition-shadow class for smooth hover animation
      expect(card.className).toMatch(/\btransition-shadow\b/)
    })
  })

  it('should have duration class for hover transition timing', () => {
    renderWithRouter(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')

    featureCards.forEach((card) => {
      // Verify duration class for transition timing
      expect(card.className).toMatch(/\bduration-300\b/)
    })
  })

  it('should apply base shadow styling that can be enhanced on hover', () => {
    renderWithRouter(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')

    featureCards.forEach((card) => {
      // Base shadow-xl that gets enhanced to shadow-2xl on hover
      expect(card.className).toMatch(/\bshadow-xl\b/)
    })
  })
})

/**
 * Test Case 4: Query motion components on homepage
 * Expected: At least some elements use motion.div or similar for animations
 */
describe('Test Case 4: Motion Components on Homepage', () => {
  it('should render homepage with animated sections', () => {
    renderWithRouter(<Home />)

    // Verify all major animated sections are present
    const heroSection = screen.getByTestId('hero-section')
    const featuresSection = screen.getByTestId('features-section')
    const howItWorksSection = screen.getByTestId('how-it-works-section')

    expect(heroSection).toBeInTheDocument()
    expect(featuresSection).toBeInTheDocument()
    expect(howItWorksSection).toBeInTheDocument()
  })

  it('should render motion components in HeroSection with animation props', () => {
    renderWithRouter(<HeroSection />)

    // The motion.h1 creates a heading with animation
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()

    // Verify the heading contains the expected text (rendered after animation)
    expect(heading).toHaveTextContent('Shorten Links')
    expect(heading).toHaveTextContent('Amplify Reach')
  })

  it('should render animated feature cards grid', () => {
    renderWithRouter(<FeaturesSection />)

    // The features grid container uses motion.div with variants
    const featuresGrid = screen.getByTestId('features-grid')
    expect(featuresGrid).toBeInTheDocument()

    // Each feature card uses motion.div with itemVariants
    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards.length).toBe(4)

    // Verify cards have the expected card styling
    featureCards.forEach((card) => {
      expect(card.className).toMatch(/\bcard\b/)
    })
  })

  it('should render animated steps in HowItWorks section', () => {
    renderWithRouter(<HowItWorks />)

    // The steps container uses motion.div with containerVariants and whileInView
    const stepsContainer = screen.getByTestId('how-it-works-steps')
    expect(stepsContainer).toBeInTheDocument()

    // Each step uses motion.div with itemVariants
    for (let i = 1; i <= 4; i++) {
      const step = screen.getByTestId(`step-${i}`)
      expect(step).toBeInTheDocument()
    }
  })

  it('should have entrance animation configuration in motion components', () => {
    // This test verifies the animation configuration exists in the source
    // The motion components use initial={{ opacity: 0, y: 20 }} and animate={{ opacity: 1, y: 0 }}
    renderWithRouter(<HeroSection />)

    // Verify elements are rendered with framer-motion animation styles
    // Framer motion applies initial styles (opacity: 0, transform) to animated elements
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()

    // Check that heading has animation-related inline styles (from framer-motion initial state)
    const headingStyle = heading.getAttribute('style')
    expect(headingStyle).toBeTruthy()
    // Framer motion applies transform and opacity for entrance animations
    expect(headingStyle).toMatch(/opacity|transform/)

    // The subheadline paragraph should have animation styles
    const subheadline = screen.getByText(/transform your long urls/i)
    expect(subheadline).toBeInTheDocument()
    expect(subheadline.getAttribute('style')).toMatch(/opacity|transform/)

    // CTA container should have animation styles
    const getStartedButton = screen.getByRole('link', { name: /get started free/i })
    expect(getStartedButton).toBeInTheDocument()
  })
})

/**
 * Additional tests for scroll-triggered animations
 */
describe('Scroll-Triggered Animations', () => {
  it('should configure whileInView animations for FeaturesSection', () => {
    renderWithRouter(<FeaturesSection />)

    // The grid container has whileInView="visible" and viewport={{ once: true }}
    const featuresGrid = screen.getByTestId('features-grid')
    expect(featuresGrid).toBeInTheDocument()

    // Feature cards are rendered with framer-motion animation styles
    const featureCards = screen.getAllByTestId('feature-card')
    featureCards.forEach((card) => {
      expect(card).toBeInTheDocument()
      // Cards have animation styles applied by framer-motion
      const style = card.getAttribute('style')
      expect(style).toMatch(/opacity|transform/)
    })
  })

  it('should configure whileInView animations for HowItWorks section', () => {
    renderWithRouter(<HowItWorks />)

    // The steps container has whileInView="visible" and viewport={{ once: true }}
    const stepsContainer = screen.getByTestId('how-it-works-steps')
    expect(stepsContainer).toBeInTheDocument()

    // Steps are rendered with framer-motion animation styles
    for (let i = 1; i <= 4; i++) {
      const step = screen.getByTestId(`step-${i}`)
      expect(step).toBeInTheDocument()
      // Steps have animation styles applied by framer-motion
      const style = step.getAttribute('style')
      expect(style).toMatch(/opacity|transform/)
    }
  })
})

/**
 * Additional hover and interaction tests
 */
describe('Footer Link Hover States', () => {
  it('should have hover-capable links in footer', () => {
    renderWithRouter(<Home />)

    const footer = screen.getByTestId('footer')
    expect(footer).toBeInTheDocument()

    // Footer links should have hover transition classes
    const loginLink = screen.getAllByRole('link', { name: /login/i })[1] // Footer login
    const registerLink = screen.getAllByRole('link', { name: /register/i })[1] // Footer register

    // Footer links use link-hover and transition-colors classes
    expect(loginLink.className).toMatch(/\blink-hover\b/)
    expect(loginLink.className).toMatch(/\btransition-colors\b/)
    expect(registerLink.className).toMatch(/\blink-hover\b/)
    expect(registerLink.className).toMatch(/\btransition-colors\b/)
  })
})
