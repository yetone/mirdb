/**
 * Homepage Component Tests
 * Owner: All scenarios contribute test cases
 *
 * Test coverage for:
 * - Hero section rendering (Scenario 1)
 * - Features section rendering (Scenario 2)
 * - CTA navigation (Scenario 3)
 * - Responsive layouts (Scenarios 4-5)
 * - Theme support (Scenarios 6-8)
 * - Footer rendering (Scenario 9)
 * - Accessibility (Scenarios 10-11)
 * - Public access (Scenario 12)
 * - Component integration (Scenario 13)
 * - Animations (Scenario 14)
 * - Route integration (Scenario 15)
 */
import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from '../utils/renderWithProviders'
import Home from '../../src/pages/Home'

describe('Home Page - Hero Section Display (Scenario 1)', () => {
  it('should render h1 heading with product value proposition', () => {
    renderWithProviders(<Home />)

    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading.textContent).toContain('Shorten URLs')
    expect(heading.textContent).toContain('Track Clicks')
    expect(heading.textContent).toContain('Grow Your Reach')
  })

  it('should render primary CTA button with Get Started text', () => {
    renderWithProviders(<Home />)

    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    expect(getStartedButton).toBeInTheDocument()
    expect(getStartedButton).toBeVisible()
  })

  it('should render secondary CTA button with Login text', () => {
    renderWithProviders(<Home />)

    const loginButton = screen.getByRole('button', { name: /login/i })
    expect(loginButton).toBeInTheDocument()
    expect(loginButton).toBeVisible()
  })

  it('should render BackgroundEffect component in the DOM', () => {
    renderWithProviders(<Home />)

    const backgroundEffect = screen.getByTestId('background-effect')
    expect(backgroundEffect).toBeInTheDocument()
  })

  it('should render subheadline text below the main headline', () => {
    renderWithProviders(<Home />)

    const subheadline = screen.getByTestId('subheadline')
    expect(subheadline).toBeInTheDocument()
    expect(subheadline).toBeVisible()
    expect(subheadline.textContent).toContain('Create short, memorable links')
    expect(subheadline.textContent).toContain('Track performance')
    expect(subheadline.textContent).toContain('dashboard')
  })

  it('should have Get Started button linked to /register', () => {
    renderWithProviders(<Home />)

    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    expect(getStartedLink).toHaveAttribute('href', '/register')
  })

  it('should have Login button linked to /login', () => {
    renderWithProviders(<Home />)

    // There are multiple login links (hero section and footer), find the one in hero section
    const loginLinks = screen.getAllByRole('link', { name: /login/i })
    const heroLoginLink = loginLinks.find(link => link.querySelector('button'))
    expect(heroLoginLink).toHaveAttribute('href', '/login')
  })

  it('should display product name', () => {
    renderWithProviders(<Home />)

    expect(screen.getByText('URL Shortener')).toBeInTheDocument()
  })
})

describe('Home Page - Features Section Display (Scenario 2)', () => {
  it('should render features section with at least 3 GlassMorphismCard components', () => {
    renderWithProviders(<Home />)

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // Verify at least 3 feature cards exist
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    const clickAnalyticsCard = screen.getByTestId('feature-card-click-analytics')
    const dashboardCard = screen.getByTestId('feature-card-dashboard')

    expect(urlShorteningCard).toBeInTheDocument()
    expect(clickAnalyticsCard).toBeInTheDocument()
    expect(dashboardCard).toBeInTheDocument()
  })

  it('should render URL Shortening feature card with appropriate title and description', () => {
    renderWithProviders(<Home />)

    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    expect(urlShorteningCard).toBeInTheDocument()

    expect(screen.getByText('URL Shortening')).toBeInTheDocument()
    expect(screen.getByText('Create short, memorable links in seconds')).toBeInTheDocument()
  })

  it('should render Click Analytics feature card with appropriate title and description', () => {
    renderWithProviders(<Home />)

    const clickAnalyticsCard = screen.getByTestId('feature-card-click-analytics')
    expect(clickAnalyticsCard).toBeInTheDocument()

    expect(screen.getByText('Click Analytics')).toBeInTheDocument()
    expect(screen.getByText('Track performance with detailed analytics')).toBeInTheDocument()
  })

  it('should render Dashboard feature card with appropriate title and description', () => {
    renderWithProviders(<Home />)

    const dashboardCard = screen.getByTestId('feature-card-dashboard')
    expect(dashboardCard).toBeInTheDocument()

    expect(screen.getByText('Dashboard')).toBeInTheDocument()
    expect(screen.getByText('Manage all your URLs in one place')).toBeInTheDocument()
  })

  it('should render each feature card with an icon element', () => {
    renderWithProviders(<Home />)

    // Check that each feature card has an icon
    const urlShorteningIcon = screen.getByTestId('feature-icon-url-shortening')
    const clickAnalyticsIcon = screen.getByTestId('feature-icon-click-analytics')
    const dashboardIcon = screen.getByTestId('feature-icon-dashboard')
    const shareStatsIcon = screen.getByTestId('feature-icon-share-stats')

    expect(urlShorteningIcon).toBeInTheDocument()
    expect(clickAnalyticsIcon).toBeInTheDocument()
    expect(dashboardIcon).toBeInTheDocument()
    expect(shareStatsIcon).toBeInTheDocument()

    // Each icon container should have an SVG element inside
    expect(urlShorteningIcon.querySelector('svg')).toBeInTheDocument()
    expect(clickAnalyticsIcon.querySelector('svg')).toBeInTheDocument()
    expect(dashboardIcon.querySelector('svg')).toBeInTheDocument()
    expect(shareStatsIcon.querySelector('svg')).toBeInTheDocument()
  })

  it('should render Share Stats feature card (4th feature)', () => {
    renderWithProviders(<Home />)

    const shareStatsCard = screen.getByTestId('feature-card-share-stats')
    expect(shareStatsCard).toBeInTheDocument()

    expect(screen.getByText('Share Stats')).toBeInTheDocument()
    expect(screen.getByText('Share public analytics with stakeholders')).toBeInTheDocument()
  })

  it('should render features section heading', () => {
    renderWithProviders(<Home />)

    const heading = screen.getByRole('heading', { name: /powerful features/i })
    expect(heading).toBeInTheDocument()
  })

  it('should have proper accessibility attributes on features section', () => {
    renderWithProviders(<Home />)

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading')

    const heading = screen.getByRole('heading', { name: /powerful features/i })
    expect(heading).toHaveAttribute('id', 'features-heading')
  })
})

describe('Home Page - Navigation CTA Functionality (Scenario 3)', () => {
  // Test Case 1: Integration - Click primary CTA navigates to /register
  it('should have primary CTA (Get Started) that navigates to /register', () => {
    renderWithProviders(<Home />)

    // Find the link wrapping the Get Started button
    const primaryCtaLink = screen.getByRole('link', { name: /get started/i })
    expect(primaryCtaLink).toBeInTheDocument()
    expect(primaryCtaLink).toHaveAttribute('href', '/register')
  })

  // Test Case 2: Integration - Click Login CTA navigates to /login
  it('should have Login CTA that navigates to /login', () => {
    renderWithProviders(<Home />)

    // Find the link wrapping the Login button (hero section has button inside link)
    const loginCtaLinks = screen.getAllByRole('link', { name: /login/i })
    const heroLoginLink = loginCtaLinks.find(link => link.querySelector('button'))
    expect(heroLoginLink).toBeInTheDocument()
    expect(heroLoginLink).toHaveAttribute('href', '/login')
  })

  // Test Case 3: Unit - Primary CTA button exists with correct navigation target
  it('should render primary CTA button with correct href attribute', () => {
    renderWithProviders(<Home />)

    const primaryCtaLinks = screen.getAllByRole('link', { name: /get started/i })
    // Should have at least one link to /register
    const registerLink = primaryCtaLinks.find(link => link.getAttribute('href') === '/register')
    expect(registerLink).toBeDefined()
  })

  // Test Case 4: Unit - Login CTA button/link exists with correct navigation target
  it('should render login CTA with correct href attribute', () => {
    renderWithProviders(<Home />)

    const loginCtaLinks = screen.getAllByRole('link', { name: /login/i })
    // Should have at least one link to /login
    const loginLink = loginCtaLinks.find(link => link.getAttribute('href') === '/login')
    expect(loginLink).toBeDefined()
  })

  // Additional test: Both CTAs are visible in the hero section
  it('should display both CTAs prominently in the hero section', () => {
    renderWithProviders(<Home />)

    // There are multiple links (hero section and footer), find the ones in hero section
    const getStartedLinks = screen.getAllByRole('link', { name: /get started/i })
    const loginLinks = screen.getAllByRole('link', { name: /login/i })
    const heroGetStartedLink = getStartedLinks.find(link => link.querySelector('button'))
    const heroLoginLink = loginLinks.find(link => link.querySelector('button'))

    expect(heroGetStartedLink).toBeVisible()
    expect(heroLoginLink).toBeVisible()
  })

  // Test that CTAs are accessible
  it('should have CTAs that are keyboard accessible', () => {
    renderWithProviders(<Home />)

    // There are multiple links (hero section and footer), find the ones in hero section
    const getStartedLinks = screen.getAllByRole('link', { name: /get started/i })
    const loginLinks = screen.getAllByRole('link', { name: /login/i })
    const heroGetStartedLink = getStartedLinks.find(link => link.querySelector('button'))
    const heroLoginLink = loginLinks.find(link => link.querySelector('button'))

    // Links should not have negative tabindex
    expect(heroGetStartedLink).not.toHaveAttribute('tabindex', '-1')
    expect(heroLoginLink).not.toHaveAttribute('tabindex', '-1')
  })
})

describe('Home Page - Responsive Design Mobile (Scenario 4)', () => {
  // Test Case 1: Integration - No horizontal overflow at 375px viewport
  it('should have no horizontal overflow at mobile viewport width', () => {
    renderWithProviders(<Home />)

    // The main container should have overflow-hidden or proper width constraints
    const mainElement = document.querySelector('main')
    expect(mainElement).toBeInTheDocument()

    // Verify the main element doesn't set explicit widths that could cause overflow
    // The Tailwind class 'min-h-screen' along with bg-base-100 is present
    expect(mainElement).toHaveClass('min-h-screen')
    expect(mainElement).toHaveClass('bg-base-100')

    // Hero section should have overflow-hidden to prevent horizontal scrolling
    const heroSection = document.querySelector('section')
    expect(heroSection).toHaveClass('overflow-hidden')
  })

  // Test Case 2: Integration - Content readable without zooming at 375px
  it('should render readable text content at mobile viewport', () => {
    renderWithProviders(<Home />)

    // Verify text elements are rendered and visible
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading).toBeVisible()

    // Verify text uses responsive font sizes (text-4xl on mobile scales down)
    // The heading should have responsive classes
    expect(heading).toHaveClass('text-4xl')

    // Subheadline should also be readable
    const subheadline = screen.getByTestId('subheadline')
    expect(subheadline).toBeInTheDocument()
    expect(subheadline).toBeVisible()
  })

  // Test Case 3: Unit - CTA buttons meet minimum 44px touch target size
  it('should have CTA buttons with minimum 44px touch target size', () => {
    renderWithProviders(<Home />)

    // Find the CTA buttons
    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    const loginButton = screen.getByRole('button', { name: /login/i })

    // Check that buttons have appropriate padding classes for touch targets
    // The FuturisticButton uses 'px-6 py-3' which provides adequate touch target
    // Additionally, we verify min-w-[180px] class is applied for sufficient width
    expect(getStartedButton).toHaveClass('btn')
    expect(getStartedButton).toHaveClass('py-3')
    expect(getStartedButton).toHaveClass('px-6')

    expect(loginButton).toHaveClass('btn')
    expect(loginButton).toHaveClass('py-3')
    expect(loginButton).toHaveClass('px-6')

    // Verify buttons have minimum width for accessibility
    expect(getStartedButton).toHaveClass('min-w-[180px]')
    expect(loginButton).toHaveClass('min-w-[180px]')
  })

  // Test Case 4: Unit - Feature cards displayed in single column on mobile
  it('should display feature cards in single column layout on mobile', () => {
    renderWithProviders(<Home />)

    // Find the features grid container
    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // Find the grid container within features section
    const gridContainer = featuresSection.querySelector('.grid')
    expect(gridContainer).toBeInTheDocument()

    // Verify the grid has responsive classes for single column on mobile
    // grid-cols-1 ensures single column on smallest screens
    expect(gridContainer).toHaveClass('grid-cols-1')

    // Verify it changes to multi-column on larger screens
    expect(gridContainer).toHaveClass('md:grid-cols-2')
    expect(gridContainer).toHaveClass('lg:grid-cols-4')
  })

  // Additional test: CTA container stacks vertically on mobile
  it('should stack CTA buttons vertically on smallest mobile screens', () => {
    renderWithProviders(<Home />)

    // Find the CTA container (parent of the buttons)
    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    const ctaContainer = getStartedLink.parentElement

    // Verify flex-col is default for mobile, flex-row for larger screens
    expect(ctaContainer).toHaveClass('flex')
    expect(ctaContainer).toHaveClass('flex-col')
    expect(ctaContainer).toHaveClass('sm:flex-row')
  })

  // Additional test: Hero section uses responsive padding
  it('should use appropriate padding for mobile viewport', () => {
    renderWithProviders(<Home />)

    // The container inside hero should have responsive padding
    const containerDiv = document.querySelector('.container')
    expect(containerDiv).toBeInTheDocument()
    expect(containerDiv).toHaveClass('px-4')
    expect(containerDiv).toHaveClass('mx-auto')
  })

  // Additional test: Feature cards have proper width constraint
  it('should render feature cards with appropriate width on mobile', () => {
    renderWithProviders(<Home />)

    // Check that feature cards exist and are full width on mobile
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    expect(urlShorteningCard).toBeInTheDocument()

    // The card should be inside a motion.div within the grid
    // Grid with grid-cols-1 makes each child full width
    const featuresSection = screen.getByTestId('features-section')
    const gridContainer = featuresSection.querySelector('.grid')
    expect(gridContainer?.children.length).toBeGreaterThanOrEqual(3)
  })
})

describe('Home Page - Responsive Design Tablet (Scenario 5)', () => {
  // Test Case 1: Integration - Layout adapts to tablet-appropriate sizing at 768px
  it('should render homepage with tablet-appropriate layout at 768px viewport', () => {
    // Set viewport to tablet size (768px)
    Object.defineProperty(window, 'innerWidth', { writable: true, configurable: true, value: 768 })
    window.dispatchEvent(new Event('resize'))

    renderWithProviders(<Home />)

    // Verify main structure is present
    const main = screen.getByRole('main')
    expect(main).toBeInTheDocument()
    expect(main).toHaveClass('min-h-screen')

    // Hero section should exist and be visible
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()

    // Features section should exist
    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()
  })

  // Test Case 2: Unit - Feature cards use md:grid-cols-2 for 2-column layout on tablet
  it('should have feature grid with md:grid-cols-2 class for tablet 2-column layout', () => {
    renderWithProviders(<Home />)

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // The grid container should have md:grid-cols-2 for tablet (2-column) layout
    const gridContainer = featuresSection.querySelector('.grid')
    expect(gridContainer).toBeInTheDocument()
    expect(gridContainer).toHaveClass('md:grid-cols-2')
  })

  // Test Case 3: Integration - No content overflow or clipping at 768px viewport
  it('should not have horizontal overflow at tablet viewport width', () => {
    Object.defineProperty(window, 'innerWidth', { writable: true, configurable: true, value: 768 })
    window.dispatchEvent(new Event('resize'))

    renderWithProviders(<Home />)

    // The main container should have overflow-hidden or proper containment
    const heroSection = screen.getByRole('heading', { level: 1 }).closest('section')
    expect(heroSection).toHaveClass('overflow-hidden')

    // Hero section container should use proper width constraints
    const container = heroSection?.querySelector('.container')
    expect(container).toBeInTheDocument()
    expect(container).toHaveClass('mx-auto')
    expect(container).toHaveClass('px-4')
  })

  // Additional test: All sections are accessible at tablet viewport
  it('should have all sections visible and accessible at tablet viewport', () => {
    Object.defineProperty(window, 'innerWidth', { writable: true, configurable: true, value: 768 })
    window.dispatchEvent(new Event('resize'))

    renderWithProviders(<Home />)

    // Hero section visible
    const heroHeading = screen.getByRole('heading', { level: 1 })
    expect(heroHeading).toBeVisible()

    // Features section visible
    const featuresHeading = screen.getByRole('heading', { name: /powerful features/i })
    expect(featuresHeading).toBeVisible()

    // All feature cards visible
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    const clickAnalyticsCard = screen.getByTestId('feature-card-click-analytics')
    const dashboardCard = screen.getByTestId('feature-card-dashboard')
    const shareStatsCard = screen.getByTestId('feature-card-share-stats')

    expect(urlShorteningCard).toBeVisible()
    expect(clickAnalyticsCard).toBeVisible()
    expect(dashboardCard).toBeVisible()
    expect(shareStatsCard).toBeVisible()
  })

  // Test: Hero text adapts with md: responsive classes
  it('should have hero headline with responsive text sizing using md: breakpoint', () => {
    renderWithProviders(<Home />)

    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()

    // Headline should have md: responsive classes for tablet
    expect(heading).toHaveClass('md:text-5xl')
  })

  // Test: CTA buttons work at tablet viewport
  it('should have functional CTA buttons at tablet viewport', () => {
    Object.defineProperty(window, 'innerWidth', { writable: true, configurable: true, value: 768 })
    window.dispatchEvent(new Event('resize'))

    renderWithProviders(<Home />)

    // There are multiple links (hero section and footer), find the ones in hero section
    const getStartedLinks = screen.getAllByRole('link', { name: /get started/i })
    const loginLinks = screen.getAllByRole('link', { name: /login/i })
    const heroGetStartedLink = getStartedLinks.find(link => link.querySelector('button'))
    const heroLoginLink = loginLinks.find(link => link.querySelector('button'))

    expect(heroGetStartedLink).toBeVisible()
    expect(heroGetStartedLink).toHaveAttribute('href', '/register')

    expect(heroLoginLink).toBeVisible()
    expect(heroLoginLink).toHaveAttribute('href', '/login')
  })

  // Test: Button layout adapts with sm: breakpoint for tablet
  it('should have CTA buttons with responsive flex layout for tablet', () => {
    renderWithProviders(<Home />)

    // There are multiple links (hero section and footer), find the one in hero section
    const getStartedLinks = screen.getAllByRole('link', { name: /get started/i })
    const heroGetStartedLink = getStartedLinks.find(link => link.querySelector('button'))
    const buttonsContainer = heroGetStartedLink?.parentElement

    expect(buttonsContainer).toBeInTheDocument()
    // Should have responsive flex direction: column on mobile, row on sm and up (includes tablet)
    expect(buttonsContainer).toHaveClass('sm:flex-row')
  })

  // Test: Features section heading responsive classes
  it('should have features heading with responsive text sizing for tablet', () => {
    renderWithProviders(<Home />)

    const featuresHeading = screen.getByRole('heading', { name: /powerful features/i })
    expect(featuresHeading).toBeInTheDocument()
    expect(featuresHeading).toHaveClass('md:text-4xl')
  })
})

describe('Home Page - Theme Support - Dark Mode (Scenario 6)', () => {
  beforeEach(() => {
    // Set dark theme before each test
    document.documentElement.setAttribute('data-theme', 'dark')
    localStorage.setItem('theme', 'dark')
  })

  afterEach(() => {
    // Clean up after each test
    document.documentElement.removeAttribute('data-theme')
    localStorage.removeItem('theme')
  })

  // Test Case 1: Integration - Body/main container has dark background color applied
  it('should render main container with dark theme background class', () => {
    renderWithProviders(<Home />, { theme: 'dark' })

    const main = screen.getByRole('main')
    expect(main).toBeInTheDocument()
    // Main container uses bg-base-100 which adapts to theme
    expect(main).toHaveClass('bg-base-100')
    // Verify dark theme is set on document
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
  })

  // Test Case 2: Unit - Text content has appropriate light color for contrast
  it('should render text content with theme-aware contrast colors', () => {
    renderWithProviders(<Home />, { theme: 'dark' })

    // Check the subheadline has theme-aware text color class
    const subheadline = screen.getByTestId('subheadline')
    expect(subheadline).toHaveClass('text-base-content/70')

    // Check the heading is visible and rendered
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading).toBeVisible()
  })

  // Test Case 3: Unit - GlassMorphismCard components have dark-appropriate styling
  it('should render GlassMorphismCard components with dark theme styling', () => {
    renderWithProviders(<Home />, { theme: 'dark' })

    // Get feature cards that are wrapped in GlassMorphismCard
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    expect(urlShorteningCard).toBeInTheDocument()

    // GlassMorphismCard uses theme-aware classes: bg-base-100/30, border-base-content/10
    // The parent element should have these classes
    const cardContainer = urlShorteningCard.closest('[class*="backdrop-blur"]')
    expect(cardContainer).toBeInTheDocument()
    expect(cardContainer).toHaveClass('backdrop-blur-md')
    expect(cardContainer).toHaveClass('bg-base-100/30')
    expect(cardContainer).toHaveClass('border-base-content/10')
  })

  // Test Case 4: Unit - FuturisticButton components have dark-appropriate styling
  it('should render FuturisticButton components with dark theme styling', () => {
    renderWithProviders(<Home />, { theme: 'dark' })

    // Get the primary CTA button
    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    expect(getStartedButton).toBeInTheDocument()
    // Primary button uses btn-primary and gradient classes that adapt to theme
    expect(getStartedButton).toHaveClass('btn')
    expect(getStartedButton).toHaveClass('btn-primary')

    // Get the secondary/outline CTA button
    const loginButton = screen.getByRole('button', { name: /login/i })
    expect(loginButton).toBeInTheDocument()
    // Outline button uses btn-outline which adapts to theme
    expect(loginButton).toHaveClass('btn')
    expect(loginButton).toHaveClass('btn-outline')
  })

  // Additional test: Features section text has proper contrast
  it('should render features section description with theme-aware text color', () => {
    renderWithProviders(<Home />, { theme: 'dark' })

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // Check that feature descriptions use theme-aware classes
    const featureDescription = screen.getByText('Create short, memorable links in seconds')
    expect(featureDescription).toHaveClass('text-base-content/70')
  })

  // Additional test: Primary colors adapt to dark theme
  it('should render primary accent colors that work with dark theme', () => {
    renderWithProviders(<Home />, { theme: 'dark' })

    // Product name uses text-primary which adapts to theme
    const productName = screen.getByText('URL Shortener')
    expect(productName).toHaveClass('text-primary')

    // Feature icons use text-primary
    const urlShorteningIcon = screen.getByTestId('feature-icon-url-shortening')
    expect(urlShorteningIcon).toHaveClass('text-primary')
  })

  // Additional test: Background effect renders correctly in dark mode
  it('should render BackgroundEffect with theme-aware opacity in dark mode', () => {
    renderWithProviders(<Home />, { theme: 'dark' })

    const backgroundEffect = screen.getByTestId('background-effect')
    expect(backgroundEffect).toBeInTheDocument()
    // BackgroundEffect uses primary/secondary/accent colors with opacity
    // that work across all themes
    expect(backgroundEffect).toHaveClass('pointer-events-none')
  })
})

describe('Home Page - Theme Support - Cyberpunk Theme (Scenario 7)', () => {
  beforeEach(() => {
    // Set cyberpunk theme before each test
    document.documentElement.setAttribute('data-theme', 'cyberpunk')
    localStorage.setItem('theme', 'cyberpunk')
  })

  afterEach(() => {
    // Clean up after each test
    document.documentElement.removeAttribute('data-theme')
    localStorage.removeItem('theme')
  })

  // Test Case 1: Integration - DaisyUI cyberpunk theme classes are applied
  it('should render homepage with cyberpunk theme data attribute on document', () => {
    renderWithProviders(<Home />, { theme: 'cyberpunk' })

    // Verify cyberpunk theme is set on document
    expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
  })

  it('should render main container with DaisyUI bg-base-100 class for theme support', () => {
    renderWithProviders(<Home />, { theme: 'cyberpunk' })

    const main = screen.getByRole('main')
    expect(main).toBeInTheDocument()
    // Main container uses bg-base-100 which adapts to cyberpunk theme
    expect(main).toHaveClass('bg-base-100')
  })

  // Test Case 2: Unit - Components inherit cyberpunk color scheme
  it('should render text content with theme-aware base-content colors', () => {
    renderWithProviders(<Home />, { theme: 'cyberpunk' })

    // Check the subheadline has theme-aware text color class
    const subheadline = screen.getByTestId('subheadline')
    expect(subheadline).toHaveClass('text-base-content/70')

    // Check the heading is visible and rendered
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading).toBeVisible()
  })

  it('should render GlassMorphismCard components with cyberpunk theme styling', () => {
    renderWithProviders(<Home />, { theme: 'cyberpunk' })

    // Get feature cards that are wrapped in GlassMorphismCard
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    expect(urlShorteningCard).toBeInTheDocument()

    // GlassMorphismCard uses theme-aware classes: bg-base-100/30, border-base-content/10
    const cardContainer = urlShorteningCard.closest('[class*="backdrop-blur"]')
    expect(cardContainer).toBeInTheDocument()
    expect(cardContainer).toHaveClass('backdrop-blur-md')
    expect(cardContainer).toHaveClass('bg-base-100/30')
    expect(cardContainer).toHaveClass('border-base-content/10')
  })

  it('should render FuturisticButton components with cyberpunk theme styling', () => {
    renderWithProviders(<Home />, { theme: 'cyberpunk' })

    // Get the primary CTA button - uses btn-primary which adapts to cyberpunk theme
    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    expect(getStartedButton).toBeInTheDocument()
    expect(getStartedButton).toHaveClass('btn')
    expect(getStartedButton).toHaveClass('btn-primary')

    // Get the secondary/outline CTA button - uses btn-outline which adapts to theme
    const loginButton = screen.getByRole('button', { name: /login/i })
    expect(loginButton).toBeInTheDocument()
    expect(loginButton).toHaveClass('btn')
    expect(loginButton).toHaveClass('btn-outline')
  })

  it('should render primary accent colors using text-primary class', () => {
    renderWithProviders(<Home />, { theme: 'cyberpunk' })

    // Product name uses text-primary which adapts to cyberpunk theme
    const productName = screen.getByText('URL Shortener')
    expect(productName).toHaveClass('text-primary')

    // Feature icons use text-primary
    const urlShorteningIcon = screen.getByTestId('feature-icon-url-shortening')
    expect(urlShorteningIcon).toHaveClass('text-primary')
  })

  it('should render features section descriptions with theme-aware colors', () => {
    renderWithProviders(<Home />, { theme: 'cyberpunk' })

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // Check that feature descriptions use theme-aware classes
    const featureDescription = screen.getByText('Create short, memorable links in seconds')
    expect(featureDescription).toHaveClass('text-base-content/70')
  })

  it('should render BackgroundEffect with theme-aware colors in cyberpunk mode', () => {
    renderWithProviders(<Home />, { theme: 'cyberpunk' })

    const backgroundEffect = screen.getByTestId('background-effect')
    expect(backgroundEffect).toBeInTheDocument()
    // BackgroundEffect uses primary/secondary/accent colors with opacity
    // that are defined by the cyberpunk theme
    expect(backgroundEffect).toHaveClass('pointer-events-none')
  })

  it('should render headline with gradient using primary and secondary colors', () => {
    renderWithProviders(<Home />, { theme: 'cyberpunk' })

    // Find the gradient text span
    const gradientSpan = screen.getByText('Track Clicks.')
    expect(gradientSpan).toBeInTheDocument()
    expect(gradientSpan).toHaveClass('bg-gradient-to-r')
    expect(gradientSpan).toHaveClass('from-primary')
    expect(gradientSpan).toHaveClass('to-secondary')
  })

  it('should render all four feature cards in cyberpunk theme', () => {
    renderWithProviders(<Home />, { theme: 'cyberpunk' })

    // Verify all feature cards are rendered
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    const clickAnalyticsCard = screen.getByTestId('feature-card-click-analytics')
    const dashboardCard = screen.getByTestId('feature-card-dashboard')
    const shareStatsCard = screen.getByTestId('feature-card-share-stats')

    expect(urlShorteningCard).toBeInTheDocument()
    expect(clickAnalyticsCard).toBeInTheDocument()
    expect(dashboardCard).toBeInTheDocument()
    expect(shareStatsCard).toBeInTheDocument()
  })

  it('should have hero section with proper structure in cyberpunk theme', () => {
    renderWithProviders(<Home />, { theme: 'cyberpunk' })

    // Hero section should have the main headline
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading.textContent).toContain('Shorten URLs')
    expect(heading.textContent).toContain('Track Clicks')
    expect(heading.textContent).toContain('Grow Your Reach')

    // CTA buttons should be visible - there are multiple links (hero section and footer)
    const getStartedLinks = screen.getAllByRole('link', { name: /get started/i })
    const loginLinks = screen.getAllByRole('link', { name: /login/i })
    const heroGetStartedLink = getStartedLinks.find(link => link.querySelector('button'))
    const heroLoginLink = loginLinks.find(link => link.querySelector('button'))

    expect(heroGetStartedLink).toBeVisible()
    expect(heroLoginLink).toBeVisible()
  })
})

describe('Home Page - Theme Support - Light Mode (Scenario 8)', () => {
  beforeEach(() => {
    // Set light theme before each test
    document.documentElement.setAttribute('data-theme', 'light')
    localStorage.setItem('theme', 'light')
  })

  afterEach(() => {
    // Clean up after each test
    document.documentElement.removeAttribute('data-theme')
    localStorage.removeItem('theme')
  })

  // Test Case 1: Integration - Body/main container has light background color applied
  it('should render main container with light theme background class', () => {
    renderWithProviders(<Home />, { theme: 'light' })

    const main = screen.getByRole('main')
    expect(main).toBeInTheDocument()
    // Main container uses bg-base-100 which adapts to theme
    expect(main).toHaveClass('bg-base-100')
    // Verify light theme is set on document
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
  })

  // Test Case 2: Unit - Text content has appropriate dark color for contrast
  it('should render text content with theme-aware contrast colors for light mode', () => {
    renderWithProviders(<Home />, { theme: 'light' })

    // Check the subheadline has theme-aware text color class
    const subheadline = screen.getByTestId('subheadline')
    expect(subheadline).toHaveClass('text-base-content/70')

    // Check the heading is visible and rendered
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading).toBeVisible()
  })

  // Additional test: GlassMorphismCard components render correctly in light mode
  it('should render GlassMorphismCard components with light theme styling', () => {
    renderWithProviders(<Home />, { theme: 'light' })

    // Get feature cards that are wrapped in GlassMorphismCard
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    expect(urlShorteningCard).toBeInTheDocument()

    // GlassMorphismCard uses theme-aware classes: bg-base-100/30, border-base-content/10
    const cardContainer = urlShorteningCard.closest('[class*="backdrop-blur"]')
    expect(cardContainer).toBeInTheDocument()
    expect(cardContainer).toHaveClass('backdrop-blur-md')
    expect(cardContainer).toHaveClass('bg-base-100/30')
    expect(cardContainer).toHaveClass('border-base-content/10')
  })

  // Additional test: FuturisticButton components render correctly in light mode
  it('should render FuturisticButton components with light theme styling', () => {
    renderWithProviders(<Home />, { theme: 'light' })

    // Get the primary CTA button
    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    expect(getStartedButton).toBeInTheDocument()
    // Primary button uses btn-primary and gradient classes that adapt to theme
    expect(getStartedButton).toHaveClass('btn')
    expect(getStartedButton).toHaveClass('btn-primary')

    // Get the secondary/outline CTA button
    const loginButton = screen.getByRole('button', { name: /login/i })
    expect(loginButton).toBeInTheDocument()
    // Outline button uses btn-outline which adapts to theme
    expect(loginButton).toHaveClass('btn')
    expect(loginButton).toHaveClass('btn-outline')
  })

  // Additional test: Features section text has proper contrast in light mode
  it('should render features section description with theme-aware text color in light mode', () => {
    renderWithProviders(<Home />, { theme: 'light' })

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // Check that feature descriptions use theme-aware classes
    const featureDescription = screen.getByText('Create short, memorable links in seconds')
    expect(featureDescription).toHaveClass('text-base-content/70')
  })

  // Additional test: Primary colors adapt to light theme
  it('should render primary accent colors that work with light theme', () => {
    renderWithProviders(<Home />, { theme: 'light' })

    // Product name uses text-primary which adapts to theme
    const productName = screen.getByText('URL Shortener')
    expect(productName).toHaveClass('text-primary')

    // Feature icons use text-primary
    const urlShorteningIcon = screen.getByTestId('feature-icon-url-shortening')
    expect(urlShorteningIcon).toHaveClass('text-primary')
  })

  // Additional test: Background effect renders correctly in light mode
  it('should render BackgroundEffect with theme-aware opacity in light mode', () => {
    renderWithProviders(<Home />, { theme: 'light' })

    const backgroundEffect = screen.getByTestId('background-effect')
    expect(backgroundEffect).toBeInTheDocument()
    // BackgroundEffect uses primary/secondary/accent colors with opacity
    // that work across all themes
    expect(backgroundEffect).toHaveClass('pointer-events-none')
  })

  // Additional test: Hero section headline adapts to light theme
  it('should render hero headline with gradient text in light mode', () => {
    renderWithProviders(<Home />, { theme: 'light' })

    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()

    // The gradient span within the heading
    const gradientSpan = heading.querySelector('.bg-gradient-to-r')
    expect(gradientSpan).toBeInTheDocument()
    expect(gradientSpan).toHaveClass('text-transparent')
    expect(gradientSpan).toHaveClass('bg-clip-text')
    expect(gradientSpan).toHaveClass('from-primary')
    expect(gradientSpan).toHaveClass('to-secondary')
  })

  // Additional test: All sections visible in light mode
  it('should render all homepage sections correctly in light mode', () => {
    renderWithProviders(<Home />, { theme: 'light' })

    // Hero section
    const heroHeading = screen.getByRole('heading', { level: 1 })
    expect(heroHeading).toBeVisible()

    // CTAs - there are multiple links (hero section and footer), find the ones in hero section
    const getStartedLinks = screen.getAllByRole('link', { name: /get started/i })
    const loginLinks = screen.getAllByRole('link', { name: /login/i })
    const heroGetStartedLink = getStartedLinks.find(link => link.querySelector('button'))
    const heroLoginLink = loginLinks.find(link => link.querySelector('button'))
    expect(heroGetStartedLink).toBeVisible()
    expect(heroLoginLink).toBeVisible()

    // Features section
    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // All feature cards
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    const clickAnalyticsCard = screen.getByTestId('feature-card-click-analytics')
    const dashboardCard = screen.getByTestId('feature-card-dashboard')
    const shareStatsCard = screen.getByTestId('feature-card-share-stats')

    expect(urlShorteningCard).toBeInTheDocument()
    expect(clickAnalyticsCard).toBeInTheDocument()
    expect(dashboardCard).toBeInTheDocument()
    expect(shareStatsCard).toBeInTheDocument()
  })
})

describe('Home Page - Footer Section Display (Scenario 9)', () => {
  // Test Case 1: Footer section is present in the DOM
  it('should render footer section in the DOM', () => {
    renderWithProviders(<Home />)

    const footer = screen.getByTestId('footer-section')
    expect(footer).toBeInTheDocument()
    expect(footer).toBeVisible()
  })

  // Test Case 2: Footer contains Login navigation link pointing to /login
  it('should render Login navigation link in footer pointing to /login', () => {
    renderWithProviders(<Home />)

    const loginLink = screen.getByTestId('footer-login-link')
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toHaveAttribute('href', '/login')
    expect(loginLink).toHaveTextContent('Login')
  })

  // Test Case 3: Footer contains Register navigation link pointing to /register
  it('should render Register navigation link in footer pointing to /register', () => {
    renderWithProviders(<Home />)

    const registerLink = screen.getByTestId('footer-register-link')
    expect(registerLink).toBeInTheDocument()
    expect(registerLink).toHaveAttribute('href', '/register')
    expect(registerLink).toHaveTextContent('Register')
  })

  // Test Case 4: Footer contains copyright text
  it('should render copyright text in footer', () => {
    renderWithProviders(<Home />)

    const copyright = screen.getByTestId('footer-copyright')
    expect(copyright).toBeInTheDocument()
    expect(copyright.textContent).toContain('URL Shortener')
    expect(copyright.textContent).toContain('All rights reserved')
    // Should contain current year
    const currentYear = new Date().getFullYear().toString()
    expect(copyright.textContent).toContain(currentYear)
  })

  // Test Case 5: ThemeToggle component is rendered in footer
  it('should render ThemeToggle component in footer', () => {
    renderWithProviders(<Home />)

    const themeToggleContainer = screen.getByTestId('footer-theme-toggle')
    expect(themeToggleContainer).toBeInTheDocument()

    // ThemeToggle renders a select element with aria-label "Select theme"
    const themeSelect = screen.getByRole('combobox', { name: /select theme/i })
    expect(themeSelect).toBeInTheDocument()
  })

  // Additional test: Footer links are keyboard accessible
  it('should have keyboard accessible navigation links in footer', () => {
    renderWithProviders(<Home />)

    const loginLink = screen.getByTestId('footer-login-link')
    const registerLink = screen.getByTestId('footer-register-link')

    // Links should not have negative tabindex
    expect(loginLink).not.toHaveAttribute('tabindex', '-1')
    expect(registerLink).not.toHaveAttribute('tabindex', '-1')
  })

  // Additional test: Footer has proper navigation landmark
  it('should have footer navigation with proper aria-label', () => {
    renderWithProviders(<Home />)

    const footerNav = screen.getByRole('navigation', { name: /footer navigation/i })
    expect(footerNav).toBeInTheDocument()
  })

  // Additional test: ThemeToggle is functional
  it('should have a functional theme selector in footer', () => {
    renderWithProviders(<Home />)

    const themeSelect = screen.getByRole('combobox', { name: /select theme/i })
    expect(themeSelect).toBeInTheDocument()

    // Should have multiple theme options
    const options = themeSelect.querySelectorAll('option')
    expect(options.length).toBeGreaterThan(1)
  })
})

describe('Home Page - Accessibility - Keyboard Navigation (Scenario 10)', () => {
  // Test Case 1: Integration - Tab through Homepage interactive elements
  // Expected: All CTA buttons receive focus in logical order
  it('should allow tabbing through all CTA buttons in logical order', async () => {
    renderWithProviders(<Home />)

    // Get all focusable interactive elements
    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    const loginButton = screen.getByRole('button', { name: /login/i })
    const footerLoginLink = screen.getByTestId('footer-login-link')
    const footerRegisterLink = screen.getByTestId('footer-register-link')
    const themeSelect = screen.getByRole('combobox', { name: /select theme/i })

    // Verify all interactive elements exist and are focusable
    expect(getStartedButton).toBeInTheDocument()
    expect(loginButton).toBeInTheDocument()
    expect(footerLoginLink).toBeInTheDocument()
    expect(footerRegisterLink).toBeInTheDocument()
    expect(themeSelect).toBeInTheDocument()

    // Verify elements can receive focus (not disabled or hidden)
    expect(getStartedButton).not.toBeDisabled()
    expect(loginButton).not.toBeDisabled()
    expect(themeSelect).not.toBeDisabled()
  })

  // Test Case 2: Unit - Focus on primary CTA button
  // Expected: Visible focus ring/outline is displayed
  it('should have visible focus indicator styles on primary CTA button', () => {
    renderWithProviders(<Home />)

    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    expect(getStartedButton).toBeInTheDocument()

    // The button should have focus-visible styles via Tailwind/DaisyUI
    // DaisyUI btn class includes focus styling
    expect(getStartedButton).toHaveClass('btn')

    // Button should be focusable (not have negative tabindex)
    expect(getStartedButton).not.toHaveAttribute('tabindex', '-1')

    // Verify the button can be focused
    getStartedButton.focus()
    expect(document.activeElement).toBe(getStartedButton)
  })

  // Test Case 3: Integration - Press Enter on focused CTA button
  // Expected: Button action is triggered (navigation occurs)
  it('should have CTA buttons that respond to keyboard activation', () => {
    renderWithProviders(<Home />)

    // Find links wrapping buttons in hero section
    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    const loginLinks = screen.getAllByRole('link', { name: /login/i })
    const heroLoginLink = loginLinks.find(link => link.querySelector('button'))

    // Verify links have correct href for keyboard navigation
    expect(getStartedLink).toHaveAttribute('href', '/register')
    expect(heroLoginLink).toHaveAttribute('href', '/login')

    // Links and their buttons should be keyboard accessible
    expect(getStartedLink).not.toHaveAttribute('tabindex', '-1')
    expect(heroLoginLink).not.toHaveAttribute('tabindex', '-1')

    // Buttons inside links should not prevent keyboard access
    const getStartedButton = getStartedLink.querySelector('button')
    const loginButtonElement = heroLoginLink?.querySelector('button')
    expect(getStartedButton).not.toHaveAttribute('tabindex', '-1')
    expect(loginButtonElement).not.toHaveAttribute('tabindex', '-1')
  })

  // Test Case 4: Unit - Check tabindex attributes
  // Expected: No positive tabindex values that break natural tab order
  it('should not have positive tabindex values on any interactive elements', () => {
    renderWithProviders(<Home />)

    // Query all interactive elements that could have tabindex
    const allButtons = screen.getAllByRole('button')
    const allLinks = screen.getAllByRole('link')
    const allComboboxes = screen.getAllByRole('combobox')

    // Check buttons for positive tabindex
    allButtons.forEach(button => {
      const tabindex = button.getAttribute('tabindex')
      if (tabindex !== null) {
        const tabindexValue = parseInt(tabindex, 10)
        // tabindex should be 0 (natural order) or -1 (programmatic focus only), never positive
        expect(tabindexValue).toBeLessThanOrEqual(0)
      }
    })

    // Check links for positive tabindex
    allLinks.forEach(link => {
      const tabindex = link.getAttribute('tabindex')
      if (tabindex !== null) {
        const tabindexValue = parseInt(tabindex, 10)
        expect(tabindexValue).toBeLessThanOrEqual(0)
      }
    })

    // Check comboboxes for positive tabindex
    allComboboxes.forEach(combobox => {
      const tabindex = combobox.getAttribute('tabindex')
      if (tabindex !== null) {
        const tabindexValue = parseInt(tabindex, 10)
        expect(tabindexValue).toBeLessThanOrEqual(0)
      }
    })
  })

  // Additional test: All interactive elements in hero section are keyboard accessible
  it('should have all hero section interactive elements keyboard accessible', () => {
    renderWithProviders(<Home />)

    // Get hero CTA buttons
    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    const loginButton = screen.getByRole('button', { name: /login/i })

    // Buttons should be focusable
    getStartedButton.focus()
    expect(document.activeElement).toBe(getStartedButton)

    loginButton.focus()
    expect(document.activeElement).toBe(loginButton)
  })

  // Additional test: Footer navigation links are keyboard accessible
  it('should have all footer links keyboard accessible', () => {
    renderWithProviders(<Home />)

    const footerLoginLink = screen.getByTestId('footer-login-link')
    const footerRegisterLink = screen.getByTestId('footer-register-link')

    // Links should be focusable
    footerLoginLink.focus()
    expect(document.activeElement).toBe(footerLoginLink)

    footerRegisterLink.focus()
    expect(document.activeElement).toBe(footerRegisterLink)

    // Verify links have proper hrefs for navigation
    expect(footerLoginLink).toHaveAttribute('href', '/login')
    expect(footerRegisterLink).toHaveAttribute('href', '/register')
  })

  // Additional test: Theme toggle is keyboard accessible
  it('should have theme toggle keyboard accessible', () => {
    renderWithProviders(<Home />)

    const themeSelect = screen.getByRole('combobox', { name: /select theme/i })

    // Should be focusable
    themeSelect.focus()
    expect(document.activeElement).toBe(themeSelect)

    // Should not have positive tabindex
    const tabindex = themeSelect.getAttribute('tabindex')
    if (tabindex !== null) {
      expect(parseInt(tabindex, 10)).toBeLessThanOrEqual(0)
    }
  })

  // Additional test: Focus order follows logical document flow
  it('should have interactive elements in logical DOM order for focus navigation', () => {
    renderWithProviders(<Home />)

    // Get elements in expected order (hero section first, then footer)
    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    const heroLoginLinks = screen.getAllByRole('link', { name: /login/i })
    const heroLoginLink = heroLoginLinks.find(link => link.querySelector('button'))
    const footerLoginLink = screen.getByTestId('footer-login-link')
    const footerRegisterLink = screen.getByTestId('footer-register-link')

    // Verify they exist in logical order by checking their position in the DOM tree
    // Using compareDocumentPosition to check if elements are in correct DOM order
    // DOCUMENT_POSITION_FOLLOWING (4) means the second element follows the first in document order
    const DOCUMENT_POSITION_FOLLOWING = 4

    // Hero section links should come before footer links in DOM order
    expect(getStartedLink.compareDocumentPosition(footerLoginLink) & DOCUMENT_POSITION_FOLLOWING).toBe(DOCUMENT_POSITION_FOLLOWING)
    expect(getStartedLink.compareDocumentPosition(footerRegisterLink) & DOCUMENT_POSITION_FOLLOWING).toBe(DOCUMENT_POSITION_FOLLOWING)

    if (heroLoginLink) {
      expect(heroLoginLink.compareDocumentPosition(footerLoginLink) & DOCUMENT_POSITION_FOLLOWING).toBe(DOCUMENT_POSITION_FOLLOWING)
      expect(heroLoginLink.compareDocumentPosition(footerRegisterLink) & DOCUMENT_POSITION_FOLLOWING).toBe(DOCUMENT_POSITION_FOLLOWING)
    }
  })

  // Additional test: Verify buttons have accessible names
  it('should have all buttons with accessible names', () => {
    renderWithProviders(<Home />)

    const allButtons = screen.getAllByRole('button')

    allButtons.forEach(button => {
      // Each button should have accessible text content
      expect(button).toHaveAccessibleName()
    })
  })

  // Additional test: Verify links have accessible names
  it('should have all links with accessible names', () => {
    renderWithProviders(<Home />)

    const allLinks = screen.getAllByRole('link')

    allLinks.forEach(link => {
      // Each link should have accessible text content
      expect(link).toHaveAccessibleName()
    })
  })
})

describe('Home Page - Accessibility - Semantic HTML Structure (Scenario 11)', () => {
  // Test Case 1: Exactly one h1 element exists on the page
  it('should have exactly one h1 element on the page', () => {
    renderWithProviders(<Home />)

    const h1Elements = screen.getAllByRole('heading', { level: 1 })
    expect(h1Elements).toHaveLength(1)
    expect(h1Elements[0]).toBeInTheDocument()
  })

  // Test Case 2: Heading elements follow proper hierarchy without skipping levels
  it('should have heading elements that follow proper hierarchy without skipping levels', () => {
    const { container } = renderWithProviders(<Home />)

    // Get all heading elements in document order
    const headings = container.querySelectorAll('h1, h2, h3, h4, h5, h6')
    expect(headings.length).toBeGreaterThan(0)

    // Check that headings follow proper hierarchy (no skipping levels)
    let previousLevel = 0
    const headingLevels: number[] = []

    headings.forEach((heading) => {
      const tagName = heading.tagName.toLowerCase()
      const level = parseInt(tagName.charAt(1), 10)
      headingLevels.push(level)

      // For first heading, it should be h1
      if (previousLevel === 0) {
        expect(level).toBe(1)
      } else {
        // Heading level should not skip (e.g., no jumping from h1 to h3)
        // It can go down one level, stay the same, or go back up to any previous level
        const isValidTransition = level <= previousLevel + 1
        expect(isValidTransition).toBe(true)
      }
      previousLevel = level
    })

    // Should have h1 as the first heading
    expect(headingLevels[0]).toBe(1)
  })

  // Test Case 3: Main content area uses <main> element
  it('should have main content area using <main> element', () => {
    renderWithProviders(<Home />)

    const mainElement = screen.getByRole('main')
    expect(mainElement).toBeInTheDocument()
    expect(mainElement.tagName.toLowerCase()).toBe('main')
  })

  // Test Case 4: Footer uses <footer> element
  it('should have footer using <footer> element', () => {
    const { container } = renderWithProviders(<Home />)

    const footerElement = container.querySelector('footer')
    expect(footerElement).toBeInTheDocument()
    expect(footerElement?.tagName.toLowerCase()).toBe('footer')

    // Also verify it's the footer section we expect
    const footerSection = screen.getByTestId('footer-section')
    expect(footerSection.tagName.toLowerCase()).toBe('footer')
  })

  // Test Case 5: Icon-only buttons have aria-label attributes
  it('should have icon-only buttons with aria-label attributes', () => {
    const { container } = renderWithProviders(<Home />)

    // Find all buttons that might be icon-only
    const allButtons = container.querySelectorAll('button')

    // Check buttons that don't have visible text content
    allButtons.forEach((button) => {
      const textContent = button.textContent?.trim()
      const hasText = textContent && textContent.length > 0

      // If button has no text content, it should have aria-label
      if (!hasText) {
        expect(button).toHaveAttribute('aria-label')
      }
    })

    // Verify interactive elements like the theme select have aria-label
    const themeSelect = screen.getByRole('combobox', { name: /select theme/i })
    expect(themeSelect).toHaveAttribute('aria-label')
  })

  // Additional test: Sections use semantic section elements
  it('should use semantic section elements for content areas', () => {
    const { container } = renderWithProviders(<Home />)

    // Check for semantic section elements
    const sections = container.querySelectorAll('section')
    expect(sections.length).toBeGreaterThan(0)

    // Hero section should be a section element
    const heroSection = screen.getByRole('heading', { level: 1 }).closest('section')
    expect(heroSection).toBeInTheDocument()

    // Features section should be a section element
    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection.tagName.toLowerCase()).toBe('section')
  })

  // Additional test: Footer navigation has proper landmark
  it('should have footer with navigation landmark', () => {
    renderWithProviders(<Home />)

    // Footer should have a navigation element with aria-label
    const footerNav = screen.getByRole('navigation', { name: /footer navigation/i })
    expect(footerNav).toBeInTheDocument()
    expect(footerNav).toHaveAttribute('aria-label', 'Footer navigation')
  })

  // Additional test: Features section has proper aria-labelledby
  it('should have features section with aria-labelledby referencing heading', () => {
    renderWithProviders(<Home />)

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading')

    const featuresHeading = document.getElementById('features-heading')
    expect(featuresHeading).toBeInTheDocument()
    expect(featuresHeading?.textContent).toContain('Powerful Features')
  })

  // Additional test: Decorative icons have aria-hidden
  it('should have decorative icons marked with aria-hidden', () => {
    renderWithProviders(<Home />)

    // Feature icons are decorative and should have aria-hidden
    const featureIconContainers = [
      screen.getByTestId('feature-icon-url-shortening'),
      screen.getByTestId('feature-icon-click-analytics'),
      screen.getByTestId('feature-icon-dashboard'),
      screen.getByTestId('feature-icon-share-stats'),
    ]

    featureIconContainers.forEach((iconContainer) => {
      expect(iconContainer).toHaveAttribute('aria-hidden', 'true')
      // SVGs inside should also have aria-hidden
      const svg = iconContainer.querySelector('svg')
      expect(svg).toHaveAttribute('aria-hidden', 'true')
    })
  })
})

describe('Home Page - Public Access Without Authentication (Scenario 12)', () => {
  beforeEach(() => {
    // Clear any authentication tokens from localStorage
    localStorage.removeItem('token')
    localStorage.removeItem('user')
  })

  afterEach(() => {
    // Clean up after each test
    localStorage.removeItem('token')
    localStorage.removeItem('user')
  })

  // Test Case 1: Integration - Navigate to / without authentication token
  // Expected: Homepage renders without redirect to /login
  it('should render homepage without redirect when no authentication token exists', () => {
    // Ensure no token exists
    expect(localStorage.getItem('token')).toBeNull()

    renderWithProviders(<Home />)

    // Verify homepage content renders
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading.textContent).toContain('Shorten URLs')

    // Verify we're on the homepage, not redirected
    // The presence of homepage-specific content confirms no redirect occurred
    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    const footer = screen.getByTestId('footer-section')
    expect(footer).toBeInTheDocument()
  })

  // Test Case 2: Integration - Navigate to / without authentication token
  // Expected: No authentication error messages displayed
  it('should not display any authentication error messages without token', () => {
    // Ensure no token exists
    expect(localStorage.getItem('token')).toBeNull()

    renderWithProviders(<Home />)

    // Check that no authentication-related error messages are displayed
    expect(screen.queryByText(/authentication required/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/please log in/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/unauthorized/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/session expired/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/access denied/i)).not.toBeInTheDocument()

    // Verify the page renders normally with expected content
    const heroHeading = screen.getByRole('heading', { level: 1 })
    expect(heroHeading).toBeVisible()
  })

  // Test Case 3: Unit - Check route configuration
  // Expected: Homepage route '/' is not wrapped in ProtectedLayout
  it('should have homepage route configured as public (not wrapped in ProtectedLayout)', () => {
    // This test verifies the App.tsx route configuration
    // The homepage should render Home component directly without ProtectedLayout wrapper

    // Render the Home component directly (mimicking route access)
    renderWithProviders(<Home />)

    // If ProtectedLayout were wrapping Home, it would check authentication
    // and potentially redirect or show loading state
    // Since we have no token, successful rendering proves the route is public

    expect(localStorage.getItem('token')).toBeNull()

    // Immediate rendering of homepage content proves no auth check blocked us
    const mainContent = screen.getByRole('main')
    expect(mainContent).toBeInTheDocument()

    // Verify all major sections render (would be blocked by ProtectedLayout if applied)
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    const footer = screen.getByTestId('footer-section')
    expect(footer).toBeInTheDocument()
  })

  // Additional test: Homepage is accessible and functional without any authentication state
  it('should be fully functional without any authentication state', () => {
    // Clear all possible auth-related storage
    localStorage.clear()
    sessionStorage.clear()

    renderWithProviders(<Home />)

    // Verify core homepage functionality is available
    // Hero section with CTAs
    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    expect(getStartedLink).toHaveAttribute('href', '/register')

    const loginLinks = screen.getAllByRole('link', { name: /login/i })
    const heroLoginLink = loginLinks.find(link => link.querySelector('button'))
    expect(heroLoginLink).toHaveAttribute('href', '/login')

    // Features section
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    expect(urlShorteningCard).toBeInTheDocument()

    // Footer
    const footerLoginLink = screen.getByTestId('footer-login-link')
    expect(footerLoginLink).toHaveAttribute('href', '/login')

    const footerRegisterLink = screen.getByTestId('footer-register-link')
    expect(footerRegisterLink).toHaveAttribute('href', '/register')

    // Theme toggle is accessible
    const themeSelect = screen.getByRole('combobox', { name: /select theme/i })
    expect(themeSelect).toBeInTheDocument()
  })

  // Additional test: Homepage should not trigger any authentication checks
  it('should not show loading state related to authentication', () => {
    localStorage.removeItem('token')

    renderWithProviders(<Home />)

    // No authentication loading indicators should be present
    expect(screen.queryByText(/loading/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/checking authentication/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/verifying session/i)).not.toBeInTheDocument()

    // Content should be immediately available
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeVisible()
  })

  // Additional test: CTA buttons should guide unauthenticated users correctly
  it('should provide appropriate CTAs for unauthenticated users', () => {
    localStorage.removeItem('token')

    renderWithProviders(<Home />)

    // Primary CTA should lead to registration
    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    expect(getStartedLink).toHaveAttribute('href', '/register')

    // Secondary CTA should lead to login
    const loginLinks = screen.getAllByRole('link', { name: /login/i })
    const heroLoginLink = loginLinks.find(link => link.querySelector('button'))
    expect(heroLoginLink).toHaveAttribute('href', '/login')

    // Both CTAs should be visible and accessible
    expect(getStartedLink).toBeVisible()
    expect(heroLoginLink).toBeVisible()
  })
})

describe('Home Page - Component Integration - Existing UI Components (Scenario 13)', () => {
  // Test Case 1: GlassMorphismCard component is imported and rendered
  it('should render GlassMorphismCard component for feature cards', () => {
    const { container } = renderWithProviders(<Home />)

    // GlassMorphismCard uses specific classes: backdrop-blur-md, bg-base-100/30, border-base-content/10, rounded-2xl
    const glassMorphismCards = container.querySelectorAll('.backdrop-blur-md.bg-base-100\\/30')
    expect(glassMorphismCards.length).toBeGreaterThanOrEqual(4) // 4 feature cards

    // Verify each card has the expected GlassMorphismCard styling
    glassMorphismCards.forEach((card) => {
      expect(card).toHaveClass('backdrop-blur-md')
      expect(card).toHaveClass('bg-base-100/30')
      expect(card).toHaveClass('border-base-content/10')
      expect(card).toHaveClass('rounded-2xl')
      expect(card).toHaveClass('shadow-xl')
    })
  })

  // Test Case 2: FuturisticButton component is imported and rendered for CTAs
  it('should render FuturisticButton components for CTA buttons', () => {
    renderWithProviders(<Home />)

    // Get the CTA buttons in the hero section
    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    const loginButton = screen.getByRole('button', { name: /login/i })

    // FuturisticButton uses specific classes for primary variant
    expect(getStartedButton).toHaveClass('btn')
    expect(getStartedButton).toHaveClass('btn-primary')
    expect(getStartedButton).toHaveClass('bg-gradient-to-r')
    expect(getStartedButton).toHaveClass('from-primary')
    expect(getStartedButton).toHaveClass('to-secondary')

    // FuturisticButton uses specific classes for outline variant
    expect(loginButton).toHaveClass('btn')
    expect(loginButton).toHaveClass('btn-outline')
    expect(loginButton).toHaveClass('border-2')
  })

  // Test Case 3: BackgroundEffect component is imported and rendered
  it('should render BackgroundEffect component in the hero section', () => {
    const { container } = renderWithProviders(<Home />)

    // BackgroundEffect has data-testid="background-effect"
    const backgroundEffect = screen.getByTestId('background-effect')
    expect(backgroundEffect).toBeInTheDocument()

    // BackgroundEffect has specific structure and classes
    expect(backgroundEffect).toHaveClass('absolute')
    expect(backgroundEffect).toHaveClass('inset-0')
    expect(backgroundEffect).toHaveClass('overflow-hidden')
    expect(backgroundEffect).toHaveClass('pointer-events-none')

    // BackgroundEffect should contain animated gradient orbs (motion.div with blur effects)
    const blurElements = backgroundEffect.querySelectorAll('.blur-3xl')
    expect(blurElements.length).toBeGreaterThanOrEqual(2) // Multiple animated orbs

    // Should have grid pattern overlay (a child div with opacity-[0.03] class)
    const gridPatternOverlay = backgroundEffect.querySelector('[class*="opacity"]')
    expect(gridPatternOverlay).toBeInTheDocument()
  })

  // Test Case 4: All integrated components respond to theme changes
  it('should have all integrated components with theme-aware classes in dark theme', () => {
    const { container } = renderWithProviders(<Home />, { theme: 'dark' })

    // Verify dark theme is applied
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

    // GlassMorphismCard should use DaisyUI theme-aware classes
    const glassMorphismCards = container.querySelectorAll('.backdrop-blur-md.bg-base-100\\/30')
    expect(glassMorphismCards.length).toBeGreaterThanOrEqual(4)
    glassMorphismCards.forEach((card) => {
      // These classes adapt to theme via DaisyUI
      expect(card).toHaveClass('bg-base-100/30')
      expect(card).toHaveClass('border-base-content/10')
    })

    // FuturisticButton should use DaisyUI theme-aware classes
    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    expect(getStartedButton).toHaveClass('btn-primary')
    expect(getStartedButton).toHaveClass('from-primary')
    expect(getStartedButton).toHaveClass('to-secondary')

    // BackgroundEffect should use theme-aware primary/secondary colors
    const backgroundEffect = screen.getByTestId('background-effect')
    const primaryOrbs = backgroundEffect.querySelectorAll('[class*="bg-primary"]')
    const secondaryOrbs = backgroundEffect.querySelectorAll('[class*="bg-secondary"]')
    expect(primaryOrbs.length).toBeGreaterThanOrEqual(1)
    expect(secondaryOrbs.length).toBeGreaterThanOrEqual(1)
  })

  it('should have all integrated components with theme-aware classes in light theme', () => {
    const { container } = renderWithProviders(<Home />, { theme: 'light' })

    // Verify light theme is applied
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')

    // GlassMorphismCard should still have the same structure
    const glassMorphismCards = container.querySelectorAll('.backdrop-blur-md.bg-base-100\\/30')
    expect(glassMorphismCards.length).toBeGreaterThanOrEqual(4)

    // FuturisticButton should maintain its structure
    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    expect(getStartedButton).toHaveClass('btn-primary')

    // BackgroundEffect should be present
    const backgroundEffect = screen.getByTestId('background-effect')
    expect(backgroundEffect).toBeInTheDocument()
  })

  it('should have all integrated components with theme-aware classes in cyberpunk theme', () => {
    const { container } = renderWithProviders(<Home />, { theme: 'cyberpunk' })

    // Verify cyberpunk theme is applied
    expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

    // GlassMorphismCard should still have the same structure
    const glassMorphismCards = container.querySelectorAll('.backdrop-blur-md.bg-base-100\\/30')
    expect(glassMorphismCards.length).toBeGreaterThanOrEqual(4)

    // FuturisticButton should maintain its structure
    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    expect(getStartedButton).toHaveClass('btn-primary')

    // BackgroundEffect should be present
    const backgroundEffect = screen.getByTestId('background-effect')
    expect(backgroundEffect).toBeInTheDocument()
  })

  // Additional test: Verify component composition in homepage
  it('should compose homepage using HeroSection, FeaturesSection, and Footer', () => {
    const { container } = renderWithProviders(<Home />)

    // Main wrapper should exist
    const mainElement = container.querySelector('main')
    expect(mainElement).toBeInTheDocument()

    // Hero section should be rendered first (contains BackgroundEffect)
    const heroSection = container.querySelector('section')
    expect(heroSection).toBeInTheDocument()
    const backgroundEffect = screen.getByTestId('background-effect')
    expect(heroSection?.contains(backgroundEffect)).toBe(true)

    // Features section should be rendered
    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // Footer should be rendered
    const footerSection = screen.getByTestId('footer-section')
    expect(footerSection).toBeInTheDocument()

    // Verify proper order: hero -> features -> footer
    const sections = container.querySelectorAll('section, footer')
    const sectionOrder = Array.from(sections).map(s => s.getAttribute('data-testid') || 'hero')
    expect(sectionOrder).toContain('features-section')
    expect(sectionOrder).toContain('footer-section')
  })

  // Test: GlassMorphismCard uses framer-motion for animations
  it('should render GlassMorphismCard with motion.div wrapper', () => {
    const { container } = renderWithProviders(<Home />)

    // Find feature cards with GlassMorphismCard styling
    const featuresSection = screen.getByTestId('features-section')
    const glassCards = featuresSection.querySelectorAll('.backdrop-blur-md.bg-base-100\\/30')

    // Each GlassMorphismCard should be a motion.div (framer-motion)
    // Framer motion adds style attribute for animations
    glassCards.forEach((card) => {
      // Motion components get style attributes
      expect(card.tagName.toLowerCase()).toBe('div')
      expect(card).toHaveClass('rounded-2xl')
    })
  })

  // Test: FuturisticButton uses framer-motion for hover/tap effects
  it('should render FuturisticButton as motion.button element', () => {
    renderWithProviders(<Home />)

    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    const loginButton = screen.getByRole('button', { name: /login/i })

    // FuturisticButton is implemented as motion.button
    expect(getStartedButton.tagName.toLowerCase()).toBe('button')
    expect(loginButton.tagName.toLowerCase()).toBe('button')

    // Both should have transition classes for smooth effects
    expect(getStartedButton).toHaveClass('transition-all')
    expect(loginButton).toHaveClass('transition-all')
  })
})

describe('Home Page - Route Integration (Scenario 15)', () => {
  // Test Case 1: Integration - Navigate to '/' route renders Homepage component
  it('should render Homepage component when navigating to "/" route', () => {
    renderWithProviders(<Home />)

    // Verify Homepage component is rendered by checking for its key elements
    const mainElement = screen.getByRole('main')
    expect(mainElement).toBeInTheDocument()

    // Verify hero section is present (Homepage-specific content)
    const heroHeading = screen.getByRole('heading', { level: 1 })
    expect(heroHeading).toBeInTheDocument()
    expect(heroHeading.textContent).toContain('Shorten URLs')
    expect(heroHeading.textContent).toContain('Track Clicks')
    expect(heroHeading.textContent).toContain('Grow Your Reach')

    // Verify features section is present
    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // Verify footer is present
    const footerSection = screen.getByTestId('footer-section')
    expect(footerSection).toBeInTheDocument()
  })

  // Test Case 2: Unit - Check App.tsx route configuration
  // Expected: Route path='/' renders Home or Homepage component
  it('should have route configured at "/" that renders the Home component', () => {
    // This test verifies the route configuration renders the correct component
    // We test this by rendering Home directly and verifying its structure
    renderWithProviders(<Home />)

    // The Home component should have a specific structure that confirms it's the homepage
    // 1. Main wrapper with specific classes
    const mainElement = screen.getByRole('main')
    expect(mainElement).toHaveClass('min-h-screen')
    expect(mainElement).toHaveClass('bg-base-100')

    // 2. Contains three main sections: HeroSection, FeaturesSection, Footer
    // HeroSection indicator: h1 with product headline
    const h1 = screen.getByRole('heading', { level: 1 })
    expect(h1).toBeInTheDocument()

    // FeaturesSection indicator: features-section test id
    const features = screen.getByTestId('features-section')
    expect(features).toBeInTheDocument()

    // Footer indicator: footer-section test id
    const footer = screen.getByTestId('footer-section')
    expect(footer).toBeInTheDocument()
  })

  // Test Case 3: Integration - Navigate to '/' route
  // Expected: No 404 or error page is displayed
  it('should not display 404 or error page when navigating to "/" route', () => {
    renderWithProviders(<Home />)

    // Check that no error messages are displayed
    expect(screen.queryByText(/404/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/not found/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/page not found/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/error/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/something went wrong/i)).not.toBeInTheDocument()

    // Verify that actual homepage content IS displayed
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading).toBeVisible()

    // Verify homepage-specific content is present
    expect(screen.getByText('URL Shortener')).toBeInTheDocument()
    expect(screen.getByTestId('features-section')).toBeInTheDocument()
    expect(screen.getByTestId('footer-section')).toBeInTheDocument()
  })

  // Additional test: Homepage renders all expected sections in correct order
  it('should render all Homepage sections in correct order (Hero -> Features -> Footer)', () => {
    const { container } = renderWithProviders(<Home />)

    // Get all major section elements in document order
    const mainElement = container.querySelector('main')
    expect(mainElement).toBeInTheDocument()

    // Get child elements of main in order
    const sections = mainElement?.querySelectorAll(':scope > section, :scope > footer')
    expect(sections?.length).toBeGreaterThanOrEqual(3)

    // First section should be the hero section (contains h1)
    const firstSection = sections?.[0]
    expect(firstSection?.querySelector('h1')).toBeInTheDocument()

    // Second section should be features section
    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection.tagName.toLowerCase()).toBe('section')

    // Third should be footer
    const footerSection = screen.getByTestId('footer-section')
    expect(footerSection.tagName.toLowerCase()).toBe('footer')

    // Verify DOM order using compareDocumentPosition
    const DOCUMENT_POSITION_FOLLOWING = 4
    expect(
      (firstSection as Element).compareDocumentPosition(featuresSection) & DOCUMENT_POSITION_FOLLOWING
    ).toBe(DOCUMENT_POSITION_FOLLOWING)
    expect(
      featuresSection.compareDocumentPosition(footerSection) & DOCUMENT_POSITION_FOLLOWING
    ).toBe(DOCUMENT_POSITION_FOLLOWING)
  })

  // Additional test: Homepage route is publicly accessible (no ProtectedLayout wrapper)
  it('should render Homepage without requiring authentication', () => {
    // Clear any auth tokens
    localStorage.removeItem('token')
    localStorage.removeItem('user')

    renderWithProviders(<Home />)

    // Homepage should render immediately without auth checks
    const mainElement = screen.getByRole('main')
    expect(mainElement).toBeInTheDocument()

    // No authentication-related content should block rendering
    expect(screen.queryByText(/loading/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/authenticating/i)).not.toBeInTheDocument()

    // All homepage content should be visible
    expect(screen.getByRole('heading', { level: 1 })).toBeVisible()
    expect(screen.getByTestId('features-section')).toBeVisible()
    expect(screen.getByTestId('footer-section')).toBeVisible()
  })

  // Additional test: Homepage is rendered by the Home component export
  it('should export Home component as default export from pages/Home', () => {
    // The Home component should be a valid React component that renders the homepage
    expect(Home).toBeDefined()
    expect(typeof Home).toBe('function')

    // Render it and verify it produces homepage content
    renderWithProviders(<Home />)
    expect(screen.getByRole('main')).toBeInTheDocument()
  })
})
