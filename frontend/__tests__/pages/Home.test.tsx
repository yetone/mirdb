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

    const loginLink = screen.getByRole('link', { name: /login/i })
    expect(loginLink).toHaveAttribute('href', '/login')
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

    // Find the link wrapping the Login button
    const loginCtaLink = screen.getByRole('link', { name: /login/i })
    expect(loginCtaLink).toBeInTheDocument()
    expect(loginCtaLink).toHaveAttribute('href', '/login')
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

    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    const loginLink = screen.getByRole('link', { name: /login/i })

    expect(getStartedLink).toBeVisible()
    expect(loginLink).toBeVisible()
  })

  // Test that CTAs are accessible
  it('should have CTAs that are keyboard accessible', () => {
    renderWithProviders(<Home />)

    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    const loginLink = screen.getByRole('link', { name: /login/i })

    // Links should not have negative tabindex
    expect(getStartedLink).not.toHaveAttribute('tabindex', '-1')
    expect(loginLink).not.toHaveAttribute('tabindex', '-1')
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

    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    const loginLink = screen.getByRole('link', { name: /login/i })

    expect(getStartedLink).toBeVisible()
    expect(getStartedLink).toHaveAttribute('href', '/register')

    expect(loginLink).toBeVisible()
    expect(loginLink).toHaveAttribute('href', '/login')
  })

  // Test: Button layout adapts with sm: breakpoint for tablet
  it('should have CTA buttons with responsive flex layout for tablet', () => {
    renderWithProviders(<Home />)

    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    const buttonsContainer = getStartedLink.parentElement

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

    // CTAs
    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    const loginLink = screen.getByRole('link', { name: /login/i })
    expect(getStartedLink).toBeVisible()
    expect(loginLink).toBeVisible()

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
