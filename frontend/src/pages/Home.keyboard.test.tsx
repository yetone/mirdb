import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import Home from './Home'
import Register from './Register'
import Login from './Login'
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

const renderWithRouter = (initialEntries = ['/']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <AuthProvider>
        <Navbar />
        <div className="pt-16">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/register" element={<Register />} />
            <Route path="/login" element={<Login />} />
          </Routes>
        </div>
      </AuthProvider>
    </MemoryRouter>
  )
}

describe('Accessibility - Keyboard Navigation', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.getItem.mockReturnValue(null)
  })

  describe('Test Case 1: Tab through all interactive elements', () => {
    it('all buttons, links, and inputs are reachable via Tab key', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      // Get all interactive elements that should be focusable
      const navLogo = screen.getByTestId('nav-logo')
      const navFeatures = screen.getByTestId('nav-features')
      const navHowItWorks = screen.getByTestId('nav-how-it-works')
      const themeToggle = screen.getByTestId('theme-toggle')
      const navLogin = screen.getByTestId('nav-login')
      const navRegister = screen.getByTestId('nav-register')
      const heroCTAPrimary = screen.getByTestId('hero-cta-primary')
      const heroCTASecondary = screen.getByTestId('hero-cta-secondary')
      const demoInput = screen.getByTestId('demo-input')
      const demoShortenButton = screen.getByTestId('demo-shorten-button')

      // Start tabbing and verify each element can receive focus
      await user.tab()
      expect(navLogo).toHaveFocus()

      await user.tab()
      expect(navFeatures).toHaveFocus()

      await user.tab()
      expect(navHowItWorks).toHaveFocus()

      await user.tab()
      expect(themeToggle).toHaveFocus()

      await user.tab()
      expect(navLogin).toHaveFocus()

      await user.tab()
      expect(navRegister).toHaveFocus()

      await user.tab()
      expect(heroCTAPrimary).toHaveFocus()

      await user.tab()
      expect(heroCTASecondary).toHaveFocus()

      // Continue tabbing to demo section
      // Note: There may be other elements in between (feature cards, etc.)
      // Let's verify the demo input and button are eventually reachable
      let foundDemoInput = false
      let foundDemoButton = false
      let tabCount = 0
      const maxTabs = 30 // Safety limit

      while ((!foundDemoInput || !foundDemoButton) && tabCount < maxTabs) {
        await user.tab()
        tabCount++
        if (document.activeElement === demoInput) {
          foundDemoInput = true
        }
        if (document.activeElement === demoShortenButton) {
          foundDemoButton = true
        }
      }

      expect(foundDemoInput).toBe(true)
      expect(foundDemoButton).toBe(true)
    })

    it('interactive elements have correct tabindex attribute', () => {
      renderWithRouter()

      // Buttons and links should be naturally focusable (no explicit tabindex needed)
      // or have tabindex="0" if they need explicit focus
      const navLogo = screen.getByTestId('nav-logo')
      const navFeatures = screen.getByTestId('nav-features')
      const navLogin = screen.getByTestId('nav-login')
      const heroCTAPrimary = screen.getByTestId('hero-cta-primary')
      const demoInput = screen.getByTestId('demo-input')
      const demoShortenButton = screen.getByTestId('demo-shorten-button')

      // These should NOT have tabindex="-1" which would make them unfocusable
      expect(navLogo).not.toHaveAttribute('tabindex', '-1')
      expect(navFeatures).not.toHaveAttribute('tabindex', '-1')
      expect(navLogin).not.toHaveAttribute('tabindex', '-1')
      expect(heroCTAPrimary).not.toHaveAttribute('tabindex', '-1')
      expect(demoInput).not.toHaveAttribute('tabindex', '-1')
      expect(demoShortenButton).not.toHaveAttribute('tabindex', '-1')
    })
  })

  describe('Test Case 2: Check focus indicator on navigation links', () => {
    it('navigation links have focus-visible styling classes', () => {
      renderWithRouter()

      const navLogo = screen.getByTestId('nav-logo')
      const navFeatures = screen.getByTestId('nav-features')
      const navHowItWorks = screen.getByTestId('nav-how-it-works')
      const navLogin = screen.getByTestId('nav-login')
      const navRegister = screen.getByTestId('nav-register')
      const themeToggle = screen.getByTestId('theme-toggle')

      // DaisyUI btn classes provide focus ring styling
      // Check that elements have btn class which includes focus styles
      expect(navLogo).toHaveClass('btn')
      expect(navFeatures).toHaveClass('btn')
      expect(navHowItWorks).toHaveClass('btn')
      expect(navLogin).toHaveClass('btn')
      expect(navRegister).toHaveClass('btn')
      expect(themeToggle).toHaveClass('btn')
    })

    it('focused elements can be distinguished visually', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const navLogo = screen.getByTestId('nav-logo')

      // Tab to the logo
      await user.tab()
      expect(navLogo).toHaveFocus()

      // The element should have styling classes that provide focus indication
      // DaisyUI btn class includes focus:outline-none focus:ring focus:ring-offset
      // We check that the element is focusable and has proper btn styling
      expect(navLogo.tagName.toLowerCase()).toBe('a')
      expect(navLogo).toHaveClass('btn')
    })

    it('all interactive elements in navigation are buttons or links', () => {
      renderWithRouter()

      const navLogo = screen.getByTestId('nav-logo')
      const navFeatures = screen.getByTestId('nav-features')
      const navHowItWorks = screen.getByTestId('nav-how-it-works')
      const navLogin = screen.getByTestId('nav-login')
      const navRegister = screen.getByTestId('nav-register')
      const themeToggle = screen.getByTestId('theme-toggle')

      // Check semantic HTML - should be links or buttons
      expect(navLogo.tagName.toLowerCase()).toBe('a')
      expect(navFeatures.tagName.toLowerCase()).toBe('button')
      expect(navHowItWorks.tagName.toLowerCase()).toBe('button')
      expect(navLogin.tagName.toLowerCase()).toBe('a')
      expect(navRegister.tagName.toLowerCase()).toBe('a')
      expect(themeToggle.tagName.toLowerCase()).toBe('button')
    })
  })

  describe('Test Case 3: Press Enter on focused CTA button', () => {
    it('pressing Enter on Get Started Free button navigates to register', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const heroCTAPrimary = screen.getByTestId('hero-cta-primary')

      // Focus the CTA button
      heroCTAPrimary.focus()
      expect(heroCTAPrimary).toHaveFocus()

      // Press Enter
      await user.keyboard('{Enter}')

      // Should navigate to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })
    })

    it('pressing Enter on Login link navigates to login', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const navLogin = screen.getByTestId('nav-login')

      // Focus the login link
      navLogin.focus()
      expect(navLogin).toHaveFocus()

      // Press Enter
      await user.keyboard('{Enter}')

      // Should navigate to login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })
    })

    it('pressing Enter on Learn More button triggers scroll', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const heroCTASecondary = screen.getByTestId('hero-cta-secondary')

      // Focus the secondary CTA
      heroCTASecondary.focus()
      expect(heroCTASecondary).toHaveFocus()

      // Press Enter
      await user.keyboard('{Enter}')

      // Should trigger scroll to features
      expect(Element.prototype.scrollIntoView).toHaveBeenCalled()
    })

    it('pressing Space on buttons activates them', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const navFeatures = screen.getByTestId('nav-features')

      // Focus the Features button
      navFeatures.focus()
      expect(navFeatures).toHaveFocus()

      // Press Space
      await user.keyboard(' ')

      // Should trigger scroll to features section
      expect(Element.prototype.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' })
    })

    it('Enter key works on demo shorten button', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const demoInput = screen.getByTestId('demo-input')
      const demoShortenButton = screen.getByTestId('demo-shorten-button')

      // Type a URL in the input
      await user.type(demoInput, 'https://example.com')

      // Focus the shorten button
      demoShortenButton.focus()
      expect(demoShortenButton).toHaveFocus()

      // Press Enter
      await user.keyboard('{Enter}')

      // Should show preview (after loading)
      await waitFor(() => {
        expect(screen.getByTestId('demo-preview')).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 4: Check tab order', () => {
    it('tab order follows logical visual flow (top to bottom, left to right)', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const focusOrder: Element[] = []
      let tabCount = 0
      const maxTabs = 40 // Safety limit to prevent infinite loop

      // Tab through all elements and record the order
      while (tabCount < maxTabs) {
        await user.tab()
        const activeElement = document.activeElement

        // Stop if we've cycled back to body or gone past all content
        if (!activeElement || activeElement === document.body) {
          break
        }

        // Check if we've already seen this element (completed cycle)
        if (focusOrder.includes(activeElement)) {
          break
        }

        focusOrder.push(activeElement)
        tabCount++
      }

      // Verify the key elements appear in correct order
      const navLogo = screen.getByTestId('nav-logo')
      const navFeatures = screen.getByTestId('nav-features')
      const navHowItWorks = screen.getByTestId('nav-how-it-works')
      const themeToggle = screen.getByTestId('theme-toggle')
      const navLogin = screen.getByTestId('nav-login')
      const navRegister = screen.getByTestId('nav-register')
      const heroCTAPrimary = screen.getByTestId('hero-cta-primary')
      const heroCTASecondary = screen.getByTestId('hero-cta-secondary')

      // Navigation should come first (top of page)
      const navLogoIndex = focusOrder.indexOf(navLogo)
      const navFeaturesIndex = focusOrder.indexOf(navFeatures)
      const navHowItWorksIndex = focusOrder.indexOf(navHowItWorks)
      const themeToggleIndex = focusOrder.indexOf(themeToggle)
      const navLoginIndex = focusOrder.indexOf(navLogin)
      const navRegisterIndex = focusOrder.indexOf(navRegister)
      const heroCTAPrimaryIndex = focusOrder.indexOf(heroCTAPrimary)
      const heroCTASecondaryIndex = focusOrder.indexOf(heroCTASecondary)

      // Verify navigation elements come before hero CTAs
      expect(navLogoIndex).toBeLessThan(heroCTAPrimaryIndex)
      expect(navFeaturesIndex).toBeLessThan(heroCTAPrimaryIndex)
      expect(navHowItWorksIndex).toBeLessThan(heroCTAPrimaryIndex)
      expect(themeToggleIndex).toBeLessThan(heroCTAPrimaryIndex)
      expect(navLoginIndex).toBeLessThan(heroCTAPrimaryIndex)
      expect(navRegisterIndex).toBeLessThan(heroCTAPrimaryIndex)

      // Verify left-to-right order in navigation
      expect(navLogoIndex).toBeLessThan(navFeaturesIndex)
      expect(navFeaturesIndex).toBeLessThan(navHowItWorksIndex)
      expect(navHowItWorksIndex).toBeLessThan(themeToggleIndex)
      expect(themeToggleIndex).toBeLessThan(navLoginIndex)
      expect(navLoginIndex).toBeLessThan(navRegisterIndex)

      // Verify hero primary CTA comes before secondary
      expect(heroCTAPrimaryIndex).toBeLessThan(heroCTASecondaryIndex)
    })

    it('no elements are skipped during tab navigation', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const importantElements = [
        screen.getByTestId('nav-logo'),
        screen.getByTestId('nav-features'),
        screen.getByTestId('nav-how-it-works'),
        screen.getByTestId('theme-toggle'),
        screen.getByTestId('nav-login'),
        screen.getByTestId('nav-register'),
        screen.getByTestId('hero-cta-primary'),
        screen.getByTestId('hero-cta-secondary'),
        screen.getByTestId('demo-input'),
        screen.getByTestId('demo-shorten-button'),
      ]

      const reachedElements = new Set<Element>()
      let tabCount = 0
      const maxTabs = 50

      // Tab through all elements
      while (tabCount < maxTabs) {
        await user.tab()
        const activeElement = document.activeElement

        if (!activeElement || activeElement === document.body) {
          break
        }

        if (reachedElements.has(activeElement)) {
          break
        }

        reachedElements.add(activeElement)
        tabCount++
      }

      // Verify all important elements were reached
      for (const element of importantElements) {
        expect(reachedElements.has(element)).toBe(true)
      }
    })
  })

  describe('Additional Keyboard Accessibility Tests', () => {
    it('Escape key does not break navigation', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const navLogo = screen.getByTestId('nav-logo')

      // Tab to an element
      await user.tab()
      expect(navLogo).toHaveFocus()

      // Press Escape
      await user.keyboard('{Escape}')

      // Should still be able to continue tabbing
      await user.tab()
      const navFeatures = screen.getByTestId('nav-features')
      expect(navFeatures).toHaveFocus()
    })

    it('shift+tab navigates in reverse order', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      // Tab forward a few times
      await user.tab() // nav-logo
      await user.tab() // nav-features
      await user.tab() // nav-how-it-works

      const navHowItWorks = screen.getByTestId('nav-how-it-works')
      expect(navHowItWorks).toHaveFocus()

      // Shift+Tab to go back
      await user.tab({ shift: true })
      const navFeatures = screen.getByTestId('nav-features')
      expect(navFeatures).toHaveFocus()

      await user.tab({ shift: true })
      const navLogo = screen.getByTestId('nav-logo')
      expect(navLogo).toHaveFocus()
    })

    it('demo input accepts keyboard input', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const demoInput = screen.getByTestId('demo-input')

      // Focus the input
      demoInput.focus()

      // Type a URL
      await user.type(demoInput, 'https://test.com')

      expect(demoInput).toHaveValue('https://test.com')
    })

    it('Enter key in demo input triggers shorten', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const demoInput = screen.getByTestId('demo-input')

      // Focus and type URL
      await user.type(demoInput, 'https://example.com')

      // Press Enter while in input
      await user.keyboard('{Enter}')

      // Should trigger the shorten action and show preview
      await waitFor(() => {
        expect(screen.getByTestId('demo-preview')).toBeInTheDocument()
      })
    })

    it('footer links are keyboard accessible', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const footerLinks = screen.getByTestId('footer-links')
      const links = footerLinks.querySelectorAll('a')

      // Verify footer links exist and are proper anchor elements
      expect(links.length).toBeGreaterThan(0)

      // Each link should be focusable
      for (const link of links) {
        link.focus()
        expect(link).toHaveFocus()
      }
    })
  })
})
