/**
 * Homepage Theme Integration Tests
 * Owner: Scenario 7 - Theme Integration
 *
 * Tests for verifying theme toggle functionality and theme persistence
 * on the homepage (REQ-7, US-6)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from 'react-query';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { useThemeStore } from '../../../src/store/uiStore';
import Home from '../../../src/pages/Home';
import Login from '../../../src/pages/Login';

// Mock the API module
vi.mock('../../../src/api', () => ({
  getCurrentUser: vi.fn().mockRejectedValue(new Error('Not authenticated')),
  login: vi.fn(),
  logout: vi.fn(),
  register: vi.fn(),
  default: {
    interceptors: {
      request: { use: vi.fn() },
      response: { use: vi.fn() },
    },
  },
}));

// Test wrapper with all providers
interface TestWrapperProps {
  children: React.ReactNode;
  initialEntries?: string[];
}

const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });

const TestWrapper = ({ children, initialEntries = ['/'] }: TestWrapperProps) => {
  const testQueryClient = createTestQueryClient();

  return (
    <QueryClientProvider client={testQueryClient}>
      <ThemeProvider>
        <AuthProvider>
          <MemoryRouter initialEntries={initialEntries}>
            {children}
          </MemoryRouter>
        </AuthProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
};

// App with routes for testing navigation
const AppWithRoutes = () => (
  <Routes>
    <Route path="/" element={<Home />} />
    <Route path="/login" element={<Login />} />
  </Routes>
);

// Helper function to get the theme toggle element and click to open dropdown
const openThemeDropdown = async (user: ReturnType<typeof userEvent.setup>) => {
  const themeToggle = screen.getAllByTestId('theme-toggle')[0];
  // The label is the clickable trigger (has tabIndex=0, btn class)
  // We need to target it specifically, not the buttons in the dropdown
  const toggleLabel = themeToggle.querySelector('label');
  if (!toggleLabel) throw new Error('Could not find theme toggle label');
  await user.click(toggleLabel);
  return themeToggle;
};

// Helper to select a theme from the dropdown
const selectTheme = async (user: ReturnType<typeof userEvent.setup>, themeName: string) => {
  const themeToggle = await openThemeDropdown(user);
  // Get the button within the first theme toggle's dropdown list
  const dropdown = within(themeToggle).getByRole('list');
  const themeButton = within(dropdown).getByText(themeName);
  await user.click(themeButton);
};

describe('Theme Integration - Homepage renders with theme', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    // Reset theme store to default
    useThemeStore.setState({ theme: 'dark' });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.clear();
  });

  // Test Case 1: Homepage renders with current theme applied
  it('renders homepage with current theme applied', async () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Wait for the homepage to render
    await waitFor(() => {
      expect(screen.getByText(/shorten urls\. track every click/i)).toBeInTheDocument();
    });

    // Verify theme is applied to document
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
  });

  // Test Case 1 (variant): Homepage renders with light theme when set
  it('renders homepage with light theme when light theme is set', async () => {
    // Set light theme before rendering
    useThemeStore.setState({ theme: 'light' });

    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    await waitFor(() => {
      expect(screen.getByText(/shorten urls\. track every click/i)).toBeInTheDocument();
    });

    expect(document.documentElement.getAttribute('data-theme')).toBe('light');
  });
});

describe('Theme Integration - ThemeToggle component', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    useThemeStore.setState({ theme: 'dark' });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.clear();
  });

  // Test Case 2: ThemeToggle component is rendered and clickable
  it('renders ThemeToggle component that is visible and clickable', async () => {
    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    await waitFor(() => {
      // ThemeToggle should be present (desktop and mobile versions in navbar)
      const themeToggles = screen.getAllByTestId('theme-toggle');
      expect(themeToggles.length).toBeGreaterThan(0);
    });

    // Get the first theme toggle (desktop version)
    const themeToggle = screen.getAllByTestId('theme-toggle')[0];
    expect(themeToggle).toBeInTheDocument();

    // Should have a clickable label element (the trigger button)
    const toggleLabel = themeToggle.querySelector('label');
    expect(toggleLabel).toBeInTheDocument();
    // Should have the btn class making it clickable
    expect(toggleLabel).toHaveClass('btn');
  });

  // Test Case 2 (variant): ThemeToggle dropdown shows available themes
  it('shows theme options when ThemeToggle is clicked', async () => {
    const user = userEvent.setup();

    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    await waitFor(() => {
      expect(screen.getAllByTestId('theme-toggle')[0]).toBeInTheDocument();
    });

    // Click the theme toggle button to open dropdown
    await openThemeDropdown(user);

    // Get the dropdown list and verify theme options are visible
    const themeToggle = screen.getAllByTestId('theme-toggle')[0];
    const dropdown = within(themeToggle).getByRole('list');

    expect(within(dropdown).getByText('Light')).toBeInTheDocument();
    expect(within(dropdown).getByText('Dark')).toBeInTheDocument();
    expect(within(dropdown).getByText('Cyberpunk')).toBeInTheDocument();
  });
});

describe('Theme Integration - Theme context changes', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    useThemeStore.setState({ theme: 'light' });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.clear();
  });

  // Test Case 3: Click ThemeToggle to change theme updates context
  it('changes theme context value when ThemeToggle option is clicked', async () => {
    const user = userEvent.setup();

    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Verify initial theme
    await waitFor(() => {
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    // Select dark theme
    await selectTheme(user, 'Dark');

    // Verify theme changed
    await waitFor(() => {
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    // Verify zustand store updated
    expect(useThemeStore.getState().theme).toBe('dark');
  });
});

describe('Theme Integration - Homepage elements respond to theme', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    useThemeStore.setState({ theme: 'light' });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.clear();
  });

  // Test Case 4: Homepage elements respond to theme change
  it('updates homepage elements when theme changes', async () => {
    const user = userEvent.setup();

    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    // Verify homepage is rendered
    await waitFor(() => {
      expect(screen.getByText(/shorten urls\. track every click/i)).toBeInTheDocument();
    });

    // Initial theme should be light
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');

    // Select dark theme
    await selectTheme(user, 'Dark');

    // Verify data-theme attribute changed (which controls DaisyUI theming)
    await waitFor(() => {
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    // The base classes (bg-base-100, text-base-content, etc.) automatically
    // adapt their colors based on the data-theme attribute
    // Homepage elements should now be themed differently
    // There may be multiple navigation elements (navbar and footer nav)
    const navElements = screen.getAllByRole('navigation');
    expect(navElements.length).toBeGreaterThanOrEqual(1);
  });
});

describe('Theme Integration - Theme persistence across navigation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    useThemeStore.setState({ theme: 'light' });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.clear();
  });

  // Test Case 5: Theme persists when navigating away and back
  it('persists theme selection when navigating to login and returning to homepage', async () => {
    const user = userEvent.setup();

    render(
      <TestWrapper initialEntries={['/']}>
        <AppWithRoutes />
      </TestWrapper>
    );

    // Verify initial homepage render
    await waitFor(() => {
      expect(screen.getByText(/shorten urls\. track every click/i)).toBeInTheDocument();
    });

    // Initial theme is light
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');

    // Select cyberpunk theme
    await selectTheme(user, 'Cyberpunk');

    // Verify theme changed
    await waitFor(() => {
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });

    // Verify localStorage was updated
    expect(localStorage.getItem('theme')).toBe('cyberpunk');

    // Navigate to login page
    const loginLink = screen.getByTestId('login-link');
    await user.click(loginLink);

    // Verify we're on login page
    await waitFor(() => {
      expect(screen.getByText(/sign in to your account/i)).toBeInTheDocument();
    });

    // Theme should still be cyberpunk on login page
    expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');

    // Navigate back to homepage using logo
    const logoLink = screen.getByRole('link', { name: /url shortener/i });
    await user.click(logoLink);

    // Verify we're back on homepage
    await waitFor(() => {
      expect(screen.getByText(/shorten urls\. track every click/i)).toBeInTheDocument();
    });

    // Theme should still be cyberpunk
    expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    expect(useThemeStore.getState().theme).toBe('cyberpunk');
  });

  // Test Case 5 (variant): Theme loaded from localStorage on page load
  it('loads theme from localStorage on initial render', async () => {
    // Pre-set theme in localStorage before rendering
    localStorage.setItem('theme', 'synthwave');
    useThemeStore.setState({ theme: 'synthwave' });

    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    await waitFor(() => {
      expect(screen.getByText(/shorten urls\. track every click/i)).toBeInTheDocument();
    });

    // Theme should be synthwave from localStorage
    expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave');
  });
});

describe('Theme Integration - Dark theme on homepage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.clear();
  });

  // Test Case 6: Dark theme colors applied correctly
  it('applies dark theme colors correctly on homepage', async () => {
    // Set dark theme
    localStorage.setItem('theme', 'dark');
    useThemeStore.setState({ theme: 'dark' });

    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    await waitFor(() => {
      expect(screen.getByText(/shorten urls\. track every click/i)).toBeInTheDocument();
    });

    // Verify dark theme is applied
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

    // Verify all main homepage sections are present and themed
    const heroHeadline = screen.getByText(/shorten urls\. track every click/i);
    expect(heroHeadline).toBeInTheDocument();

    // There may be multiple navigation elements (navbar and footer nav)
    const navElements = screen.getAllByRole('navigation');
    expect(navElements.length).toBeGreaterThanOrEqual(1);

    // Footer should be present
    expect(screen.getByText(/2026 URL Shortener/i)).toBeInTheDocument();
  });

  // Test Case 6 (variant): Can switch to dark theme from other theme
  it('can switch from light to dark theme and apply correctly', async () => {
    const user = userEvent.setup();
    useThemeStore.setState({ theme: 'light' });

    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    await waitFor(() => {
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    // Select dark theme
    await selectTheme(user, 'Dark');

    // Verify dark theme is now applied
    await waitFor(() => {
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    // Store should reflect the change
    expect(useThemeStore.getState().theme).toBe('dark');
    expect(localStorage.getItem('theme')).toBe('dark');
  });
});

describe('Theme Integration - Cyberpunk theme on homepage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.clear();
  });

  // Test Case 7: Cyberpunk theme colors applied correctly
  it('applies cyberpunk theme colors correctly on homepage', async () => {
    // Set cyberpunk theme
    localStorage.setItem('theme', 'cyberpunk');
    useThemeStore.setState({ theme: 'cyberpunk' });

    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    await waitFor(() => {
      expect(screen.getByText(/shorten urls\. track every click/i)).toBeInTheDocument();
    });

    // Verify cyberpunk theme is applied
    expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');

    // Verify all main homepage sections are present and themed
    const heroHeadline = screen.getByText(/shorten urls\. track every click/i);
    expect(heroHeadline).toBeInTheDocument();

    // There may be multiple navigation elements (navbar and footer nav)
    const navElements = screen.getAllByRole('navigation');
    expect(navElements.length).toBeGreaterThanOrEqual(1);
  });

  // Test Case 7 (variant): Can switch to cyberpunk theme
  it('can switch to cyberpunk theme from light theme', async () => {
    const user = userEvent.setup();
    useThemeStore.setState({ theme: 'light' });

    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    await waitFor(() => {
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    // Select cyberpunk theme
    await selectTheme(user, 'Cyberpunk');

    // Verify cyberpunk theme is applied
    await waitFor(() => {
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });

    expect(useThemeStore.getState().theme).toBe('cyberpunk');
    expect(localStorage.getItem('theme')).toBe('cyberpunk');
  });

  // Test Case 7 (extra): Cyberpunk theme persists after navigation
  it('cyberpunk theme persists after navigating to login and back', async () => {
    const user = userEvent.setup();
    localStorage.setItem('theme', 'cyberpunk');
    useThemeStore.setState({ theme: 'cyberpunk' });

    render(
      <TestWrapper initialEntries={['/']}>
        <AppWithRoutes />
      </TestWrapper>
    );

    // Verify initial state
    await waitFor(() => {
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });

    // Navigate to login
    const loginLink = screen.getByTestId('login-link');
    await user.click(loginLink);

    await waitFor(() => {
      expect(screen.getByText(/sign in to your account/i)).toBeInTheDocument();
    });

    // Theme should still be cyberpunk
    expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');

    // Navigate back to homepage
    const logoLink = screen.getByRole('link', { name: /url shortener/i });
    await user.click(logoLink);

    await waitFor(() => {
      expect(screen.getByText(/shorten urls\. track every click/i)).toBeInTheDocument();
    });

    // Theme should still be cyberpunk
    expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
  });
});

describe('Theme Integration - Additional theme tests', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    useThemeStore.setState({ theme: 'light' });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.clear();
  });

  // Test all available themes can be selected
  it.each([
    ['Synthwave', 'synthwave'],
    ['Retro', 'retro'],
    ['Valentine', 'valentine'],
    ['Night', 'night'],
  ])('can switch to %s theme', async (displayName, themeValue) => {
    const user = userEvent.setup();

    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    await waitFor(() => {
      expect(screen.getByText(/shorten urls\. track every click/i)).toBeInTheDocument();
    });

    // Select the theme
    await selectTheme(user, displayName);

    // Verify theme is applied
    await waitFor(() => {
      expect(document.documentElement.getAttribute('data-theme')).toBe(themeValue);
    });

    expect(useThemeStore.getState().theme).toBe(themeValue);
    expect(localStorage.getItem('theme')).toBe(themeValue);
  });

  // Test rapid theme switching
  it('handles rapid theme switching correctly', async () => {
    const user = userEvent.setup();

    render(
      <TestWrapper>
        <Home />
      </TestWrapper>
    );

    await waitFor(() => {
      expect(screen.getByText(/shorten urls\. track every click/i)).toBeInTheDocument();
    });

    // Switch to dark
    await selectTheme(user, 'Dark');
    await waitFor(() => expect(document.documentElement.getAttribute('data-theme')).toBe('dark'));

    // Switch to cyberpunk
    await selectTheme(user, 'Cyberpunk');
    await waitFor(() => expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk'));

    // Switch to light
    await selectTheme(user, 'Light');
    await waitFor(() => expect(document.documentElement.getAttribute('data-theme')).toBe('light'));

    // Final state should be light
    expect(useThemeStore.getState().theme).toBe('light');
    expect(localStorage.getItem('theme')).toBe('light');
  });
});
