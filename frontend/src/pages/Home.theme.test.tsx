import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter } from 'react-router-dom';
import Home from './Home';
import { ThemeProvider } from '../contexts/ThemeContext';
import { useThemeStore, AVAILABLE_THEMES, Theme } from '../store/themeStore';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

// Helper to render Home with ThemeProvider
const renderHomeWithTheme = (initialTheme: Theme = 'light') => {
  // Set initial theme in store
  useThemeStore.setState({ theme: initialTheme });

  return render(
    <BrowserRouter>
      <ThemeProvider>
        <Home />
      </ThemeProvider>
    </BrowserRouter>
  );
};

// Test Case 1: Render Home component with light theme context
describe('Home - Theme Support - Test Case 1: Light Theme Rendering', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it('renders homepage with light theme styles applied', () => {
    renderHomeWithTheme('light');

    // Verify the theme store has light theme
    const { theme } = useThemeStore.getState();
    expect(theme).toBe('light');

    // Verify homepage renders correctly
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
  });

  it('homepage elements are visible in light theme', () => {
    renderHomeWithTheme('light');

    // Verify key elements render
    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('Shorten, Share, Track');
    expect(screen.getByRole('button', { name: /Get Started Free/i })).toBeInTheDocument();
    expect(screen.getByTestId('features-grid')).toBeInTheDocument();
  });

  it('feature cards display correctly in light theme', () => {
    renderHomeWithTheme('light');

    const featureCards = screen.getAllByTestId(/^feature-card-/);
    expect(featureCards.length).toBe(4);

    featureCards.forEach((card) => {
      expect(card).toBeInTheDocument();
    });
  });
});

// Test Case 2: Render Home component with dark theme context
describe('Home - Theme Support - Test Case 2: Dark Theme Rendering', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it('renders homepage with dark theme styles applied', () => {
    renderHomeWithTheme('dark');

    // Verify the theme store has dark theme
    const { theme } = useThemeStore.getState();
    expect(theme).toBe('dark');

    // Verify homepage renders correctly
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
  });

  it('homepage elements are visible in dark theme', () => {
    renderHomeWithTheme('dark');

    // Verify key elements render
    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('Shorten, Share, Track');
    expect(screen.getByRole('button', { name: /Get Started Free/i })).toBeInTheDocument();
  });

  it('footer is visible in dark theme', () => {
    renderHomeWithTheme('dark');

    const footer = screen.getByTestId('homepage-footer');
    expect(footer).toBeInTheDocument();
    expect(footer).toBeVisible();
  });

  it('theme toggle is accessible in dark theme', () => {
    renderHomeWithTheme('dark');

    const themeToggle = screen.getByTestId('theme-toggle');
    expect(themeToggle).toBeInTheDocument();
  });
});

// Test Case 3: Toggle theme from light to dark
describe('Home - Theme Support - Test Case 3: Theme Toggle Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it('all homepage elements transition to dark mode colors when toggled', async () => {
    const user = userEvent.setup();
    renderHomeWithTheme('light');

    // Initial theme should be light
    expect(useThemeStore.getState().theme).toBe('light');

    // Find and click theme toggle to open dropdown
    const themeToggle = screen.getByTestId('theme-toggle');
    const toggleButton = within(themeToggle).getByRole('button');
    await user.click(toggleButton);

    // Click on dark theme option
    const darkOption = screen.getByTestId('theme-option-dark');
    await user.click(darkOption);

    // Verify theme changed to dark
    await waitFor(() => {
      expect(useThemeStore.getState().theme).toBe('dark');
    });
  });

  it('theme change persists in localStorage', async () => {
    const user = userEvent.setup();
    renderHomeWithTheme('light');

    // Open theme dropdown and select dark
    const themeToggle = screen.getByTestId('theme-toggle');
    const toggleButton = within(themeToggle).getByRole('button');
    await user.click(toggleButton);

    const darkOption = screen.getByTestId('theme-option-dark');
    await user.click(darkOption);

    // Verify localStorage is updated
    await waitFor(() => {
      expect(localStorage.getItem('theme')).toBe('dark');
    });
  });

  it('homepage remains functional after theme toggle', async () => {
    const user = userEvent.setup();
    renderHomeWithTheme('light');

    // Toggle to dark theme
    const themeToggle = screen.getByTestId('theme-toggle');
    const toggleButton = within(themeToggle).getByRole('button');
    await user.click(toggleButton);

    const darkOption = screen.getByTestId('theme-option-dark');
    await user.click(darkOption);

    // Verify homepage elements still work
    await waitFor(() => {
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('Shorten, Share, Track');
    });

    const getStartedButton = screen.getByRole('button', { name: /Get Started Free/i });
    expect(getStartedButton).toBeInTheDocument();
    expect(getStartedButton).not.toBeDisabled();
  });
});

// Test Case 4: Apply cyberpunk theme
describe('Home - Theme Support - Test Case 4: Cyberpunk Theme', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it('homepage adopts cyberpunk theme colors and styles', async () => {
    const user = userEvent.setup();
    renderHomeWithTheme('light');

    // Open theme dropdown and select cyberpunk
    const themeToggle = screen.getByTestId('theme-toggle');
    const toggleButton = within(themeToggle).getByRole('button');
    await user.click(toggleButton);

    const cyberpunkOption = screen.getByTestId('theme-option-cyberpunk');
    await user.click(cyberpunkOption);

    // Verify theme changed to cyberpunk
    await waitFor(() => {
      expect(useThemeStore.getState().theme).toBe('cyberpunk');
    });
  });

  it('renders correctly with cyberpunk theme applied directly', () => {
    renderHomeWithTheme('cyberpunk');

    // Verify the theme store has cyberpunk theme
    expect(useThemeStore.getState().theme).toBe('cyberpunk');

    // Verify homepage renders correctly
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
    expect(screen.getByTestId('homepage-footer')).toBeInTheDocument();
  });

  it('all feature cards render in cyberpunk theme', () => {
    renderHomeWithTheme('cyberpunk');

    const featureCards = screen.getAllByTestId(/^feature-card-/);
    expect(featureCards.length).toBe(4);

    featureCards.forEach((card) => {
      expect(card).toBeInTheDocument();
    });
  });

  it('navigation links work in cyberpunk theme', () => {
    renderHomeWithTheme('cyberpunk');

    const loginLink = screen.getByLabelText(/go to login page/i);
    expect(loginLink).toBeInTheDocument();
    expect(loginLink).toHaveAttribute('href', '/login');

    const signUpLink = screen.getByLabelText(/go to registration page/i);
    expect(signUpLink).toBeInTheDocument();
    expect(signUpLink).toHaveAttribute('href', '/register');
  });
});

// Test Case 5: ThemeToggle is accessible and functional on homepage
describe('Home - Theme Support - Test Case 5: ThemeToggle Accessibility', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it('ThemeToggle component is present on homepage', () => {
    renderHomeWithTheme('light');

    const themeToggle = screen.getByTestId('theme-toggle');
    expect(themeToggle).toBeInTheDocument();
  });

  it('ThemeToggle is in the footer section', () => {
    renderHomeWithTheme('light');

    const footer = screen.getByTestId('homepage-footer');
    const themeToggle = within(footer).getByTestId('theme-toggle');
    expect(themeToggle).toBeInTheDocument();
  });

  it('ThemeToggle has accessible aria-label', () => {
    renderHomeWithTheme('light');

    const toggleButton = screen.getByLabelText(/change theme/i);
    expect(toggleButton).toBeInTheDocument();
  });

  it('ThemeToggle dropdown shows all available themes', async () => {
    const user = userEvent.setup();
    renderHomeWithTheme('light');

    // Open dropdown
    const themeToggle = screen.getByTestId('theme-toggle');
    const toggleButton = within(themeToggle).getByRole('button');
    await user.click(toggleButton);

    // Check all theme options are present
    for (const theme of AVAILABLE_THEMES) {
      const option = screen.getByTestId(`theme-option-${theme}`);
      expect(option).toBeInTheDocument();
    }
  });

  it('ThemeToggle options have proper aria attributes', async () => {
    const user = userEvent.setup();
    renderHomeWithTheme('light');

    // Open dropdown
    const toggleButton = screen.getByLabelText(/change theme/i);
    await user.click(toggleButton);

    // Check for role="option" on theme options
    const lightOption = screen.getByTestId('theme-option-light');
    expect(lightOption).toHaveAttribute('role', 'option');
    expect(lightOption).toHaveAttribute('aria-selected', 'true');

    const darkOption = screen.getByTestId('theme-option-dark');
    expect(darkOption).toHaveAttribute('role', 'option');
    expect(darkOption).toHaveAttribute('aria-selected', 'false');
  });

  it('active theme is indicated in the dropdown', async () => {
    const user = userEvent.setup();
    renderHomeWithTheme('dark');

    // Open dropdown
    const toggleButton = screen.getByLabelText(/change theme/i);
    await user.click(toggleButton);

    // Dark option should be marked as selected
    const darkOption = screen.getByTestId('theme-option-dark');
    expect(darkOption).toHaveAttribute('aria-selected', 'true');

    // Should contain "Active" badge
    expect(within(darkOption).getByText('Active')).toBeInTheDocument();
  });
});

// Additional theme tests for other DaisyUI themes
describe('Home - Theme Support - Additional Themes', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it.each([
    ['synthwave'],
    ['retro'],
    ['valentine'],
    ['night'],
  ])('homepage renders correctly with %s theme', (themeName) => {
    renderHomeWithTheme(themeName as Theme);

    expect(useThemeStore.getState().theme).toBe(themeName);
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
    expect(screen.getByTestId('homepage-footer')).toBeInTheDocument();
  });

  it('can switch between multiple themes sequentially', async () => {
    const user = userEvent.setup();
    renderHomeWithTheme('light');

    const themes: Theme[] = ['dark', 'cyberpunk', 'synthwave', 'light'];

    for (const theme of themes) {
      // Open dropdown
      const toggleButton = screen.getByLabelText(/change theme/i);
      await user.click(toggleButton);

      // Select theme
      const option = screen.getByTestId(`theme-option-${theme}`);
      await user.click(option);

      // Verify change
      await waitFor(() => {
        expect(useThemeStore.getState().theme).toBe(theme);
      });
    }
  });
});
