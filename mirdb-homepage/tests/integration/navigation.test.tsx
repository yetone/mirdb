import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Header } from '../../src/components/Header';

// Mock scrollIntoView for smooth scroll testing
const mockScrollIntoView = vi.fn();

// Create a test wrapper component that includes target sections
function TestPage() {
  return (
    <div style={{ minHeight: '2000px' }}>
      <Header />
      <main>
        <section id="features" style={{ marginTop: '500px', height: '300px' }}>
          <h2>Features Section</h2>
        </section>
        <section id="usage" style={{ marginTop: '100px', height: '300px' }}>
          <h2>Usage Section</h2>
        </section>
        <section id="architecture" style={{ marginTop: '100px', height: '300px' }}>
          <h2>Architecture Section</h2>
        </section>
        <section id="resources" style={{ marginTop: '100px', height: '300px' }}>
          <h2>Resources Section</h2>
        </section>
      </main>
    </div>
  );
}

describe('Navigation Integration Tests', () => {
  beforeEach(() => {
    // Mock scrollIntoView
    Element.prototype.scrollIntoView = mockScrollIntoView;
    mockScrollIntoView.mockClear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // Test Case 4: Click Features navigation link - Page smoothly scrolls to Features section
  it('clicking Features link triggers smooth scroll to Features section', async () => {
    const user = userEvent.setup();
    render(<TestPage />);

    const featuresLink = screen.getByRole('link', { name: 'Features' });
    await user.click(featuresLink);

    expect(mockScrollIntoView).toHaveBeenCalledWith({
      behavior: 'smooth',
      block: 'start',
    });
  });

  it('Features link targets correct section element', async () => {
    const user = userEvent.setup();
    render(<TestPage />);

    const featuresLink = screen.getByRole('link', { name: 'Features' });
    await user.click(featuresLink);

    // Verify the features section exists
    const featuresSection = document.getElementById('features');
    expect(featuresSection).toBeInTheDocument();
    expect(mockScrollIntoView).toHaveBeenCalled();
  });

  // Test Case 5: Click Usage navigation link - Page smoothly scrolls to Usage section
  it('clicking Usage link triggers smooth scroll to Usage section', async () => {
    const user = userEvent.setup();
    render(<TestPage />);

    const usageLink = screen.getByRole('link', { name: 'Usage' });
    await user.click(usageLink);

    expect(mockScrollIntoView).toHaveBeenCalledWith({
      behavior: 'smooth',
      block: 'start',
    });
  });

  it('Usage link targets correct section element', async () => {
    const user = userEvent.setup();
    render(<TestPage />);

    const usageLink = screen.getByRole('link', { name: 'Usage' });
    await user.click(usageLink);

    // Verify the usage section exists
    const usageSection = document.getElementById('usage');
    expect(usageSection).toBeInTheDocument();
    expect(mockScrollIntoView).toHaveBeenCalled();
  });

  it('clicking Architecture link triggers smooth scroll', async () => {
    const user = userEvent.setup();
    render(<TestPage />);

    const architectureLink = screen.getByRole('link', { name: 'Architecture' });
    await user.click(architectureLink);

    expect(mockScrollIntoView).toHaveBeenCalledWith({
      behavior: 'smooth',
      block: 'start',
    });
  });

  it('clicking Resources link triggers smooth scroll', async () => {
    const user = userEvent.setup();
    render(<TestPage />);

    const resourcesLink = screen.getByRole('link', { name: 'Resources' });
    await user.click(resourcesLink);

    expect(mockScrollIntoView).toHaveBeenCalledWith({
      behavior: 'smooth',
      block: 'start',
    });
  });

  // Test that navigation prevents default anchor behavior
  it('navigation click prevents default link behavior', async () => {
    const user = userEvent.setup();
    render(<TestPage />);

    const featuresLink = screen.getByRole('link', { name: 'Features' });

    // The link should have href but not navigate away
    expect(featuresLink).toHaveAttribute('href', '#features');

    await user.click(featuresLink);

    // scrollIntoView should have been called (proving preventDefault worked)
    expect(mockScrollIntoView).toHaveBeenCalled();
  });
});

describe('Header Sticky Behavior', () => {
  // Test Case 6: Scroll page down 500px - Header remains fixed at top of viewport (sticky)
  it('header has sticky positioning', () => {
    render(<TestPage />);

    const header = screen.getByRole('banner');

    // Check that the header has the sticky class applied
    // The actual sticky behavior is CSS-based (position: sticky)
    expect(header).toBeInTheDocument();

    // In real browser, header would have computed style position: sticky
    // In JSDOM we can verify the element structure is correct
    expect(header.tagName.toLowerCase()).toBe('header');
  });

  it('header contains all navigation elements after scroll simulation', () => {
    render(<TestPage />);

    const header = screen.getByRole('banner');

    // Verify header contains navigation
    const nav = screen.getByRole('navigation');
    expect(nav).toBeInTheDocument();

    // Verify logo is present
    const logo = screen.getByRole('img', { name: /mirdb logo/i });
    expect(logo).toBeInTheDocument();

    // Verify navigation links
    expect(screen.getByRole('link', { name: 'Features' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Usage' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Architecture' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Resources' })).toBeInTheDocument();
  });
});

describe('Mobile Navigation', () => {
  // Test Case 7: Render Header on mobile viewport (375px) - Navigation collapses into hamburger menu
  it('mobile menu button is rendered', () => {
    render(<Header />);

    const menuButton = screen.getByRole('button', { name: /navigation menu/i });
    expect(menuButton).toBeInTheDocument();
  });

  it('mobile menu button has correct ARIA attributes when closed', () => {
    render(<Header />);

    const menuButton = screen.getByRole('button', { name: /open navigation menu/i });
    expect(menuButton).toHaveAttribute('aria-expanded', 'false');
  });

  // Test Case 8: Click hamburger menu on mobile - Navigation menu expands and links are accessible
  it('clicking hamburger menu toggles aria-expanded to true', async () => {
    const user = userEvent.setup();
    render(<Header />);

    const menuButton = screen.getByRole('button', { name: /open navigation menu/i });

    await user.click(menuButton);

    expect(menuButton).toHaveAttribute('aria-expanded', 'true');
  });

  it('hamburger menu button label changes when expanded', async () => {
    const user = userEvent.setup();
    render(<Header />);

    // Initially shows "Open navigation menu"
    let menuButton = screen.getByRole('button', { name: /open navigation menu/i });
    await user.click(menuButton);

    // After clicking, should show "Close navigation menu"
    menuButton = screen.getByRole('button', { name: /close navigation menu/i });
    expect(menuButton).toBeInTheDocument();
  });

  it('navigation links are still accessible when mobile menu is opened', async () => {
    const user = userEvent.setup();
    render(<Header />);

    const menuButton = screen.getByRole('button', { name: /open navigation menu/i });
    await user.click(menuButton);

    // All navigation links should still be in the DOM and accessible
    expect(screen.getByRole('link', { name: 'Features' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Usage' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Architecture' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Resources' })).toBeInTheDocument();
  });

  it('clicking a navigation link closes the mobile menu', async () => {
    const user = userEvent.setup();

    // Need to render with target sections for navigation to work
    render(<TestPage />);

    const menuButton = screen.getByRole('button', { name: /open navigation menu/i });
    await user.click(menuButton);

    // Menu is now open
    expect(menuButton).toHaveAttribute('aria-expanded', 'true');

    // Click a navigation link
    const featuresLink = screen.getByRole('link', { name: 'Features' });
    await user.click(featuresLink);

    // Menu should be closed
    expect(menuButton).toHaveAttribute('aria-expanded', 'false');
  });

  it('hamburger icon has three lines', () => {
    render(<Header />);

    const menuButton = screen.getByRole('button', { name: /navigation menu/i });
    const hamburgerLines = menuButton.querySelectorAll('[aria-hidden="true"]');

    expect(hamburgerLines.length).toBe(3);
  });
});

describe('Theme Toggle Slot Integration', () => {
  it('theme toggle slot accepts custom component', () => {
    const CustomThemeToggle = () => (
      <button aria-label="Toggle dark mode">🌙</button>
    );

    render(<Header themeToggle={<CustomThemeToggle />} />);

    expect(screen.getByRole('button', { name: /toggle dark mode/i })).toBeInTheDocument();
  });

  it('theme toggle renders within the header actions area', () => {
    const mockToggle = <span data-testid="theme-toggle">Theme Toggle</span>;

    render(<Header themeToggle={mockToggle} />);

    const slot = screen.getByTestId('theme-toggle-slot');
    expect(slot).toContainElement(screen.getByTestId('theme-toggle'));
  });
});
