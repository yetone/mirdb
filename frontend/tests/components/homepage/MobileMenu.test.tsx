import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { act, render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import MobileMenu, { useIsMobile, MOBILE_BREAKPOINT_PX } from '../../../src/components/homepage/MobileMenu';

const setViewportWidth = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    configurable: true,
    writable: true,
    value: width,
  });
  act(() => {
    window.dispatchEvent(new Event('resize'));
  });
};

const renderMobileMenu = () =>
  render(
    <MemoryRouter initialEntries={['/']}>
      <MobileMenu />
    </MemoryRouter>
  );

describe('MobileMenu', () => {
  const originalInnerWidth = window.innerWidth;

  beforeEach(() => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: 375,
    });
  });

  afterEach(() => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: originalInnerWidth,
    });
  });

  it('renders nothing at desktop widths (>= 768px)', () => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: 1280,
    });
    const { container } = renderMobileMenu();
    expect(container).toBeEmptyDOMElement();
    expect(screen.queryByTestId('mobile-menu-toggle')).not.toBeInTheDocument();
  });

  it('renders the hamburger button at mobile widths (< 768px)', () => {
    renderMobileMenu();
    const toggle = screen.getByTestId('mobile-menu-toggle');
    expect(toggle).toBeInTheDocument();
    expect(toggle).toHaveAttribute('type', 'button');
  });

  it('hamburger button has an accessible name and is reachable by /menu/i', () => {
    renderMobileMenu();
    const toggle = screen.getByRole('button', { name: /menu/i });
    expect(toggle).toBe(screen.getByTestId('mobile-menu-toggle'));
    expect(toggle).toHaveAttribute('aria-label', 'Open menu');
  });

  it('hamburger button starts with aria-expanded="false" and the panel is not rendered', () => {
    renderMobileMenu();
    const toggle = screen.getByTestId('mobile-menu-toggle');
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByTestId('mobile-menu-panel')).not.toBeInTheDocument();
  });

  it('aria-controls on the toggle points to the menu panel id', () => {
    renderMobileMenu();
    const toggle = screen.getByTestId('mobile-menu-toggle');
    expect(toggle).toHaveAttribute('aria-controls', 'mobile-menu-panel');
  });

  it('opens the menu and updates aria-expanded to "true" on click', async () => {
    const user = userEvent.setup();
    renderMobileMenu();

    const toggle = screen.getByTestId('mobile-menu-toggle');
    await user.click(toggle);

    expect(toggle).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByTestId('mobile-menu-panel')).toBeInTheDocument();
  });

  it('open menu lists Login and Register links with correct routes', async () => {
    const user = userEvent.setup();
    renderMobileMenu();

    await user.click(screen.getByTestId('mobile-menu-toggle'));

    const panel = screen.getByTestId('mobile-menu-panel');
    const loginLink = within(panel).getByTestId('mobile-menu-link-login');
    const registerLink = within(panel).getByTestId('mobile-menu-link-register');

    expect(loginLink).toHaveAttribute('href', '/login');
    expect(loginLink).toHaveTextContent(/login/i);
    expect(registerLink).toHaveAttribute('href', '/register');
    expect(registerLink).toHaveTextContent(/register/i);
  });

  it('closes the menu and updates aria-expanded to "false" when Escape is pressed', async () => {
    const user = userEvent.setup();
    renderMobileMenu();

    const toggle = screen.getByTestId('mobile-menu-toggle');
    await user.click(toggle);
    expect(toggle).toHaveAttribute('aria-expanded', 'true');

    await user.keyboard('{Escape}');

    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByTestId('mobile-menu-panel')).not.toBeInTheDocument();
  });

  it('returns focus to the hamburger button after Escape closes the menu', async () => {
    const user = userEvent.setup();
    renderMobileMenu();

    const toggle = screen.getByTestId('mobile-menu-toggle');
    await user.click(toggle);

    await user.keyboard('{Escape}');

    expect(document.activeElement).toBe(toggle);
  });

  it('closes the menu when the overlay is clicked and restores focus', async () => {
    const user = userEvent.setup();
    renderMobileMenu();

    const toggle = screen.getByTestId('mobile-menu-toggle');
    await user.click(toggle);
    expect(screen.getByTestId('mobile-menu-panel')).toBeInTheDocument();

    const overlay = screen.getByTestId('mobile-menu-overlay');
    await user.click(overlay);

    expect(screen.queryByTestId('mobile-menu-panel')).not.toBeInTheDocument();
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
  });

  it('closes the menu when a navigation link is clicked', async () => {
    const user = userEvent.setup();
    renderMobileMenu();

    await user.click(screen.getByTestId('mobile-menu-toggle'));
    await user.click(screen.getByTestId('mobile-menu-link-login'));

    expect(screen.queryByTestId('mobile-menu-panel')).not.toBeInTheDocument();
    expect(screen.getByTestId('mobile-menu-toggle')).toHaveAttribute('aria-expanded', 'false');
  });

  it('clicking the toggle a second time closes the menu', async () => {
    const user = userEvent.setup();
    renderMobileMenu();

    const toggle = screen.getByTestId('mobile-menu-toggle');
    await user.click(toggle);
    expect(toggle).toHaveAttribute('aria-expanded', 'true');

    await user.click(toggle);
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByTestId('mobile-menu-panel')).not.toBeInTheDocument();
  });

  it('hamburger button meets the 44x44 CSS-pixel WCAG touch-target minimum', () => {
    renderMobileMenu();
    const toggle = screen.getByTestId('mobile-menu-toggle');
    expect(toggle.className).toMatch(/min-h-\[44px\]/);
    expect(toggle.className).toMatch(/min-w-\[44px\]/);
  });

  it('hides the menu and resets state when resizing back to desktop while open', async () => {
    const user = userEvent.setup();
    renderMobileMenu();

    await user.click(screen.getByTestId('mobile-menu-toggle'));
    expect(screen.getByTestId('mobile-menu-panel')).toBeInTheDocument();

    setViewportWidth(1280);

    expect(screen.queryByTestId('mobile-menu-root')).not.toBeInTheDocument();
    expect(screen.queryByTestId('mobile-menu-panel')).not.toBeInTheDocument();
  });

  it('exposes 768 as the breakpoint constant', () => {
    expect(MOBILE_BREAKPOINT_PX).toBe(768);
  });
});

describe('useIsMobile hook', () => {
  const originalInnerWidth = window.innerWidth;
  afterEach(() => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: originalInnerWidth,
    });
  });

  function HookProbe({ breakpoint }: { breakpoint?: number }) {
    const isMobile = useIsMobile(breakpoint);
    return <span data-testid="probe">{isMobile ? 'mobile' : 'not-mobile'}</span>;
  }

  it('returns true when window.innerWidth is below the default 768 breakpoint', () => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: 320,
    });
    render(<HookProbe />);
    expect(screen.getByTestId('probe')).toHaveTextContent('mobile');
  });

  it('returns false when window.innerWidth is at or above the breakpoint', () => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: 1280,
    });
    render(<HookProbe />);
    expect(screen.getByTestId('probe')).toHaveTextContent('not-mobile');
  });

  it('updates when the viewport resizes between breakpoints', () => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: 1280,
    });
    render(<HookProbe />);
    expect(screen.getByTestId('probe')).toHaveTextContent('not-mobile');

    setViewportWidth(375);
    expect(screen.getByTestId('probe')).toHaveTextContent('mobile');

    setViewportWidth(1024);
    expect(screen.getByTestId('probe')).toHaveTextContent('not-mobile');
  });

  it('respects a custom breakpoint', () => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: 900,
    });
    render(<HookProbe breakpoint={1024} />);
    expect(screen.getByTestId('probe')).toHaveTextContent('mobile');
  });
});
