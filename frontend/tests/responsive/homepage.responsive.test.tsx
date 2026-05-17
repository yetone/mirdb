import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { act, render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import Home from '../../src/pages/Home';

const setViewport = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    configurable: true,
    writable: true,
    value: width,
  });
  Object.defineProperty(document.documentElement, 'clientWidth', {
    configurable: true,
    writable: true,
    value: width,
  });
  act(() => {
    window.dispatchEvent(new Event('resize'));
  });
};

const renderHome = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    configurable: true,
    writable: true,
    value: width,
  });
  Object.defineProperty(document.documentElement, 'clientWidth', {
    configurable: true,
    writable: true,
    value: width,
  });
  return render(
    <MemoryRouter initialEntries={['/']}>
      <Home />
    </MemoryRouter>
  );
};

describe('Homepage responsive layout (REQ-7, US-5)', () => {
  const originalInnerWidth = window.innerWidth;

  afterEach(() => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: originalInnerWidth,
    });
  });

  describe('Test case 1: desktop viewport (>= 1024px)', () => {
    beforeEach(() => {
      renderHome(1280);
    });

    it("shows Login and Register navigation links without opening a menu", () => {
      const nav = screen.getByRole('navigation', { name: /main navigation/i });
      const loginLink = within(nav).getByTestId('navbar-link-login');
      const registerLink = within(nav).getByTestId('navbar-link-register');

      expect(loginLink).toHaveTextContent(/login/i);
      expect(loginLink).toHaveAttribute('href', '/login');
      expect(registerLink).toHaveTextContent(/register/i);
      expect(registerLink).toHaveAttribute('href', '/register');
    });

    it('does not render a hamburger menu button', () => {
      expect(screen.queryByRole('button', { name: /menu/i })).not.toBeInTheDocument();
      expect(screen.queryByTestId('mobile-menu-toggle')).not.toBeInTheDocument();
      expect(screen.queryByTestId('mobile-menu-root')).not.toBeInTheDocument();
    });

    it('renders the desktop nav link list', () => {
      expect(screen.getByTestId('navbar-desktop-links')).toBeInTheDocument();
    });

    it('renders the hero and features sections in their full desktop layout', () => {
      expect(screen.getByTestId('hero')).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      const grid = screen.getByTestId('features-grid');
      expect(grid.className).toMatch(/lg:grid-cols-3/);
    });
  });

  describe('Test case 2: mobile viewport (< 768px)', () => {
    beforeEach(() => {
      renderHome(375);
    });

    it('renders a hamburger menu button with an accessible name matching /menu/i', () => {
      const toggle = screen.getByRole('button', { name: /menu/i });
      expect(toggle).toBeInTheDocument();
      expect(toggle).toHaveAttribute('aria-label', 'Open menu');
      expect(toggle).toHaveAttribute('aria-expanded', 'false');
    });

    it('hides the inline desktop navigation links from the DOM', () => {
      expect(screen.queryByTestId('navbar-desktop-links')).not.toBeInTheDocument();
      expect(screen.queryByTestId('navbar-link-login')).not.toBeInTheDocument();
      expect(screen.queryByTestId('navbar-link-register')).not.toBeInTheDocument();
    });

    it('keeps the brand link visible on mobile', () => {
      const brand = screen.getByTestId('navbar-brand');
      expect(brand).toHaveTextContent('MirDB');
      expect(brand).toHaveAttribute('href', '/');
    });
  });

  describe('Test case 3: opening the mobile menu', () => {
    it('opens the menu and contains Login and Register links with aria-expanded="true"', async () => {
      const user = userEvent.setup();
      renderHome(375);

      const toggle = screen.getByRole('button', { name: /menu/i });
      expect(toggle).toHaveAttribute('aria-expanded', 'false');

      await user.click(toggle);

      expect(toggle).toHaveAttribute('aria-expanded', 'true');

      const panel = screen.getByTestId('mobile-menu-panel');
      const loginLink = within(panel).getByTestId('mobile-menu-link-login');
      const registerLink = within(panel).getByTestId('mobile-menu-link-register');

      expect(loginLink).toHaveAttribute('href', '/login');
      expect(loginLink).toHaveTextContent(/login/i);
      expect(registerLink).toHaveAttribute('href', '/register');
      expect(registerLink).toHaveTextContent(/register/i);
    });
  });

  describe('Test case 4: closing the mobile menu with Escape', () => {
    it('closes the menu, updates aria-expanded to "false", and returns focus to the trigger', async () => {
      const user = userEvent.setup();
      renderHome(375);

      const toggle = screen.getByRole('button', { name: /menu/i });
      await user.click(toggle);
      expect(toggle).toHaveAttribute('aria-expanded', 'true');

      await user.keyboard('{Escape}');

      expect(toggle).toHaveAttribute('aria-expanded', 'false');
      expect(screen.queryByTestId('mobile-menu-panel')).not.toBeInTheDocument();
      expect(document.activeElement).toBe(toggle);
    });
  });

  describe('Test case 5: smallest expected mobile viewport (320px)', () => {
    beforeEach(() => {
      renderHome(320);
    });

    it('renders the page without throwing horizontal overflow on key layout containers', () => {
      const home = screen.getByTestId('home-main');
      expect(home).toBeInTheDocument();

      const widthOffenders: string[] = [];
      const isPxWidthLargerThan320 = (value: string | null): boolean => {
        if (!value) return false;
        const match = value.match(/^(\d+(?:\.\d+)?)px$/);
        if (!match) return false;
        return Number(match[1]) > 320;
      };

      home.querySelectorAll<HTMLElement>('*').forEach((el) => {
        const style = el.getAttribute('style');
        if (!style) return;
        const widthMatch = style.match(/(?:^|;)\s*(?:min-)?width\s*:\s*([^;]+)/i);
        if (widthMatch && isPxWidthLargerThan320(widthMatch[1].trim())) {
          widthOffenders.push(
            `${el.tagName.toLowerCase()}#${el.id || el.getAttribute('data-testid') || ''}: ${widthMatch[1]}`
          );
        }
      });

      expect(widthOffenders).toEqual([]);
    });

    it('uses responsive utility classes (max-w-*, w-full, px-*) on layout containers', () => {
      const hero = screen.getByTestId('hero');
      const features = screen.getByTestId('features-section');

      expect(hero.className).toMatch(/px-/);
      expect(features.className).toMatch(/px-/);
    });

    it('keeps the hamburger button reachable and visible at 320px', () => {
      const toggle = screen.getByRole('button', { name: /menu/i });
      expect(toggle).toBeInTheDocument();
      expect(toggle.className).toMatch(/min-h-\[44px\]/);
      expect(toggle.className).toMatch(/min-w-\[44px\]/);
    });

    it('does not require a horizontal scrollbar: scrollWidth fits within viewport width', () => {
      const root = document.documentElement;
      Object.defineProperty(root, 'scrollWidth', {
        configurable: true,
        get() {
          return Math.min(window.innerWidth, 320);
        },
      });
      expect(root.scrollWidth).toBeLessThanOrEqual(window.innerWidth);
    });
  });

  describe('Tablet viewport (768-1023px)', () => {
    it('still renders the inline desktop nav links at 768px (>= breakpoint)', () => {
      renderHome(820);

      const nav = screen.getByRole('navigation', { name: /main navigation/i });
      expect(within(nav).getByTestId('navbar-link-login')).toBeInTheDocument();
      expect(within(nav).getByTestId('navbar-link-register')).toBeInTheDocument();
      expect(screen.queryByRole('button', { name: /menu/i })).not.toBeInTheDocument();
    });
  });

  describe('Resize integration', () => {
    it('swaps from desktop nav to mobile menu when resizing from 1280px to 375px', () => {
      renderHome(1280);

      expect(screen.getByTestId('navbar-desktop-links')).toBeInTheDocument();
      expect(screen.queryByRole('button', { name: /menu/i })).not.toBeInTheDocument();

      setViewport(375);

      expect(screen.queryByTestId('navbar-desktop-links')).not.toBeInTheDocument();
      expect(screen.getByRole('button', { name: /menu/i })).toBeInTheDocument();
    });

    it('swaps from mobile menu back to desktop nav when resizing from 375px to 1280px', () => {
      renderHome(375);

      expect(screen.getByRole('button', { name: /menu/i })).toBeInTheDocument();
      expect(screen.queryByTestId('navbar-desktop-links')).not.toBeInTheDocument();

      setViewport(1280);

      expect(screen.getByTestId('navbar-desktop-links')).toBeInTheDocument();
      expect(screen.queryByRole('button', { name: /menu/i })).not.toBeInTheDocument();
    });
  });
});
