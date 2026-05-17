import { useCallback, useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { NAV_LINKS } from '../../utils/constants';

/**
 * Mobile hamburger menu.
 * Owner: Scenario 5 - Responsive Design Across Viewports.
 *
 * Renders a hamburger trigger button and a slide-in panel that lists the
 * primary navigation links (Login, Register). The component handles open/close
 * state internally, closes on Escape and on overlay click, and returns focus
 * to the trigger button when it closes for keyboard/screen-reader users.
 *
 * The component gates itself on viewport via `useIsMobile`, returning null at
 * tablet and desktop widths so the desktop nav remains the only navigation
 * surface above the 768px breakpoint.
 *
 * Touch targets meet the 44x44 CSS-pixel WCAG 2.1 AA minimum.
 */

export const MOBILE_BREAKPOINT_PX = 768;
const MENU_PANEL_ID = 'mobile-menu-panel';

export function useIsMobile(breakpoint: number = MOBILE_BREAKPOINT_PX): boolean {
  const getInitial = () =>
    typeof window !== 'undefined' && window.innerWidth < breakpoint;
  const [isMobile, setIsMobile] = useState<boolean>(getInitial);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const handleResize = () => {
      setIsMobile(window.innerWidth < breakpoint);
    };
    handleResize();
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, [breakpoint]);

  return isMobile;
}

export default function MobileMenu() {
  const isMobile = useIsMobile();
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);

  const close = useCallback(() => {
    setOpen(false);
    triggerRef.current?.focus();
  }, []);

  useEffect(() => {
    if (!open) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.stopPropagation();
        close();
      }
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [open, close]);

  useEffect(() => {
    if (!isMobile && open) {
      setOpen(false);
    }
  }, [isMobile, open]);

  if (!isMobile) return null;

  return (
    <div data-testid="mobile-menu-root">
      <button
        ref={triggerRef}
        type="button"
        aria-label={open ? 'Close menu' : 'Open menu'}
        aria-expanded={open}
        aria-controls={MENU_PANEL_ID}
        aria-haspopup="menu"
        data-testid="mobile-menu-toggle"
        onClick={() => setOpen((prev) => !prev)}
        className="btn btn-ghost btn-square min-h-[44px] min-w-[44px] p-2"
      >
        <svg
          xmlns="http://www.w3.org/2000/svg"
          fill="none"
          viewBox="0 0 24 24"
          strokeWidth={2}
          stroke="currentColor"
          className="w-6 h-6"
          aria-hidden="true"
        >
          {open ? (
            <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
          ) : (
            <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6.75h16.5M3.75 12h16.5M3.75 17.25h16.5" />
          )}
        </svg>
      </button>
      {open && (
        <>
          <div
            data-testid="mobile-menu-overlay"
            aria-hidden="true"
            onClick={close}
            className="fixed inset-0 bg-black/40 z-40"
          />
          <nav
            id={MENU_PANEL_ID}
            role="menu"
            aria-label="Mobile navigation"
            data-testid="mobile-menu-panel"
            className="fixed top-0 right-0 z-50 h-full w-72 max-w-[80vw] bg-base-100 shadow-xl flex flex-col gap-2 p-6"
          >
            <ul className="menu menu-vertical gap-2 w-full">
              {NAV_LINKS.map((link) => (
                <li key={link.to} role="none">
                  <Link
                    to={link.to}
                    role="menuitem"
                    aria-label={link.ariaLabel}
                    onClick={close}
                    data-testid={`mobile-menu-link-${link.label.toLowerCase()}`}
                    className="min-h-[44px] flex items-center px-3 py-2"
                  >
                    {link.label}
                  </Link>
                </li>
              ))}
            </ul>
          </nav>
        </>
      )}
    </div>
  );
}
