# React Mobile Menu Pattern

## Overview

This skill provides the pattern for implementing an accessible mobile hamburger menu in React with TypeScript. It includes a slide-out drawer, keyboard navigation (Escape to close), focus management, and click-outside-to-close functionality.

## When to Use This Skill

Use this skill when:
- Creating a mobile navigation menu
- Implementing a hamburger menu that shows on mobile viewports
- Building a responsive header that switches from desktop nav to hamburger menu
- Needing an accessible slide-out drawer pattern
- Requiring proper focus management for modal-like overlays

## Core Capabilities

### 1. Mobile Menu Component

The MobileMenu component is a dialog-style overlay with:
- Slide-in animation from the right
- Backdrop overlay that closes menu when clicked
- ARIA attributes for accessibility (`role="dialog"`, `aria-modal="true"`)
- Focus trap and initial focus on first link

```tsx
interface MobileMenuProps {
  isOpen: boolean
  onClose: () => void
}

export function MobileMenu({ isOpen, onClose }: MobileMenuProps) {
  const menuRef = useRef<HTMLDivElement>(null)
  const firstLinkRef = useRef<HTMLAnchorElement>(null)

  // Escape key handler
  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (e.key === 'Escape') {
      onClose()
    }
  }, [onClose])

  // Click outside handler
  const handleClickOutside = useCallback((e: MouseEvent) => {
    if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
      onClose()
    }
  }, [onClose])

  useEffect(() => {
    if (isOpen) {
      document.addEventListener('keydown', handleKeyDown)
      document.addEventListener('mousedown', handleClickOutside)
      document.body.style.overflow = 'hidden' // Prevent body scroll
      setTimeout(() => firstLinkRef.current?.focus(), 100)
    } else {
      document.body.style.overflow = ''
    }
    return () => {
      document.removeEventListener('keydown', handleKeyDown)
      document.removeEventListener('mousedown', handleClickOutside)
      document.body.style.overflow = ''
    }
  }, [isOpen, handleKeyDown, handleClickOutside])

  return (
    <>
      <div className={`backdrop ${isOpen ? 'open' : ''}`} aria-hidden="true" />
      <div
        ref={menuRef}
        className={`mobileMenu ${isOpen ? 'open' : ''}`}
        aria-hidden={!isOpen}
        role="dialog"
        aria-modal="true"
        aria-label="Mobile navigation menu"
      >
        {/* Nav links with tabIndex based on isOpen */}
      </div>
    </>
  )
}
```

### 2. Hamburger Button in Header

The Header component manages state and returns focus to hamburger:

```tsx
export function Header() {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)
  const hamburgerRef = useRef<HTMLButtonElement>(null)

  const handleCloseMobileMenu = useCallback(() => {
    setIsMobileMenuOpen(false)
    hamburgerRef.current?.focus() // Return focus for accessibility
  }, [])

  return (
    <header>
      <button
        ref={hamburgerRef}
        onClick={() => setIsMobileMenuOpen(prev => !prev)}
        aria-label={isMobileMenuOpen ? 'Close menu' : 'Open menu'}
        aria-expanded={isMobileMenuOpen}
      >
        {/* Hamburger icon */}
      </button>
      <MobileMenu isOpen={isMobileMenuOpen} onClose={handleCloseMobileMenu} />
    </header>
  )
}
```

### 3. CSS for Slide Animation

```css
.backdrop {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.5);
  opacity: 0;
  visibility: hidden;
  transition: opacity 0.3s, visibility 0.3s;
  z-index: 998;
}
.backdrop.open {
  opacity: 1;
  visibility: visible;
}

.mobileMenu {
  position: fixed;
  top: 0;
  right: 0;
  bottom: 0;
  width: 280px;
  max-width: 80vw;
  background: var(--color-background);
  transform: translateX(100%);
  transition: transform 0.3s;
  z-index: 999;
}
.mobileMenu.open {
  transform: translateX(0);
}
```

## Best Practices

- Always use `aria-hidden` and `tabIndex` to prevent focus on hidden menu items
- Return focus to trigger button when menu closes
- Lock body scroll when menu is open (`overflow: hidden`)
- Use `role="dialog"` and `aria-modal="true"` for screen readers
- Handle both click outside and Escape key for closing
- Provide clear `aria-label` on hamburger button indicating current state
- Use CSS transitions for smooth animations (not JavaScript)

## File Structure

```
src/components/layout/
├── Header/
│   ├── Header.tsx          # Contains hamburger button and state
│   ├── Header.module.css   # Responsive styles
│   └── Header.test.tsx     # Unit tests
└── Navigation/
    ├── Navigation.tsx      # Desktop nav + MobileMenu component
    ├── Navigation.module.css
    └── Navigation.test.tsx
```

## Testing

Test the following scenarios:
- Menu opens when hamburger clicked
- Menu closes on Escape key
- Menu closes on click outside
- Focus returns to hamburger on close
- Links have correct tabIndex based on open state
- ARIA attributes are correctly set
