# mirdb-mobile-disclosure-menu

Implement an accessible hamburger / disclosure menu in MirDB's React + Tailwind + DaisyUI frontend. Reference implementation: `frontend/src/components/homepage/MobileMenu.tsx`.

## When to use

- Adding a hamburger or off-canvas navigation surface
- Building a dropdown menu triggered by a button (account menu, filter pop-over, etc.)
- Any disclosure widget that opens transient UI a screen-reader user must be able to announce, dismiss, and re-focus from

## Pattern checklist

A correct disclosure widget in this codebase has **all** of the following. The unit-test suite at `frontend/tests/components/homepage/MobileMenu.test.tsx` asserts each one literally, so skipping any will fail tests.

1. **Trigger button**
   - `type="button"` (never default-submit inside a form)
   - `aria-expanded` reflects open state as the string `"true"` / `"false"`
   - `aria-controls` points to the panel's `id`
   - `aria-haspopup="menu"` (matches the panel's role)
   - `aria-label` flips between an action-oriented "Open X" and "Close X"
   - Touch target classes `min-h-[44px] min-w-[44px]` (WCAG 2.1 AA)
   - Stored in a `useRef<HTMLButtonElement>` so focus can return after close

2. **Panel**
   - Rendered only when open (conditional render, not `display: none`)
   - `id` matches the trigger's `aria-controls`
   - `role="menu"` plus a meaningful `aria-label`
   - Children use `role="menuitem"`; their wrapping `<li>` uses `role="none"` so screen readers don't double-announce listitem + menuitem

3. **Close paths**
   - Document-level `keydown` listener calls a memoised `close()` on `Escape` (only attached while open — clean up in the effect's return)
   - An overlay div behind the panel calls `close()` on click
   - Each menuitem link calls `close()` on activation
   - All three paths call the same `close()` so focus return is consistent

4. **Focus return**
   - `close()` runs `triggerRef.current?.focus()` so dismissed UI returns focus to its trigger (WCAG 2.4.3 Focus Order)
   - `close()` is wrapped in `useCallback` so the keydown effect's dependency list is stable

5. **Viewport gating** (when the menu is mobile-only)
   - Import `useIsMobile` from `MobileMenu.tsx` (do not duplicate the hook)
   - `if (!isMobile) return null;` at the top of the component — this makes the trigger genuinely absent from the DOM at desktop widths, which is what `queryByRole('button', { name: /menu/i })` proves
   - On a transition to `!isMobile`, an effect collapses any open state so stale state cannot bleed between viewports

## Skeleton (copy-paste starting point)

```tsx
import { useCallback, useEffect, useRef, useState } from 'react';

const PANEL_ID = 'my-menu-panel';

export default function MyDisclosure() {
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);

  const close = useCallback(() => {
    setOpen(false);
    triggerRef.current?.focus();
  }, []);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.stopPropagation();
        close();
      }
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [open, close]);

  return (
    <>
      <button
        ref={triggerRef}
        type="button"
        aria-label={open ? 'Close menu' : 'Open menu'}
        aria-expanded={open}
        aria-controls={PANEL_ID}
        aria-haspopup="menu"
        onClick={() => setOpen((p) => !p)}
        className="btn btn-ghost btn-square min-h-[44px] min-w-[44px] p-2"
      >
        {/* svg icon, aria-hidden */}
      </button>
      {open && (
        <>
          <div onClick={close} aria-hidden="true" className="fixed inset-0 bg-black/40 z-40" />
          <nav id={PANEL_ID} role="menu" aria-label="My menu" className="fixed top-0 right-0 z-50 ...">
            <ul className="menu menu-vertical gap-2 w-full">
              <li role="none">
                <a role="menuitem" onClick={close} className="min-h-[44px] flex items-center px-3 py-2">
                  Link label
                </a>
              </li>
            </ul>
          </nav>
        </>
      )}
    </>
  );
}
```

## Common mistakes to avoid

- Using `hidden md:flex` instead of conditional render. JSDOM does not load Tailwind, so the trigger remains discoverable to Testing Library queries even when it visually disappears — tests assert absence with `queryByRole`, which only works when the element is not rendered at all.
- Putting the keydown listener on the panel instead of the document. The panel is not focused, so its keydown never fires.
- Forgetting `event.stopPropagation()` inside the Escape handler — without it, an Escape that closes this menu can also propagate up and close a parent modal.
- Letting `close` be re-created every render. Wrap it in `useCallback` so the keydown effect doesn't re-subscribe on every state change.
- Hard-coding `min-h-11` etc. Tailwind's default scale skips 44px — always use the arbitrary `min-h-[44px] min-w-[44px]` so the value is exact and is grep-able by the test suite.

## Related skills

- `mirdb-jsdom-viewport-testing` — for the `useIsMobile` hook and the synthetic resize event helper
- `mirdb-responsive-test-partitioning` — for the integration-test layout that exercises this menu
