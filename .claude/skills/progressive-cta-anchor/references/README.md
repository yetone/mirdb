# Progressive CTA Anchor

A pattern for building homepage call-to-action elements (Sign-Up, Login, Download, etc.) on the MirDB homepage so they:

1. Work without JavaScript (semantic `<a href="…">`)
2. Enhance with SPA-style `history.pushState` navigation when JS is available
3. Emit a vendor-neutral telemetry `CustomEvent`
4. Are idempotent — safe to mount in multiple slots (nav strip + hero)
5. Meet WCAG 2.1 AA (`aria-label`, `:focus-visible`, `prefers-reduced-motion`)

## When to Use

Any time you add a CTA button/link under `homepage/components/cta-*/`. Examples:
- Sign-Up → `/register`
- Login → `/login`
- "Download" or "Try it" → `/downloads`
- "View docs" → `/docs`

This is **not** the right pattern for arbitrary in-page buttons that only run JS (use `<button>` for those).

## File Layout

```
homepage/components/cta-<name>/
├── cta-<name>.html      # Single anchor, no scripts, no inline handlers
├── cta-<name>.css       # Button + :focus-visible + prefers-reduced-motion
└── cta-<name>.js        # Exports: createXAnchor, findXAnchor, attachXHandler
homepage/tests/components/cta-<name>.test.js
```

## HTML Template

```html
<a class="btn btn--primary cta-<name>"
   href="/<destination>"
   data-cta="<name>"
   aria-label="<verb the destination, e.g. Sign up for MirDB>">
    <Visible label>
</a>
```

**Required attributes**:
- `href` — the static destination (anchor-default works without JS)
- `data-cta` — DOM hook the JS handler / tests select on
- `aria-label` — describes the action + product, not just the visible label

## JS Module Template

```js
export const X_HREF = "/<destination>";
export const X_TELEMETRY_EVENT = "cta:<name>:click";

export function createXAnchor({ href = X_HREF, label = "<Visible>" } = {}) {
    const a = document.createElement("a");
    a.className = "btn btn--primary cta-<name>";
    a.dataset.cta = "<name>";
    a.setAttribute("aria-label", "<verb the destination>");
    a.href = href && href.length > 0 ? href : X_HREF;
    a.textContent = label;
    return a;
}

export function findXAnchor(root) {
    if (!root) return null;
    if (root.matches && root.matches('a[data-cta="<name>"]')) return root;
    return root.querySelector('a[data-cta="<name>"]');
}

function emitTelemetry(detail) {
    try {
        document.dispatchEvent(new CustomEvent(X_TELEMETRY_EVENT, { detail, bubbles: true }));
    } catch { /* old test envs */ }
}

function supportsHistoryNavigation() {
    return typeof window !== "undefined"
        && typeof window.history !== "undefined"
        && typeof window.history.pushState === "function";
}

export function attachXHandler(root, options = {}) {
    const anchor = findXAnchor(root);
    if (!anchor) return null;

    // Restore the default destination if a consumer wiped it.
    if (!anchor.getAttribute("href")) anchor.setAttribute("href", X_HREF);
    if (!anchor.textContent || !anchor.textContent.trim()) anchor.textContent = "<Visible>";

    // Idempotent — bail if already attached.
    if (anchor.dataset.xAttached === "true") return anchor;
    anchor.dataset.xAttached = "true";

    anchor.addEventListener("click", (event) => {
        const href = anchor.getAttribute("href") || X_HREF;
        emitTelemetry({ href, source: "cta-<name>" });

        if (options.spa !== false && supportsHistoryNavigation()) {
            event.preventDefault();
            try {
                window.history.pushState({ cta: "<name>" }, "", href);
                window.dispatchEvent(new PopStateEvent("popstate", { state: { cta: "<name>" } }));
            } catch {
                window.location.href = href;
            }
        }
    });
    return anchor;
}
```

## CSS Template

```css
.cta-<name> {
    display: inline-block;
    padding: 0.75rem 1.5rem;
    /* Primary brand color, or transparent/outline for secondary */
    background-color: #2563eb;
    color: #ffffff;
    font-weight: 600;
    border-radius: 6px;
    border: 2px solid transparent;
    text-decoration: none;
    cursor: pointer;
    transition: background-color 150ms ease, transform 100ms ease, box-shadow 150ms ease;
}

.cta-<name>:hover,
.cta-<name>:focus-visible { background-color: #1d4ed8; }

/* High-contrast focus ring per WCAG 2.4.7 */
.cta-<name>:focus-visible {
    outline: 3px solid #f59e0b;
    outline-offset: 2px;
    box-shadow: 0 0 0 4px rgba(245, 158, 11, 0.25);
}

.cta-<name>:active { transform: translateY(1px); }

@media (prefers-reduced-motion: reduce) {
    .cta-<name>          { transition: none; }
    .cta-<name>:active   { transform: none; }
}
```

## Test Skeleton (Vitest + jsdom)

```js
import { describe, it, expect, beforeEach, vi } from "vitest";
import { mountComponent } from "../helpers/dom-helpers.js";
import {
    attachXHandler, createXAnchor, findXAnchor,
    X_HREF, X_TELEMETRY_EVENT,
} from "../../components/cta-<name>/cta-<name>.js";

describe("cta-<name>", () => {
    let root;
    beforeEach(async () => {
        root = document.createElement("div");
        root.setAttribute("data-component", "cta-<name>");
        document.body.appendChild(root);
        await mountComponent("cta-<name>", root);
    });

    it("renders the anchor with correct href and a non-empty label", () => {
        const a = document.querySelector('a[data-cta="<name>"]');
        expect(a).not.toBeNull();
        expect(a.getAttribute("href")).toBe(X_HREF);
        expect(a.textContent.trim()).toMatch(/<label regex>/i);
    });

    it("emits telemetry and pushState-navigates on click", () => {
        attachXHandler(root);
        const spy = vi.fn();
        document.addEventListener(X_TELEMETRY_EVENT, spy);
        findXAnchor(root).click();
        expect(spy).toHaveBeenCalledTimes(1);
        expect(window.location.pathname).toBe(X_HREF);
    });

    it("keeps href even with no handler (no-JS fallback)", () => {
        expect(findXAnchor(root).getAttribute("href")).toBe(X_HREF);
    });

    it("supplies default href when constructed with href=''", () => {
        expect(createXAnchor({ href: "" }).getAttribute("href")).toBe(X_HREF);
    });

    it("is idempotent — double-attach yields one listener", () => {
        attachXHandler(root); attachXHandler(root);
        const spy = vi.fn();
        document.addEventListener(X_TELEMETRY_EVENT, spy);
        findXAnchor(root).click();
        expect(spy).toHaveBeenCalledTimes(1);
    });
});
```

## Wiring into the App

`homepage/index.html` places a placeholder:
```html
<div data-component="cta-<name>"></div>
```

`homepage/js/main.js` should call the handler after `loadAllComponents()`:
```js
document.querySelectorAll('[data-component="cta-<name>"]').forEach(slot => {
    attachXHandler(slot);
});
```

## Key Design Decisions

| Decision                                   | Why                                                                                  |
|--------------------------------------------|--------------------------------------------------------------------------------------|
| `<a href>` not `<button>`                  | Works without JS, correctly announced by screen readers, native focus order          |
| Telemetry as bubbling `CustomEvent`        | No vendor lock-in; tests subscribe via `addEventListener`, no global mocks needed    |
| Centralised `X_HREF` constant              | Single source of truth; reused by markup default, factory, and restore path          |
| `dataset.xAttached` idempotency marker     | Cheapest "have we attached?" check; survives multi-slot mounting                     |
| `:focus-visible` (not `:focus`)            | Avoids ring on mouse click; still visible for keyboard / programmatic focus          |
| `prefers-reduced-motion: reduce` override  | Vestibular-safe; required for WCAG conformance                                       |

## Anti-Patterns

- Do **not** use `<button onclick="…">` — breaks no-JS, fails accessibility tests.
- Do **not** dispatch telemetry through a global SDK call from inside the component — couples it to a vendor.
- Do **not** read the destination from a config object passed at mount time — keep it as a module-level constant so URL changes are a single grep.
- Do **not** use `:focus { outline: none }` to "clean up" the ring — leave `:focus-visible` styled.
