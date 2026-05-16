import { describe, it, expect, beforeEach, vi } from "vitest";
import { mountComponent } from "../helpers/dom-helpers.js";
import {
    attachSignupHandler,
    createSignupAnchor,
    findSignupAnchor,
    SIGNUP_HREF,
    SIGNUP_TELEMETRY_EVENT,
} from "../../components/cta-signup/cta-signup.js";

describe("cta-signup component", () => {
    let root;

    beforeEach(async () => {
        root = document.createElement("div");
        root.setAttribute("data-component", "cta-signup");
        document.body.appendChild(root);
        await mountComponent("cta-signup", root);
    });

    // ---------- TC 1: unit — rendering ----------
    it("renders an anchor with data-cta='signup', href='/register', and non-empty text", () => {
        const anchor = document.querySelector('a[data-cta="signup"]');
        expect(anchor).not.toBeNull();
        expect(anchor.getAttribute("href")).toBe("/register");
        expect(anchor.textContent.trim().length).toBeGreaterThan(0);
        expect(anchor.textContent.trim()).toMatch(/sign\s*up|get\s*started/i);
    });

    // ---------- TC 2: integration — click triggers SPA navigation ----------
    it("intercepts the click, fires telemetry, and pushState-navigates to /register", () => {
        attachSignupHandler(root);
        const anchor = findSignupAnchor(root);
        expect(anchor).not.toBeNull();

        const telemetrySpy = vi.fn();
        document.addEventListener(SIGNUP_TELEMETRY_EVENT, telemetrySpy);

        anchor.click();

        expect(telemetrySpy).toHaveBeenCalledTimes(1);
        expect(telemetrySpy.mock.calls[0][0].detail).toMatchObject({
            href: "/register",
            source: "cta-signup",
        });
        expect(window.location.pathname).toBe("/register");
    });

    // ---------- TC 3: e2e-like — getByRole('link', { name: /sign up|get started/i }) navigates ----------
    it("is discoverable by accessible name and navigates to /register on click", () => {
        attachSignupHandler(root);
        const links = Array.from(document.querySelectorAll('a'))
            .filter(a => /sign\s*up|get\s*started/i.test(
                a.getAttribute("aria-label") || a.textContent || ""
            ));
        expect(links.length).toBeGreaterThan(0);
        const link = links[0];

        link.click();
        expect(window.location.pathname).toMatch(/\/register$/);
    });

    // ---------- TC 4: focus-visible — anchor is keyboard reachable ----------
    it("is focusable (anchors default to tabindex 0) and exposes an accessible name", () => {
        const anchor = findSignupAnchor(root);
        anchor.focus();
        expect(document.activeElement).toBe(anchor);

        const accessibleName = anchor.getAttribute("aria-label") || anchor.textContent.trim();
        expect(accessibleName.length).toBeGreaterThan(0);

        // tabindex should default to 0 for <a> with href
        const tabIndex = anchor.tabIndex;
        expect(tabIndex).toBeGreaterThanOrEqual(0);
    });

    // ---------- TC 5: no JS — anchor href fallback ----------
    it("retains href='/register' even when no handler is attached (no-JS fallback)", () => {
        // intentionally do NOT call attachSignupHandler
        const anchor = findSignupAnchor(root);
        expect(anchor.getAttribute("href")).toBe("/register");
        // Anchor has correct href so native browser navigation works without JS.
        // jsdom does not actually navigate, but the contract (href === '/register')
        // is what guarantees the no-JS UX.
    });

    // ---------- TC 6: negative — default href applied when omitted ----------
    it("supplies a default href of '/register' when the component is rendered with href omitted", () => {
        const anchor = createSignupAnchor({ href: "" });
        expect(anchor.getAttribute("href")).toBe(SIGNUP_HREF);

        // Also assert that the live component path is resilient: stripping the
        // href and re-attaching the handler restores the default rather than
        // rendering an empty href.
        const liveAnchor = findSignupAnchor(root);
        liveAnchor.removeAttribute("href");
        attachSignupHandler(root);
        expect(liveAnchor.getAttribute("href")).toBe(SIGNUP_HREF);
    });
});

describe("cta-signup public API", () => {
    it("createSignupAnchor produces a well-formed link", () => {
        const a = createSignupAnchor();
        expect(a.tagName).toBe("A");
        expect(a.dataset.cta).toBe("signup");
        expect(a.getAttribute("href")).toBe("/register");
        expect(a.classList.contains("cta-signup")).toBe(true);
        expect(a.textContent.trim()).toMatch(/sign\s*up/i);
    });

    it("attachSignupHandler is idempotent", () => {
        const container = document.createElement("div");
        container.appendChild(createSignupAnchor());
        document.body.appendChild(container);

        attachSignupHandler(container);
        attachSignupHandler(container);
        const anchor = findSignupAnchor(container);
        expect(anchor.dataset.signupAttached).toBe("true");

        const spy = vi.fn();
        document.addEventListener(SIGNUP_TELEMETRY_EVENT, spy);
        anchor.click();
        expect(spy).toHaveBeenCalledTimes(1);
    });
});
