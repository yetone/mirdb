export const SIGNUP_HREF = "/register";
export const SIGNUP_TELEMETRY_EVENT = "cta:signup:click";

export function createSignupAnchor({ href = SIGNUP_HREF, label = "Sign Up" } = {}) {
    const anchor = document.createElement("a");
    anchor.className = "btn btn--primary cta-signup";
    anchor.dataset.cta = "signup";
    anchor.setAttribute("aria-label", `Sign up for MirDB`);
    anchor.href = href && href.length > 0 ? href : SIGNUP_HREF;
    anchor.textContent = label;
    return anchor;
}

export function findSignupAnchor(root) {
    if (!root) return null;
    if (root.matches && root.matches('a[data-cta="signup"]')) return root;
    return root.querySelector('a[data-cta="signup"]');
}

function emitTelemetry(detail) {
    try {
        const ev = new CustomEvent(SIGNUP_TELEMETRY_EVENT, { detail, bubbles: true });
        document.dispatchEvent(ev);
    } catch {
        /* environments without CustomEvent simply skip telemetry */
    }
}

function supportsHistoryNavigation() {
    return typeof window !== "undefined"
        && typeof window.history !== "undefined"
        && typeof window.history.pushState === "function";
}

export function attachSignupHandler(root, options = {}) {
    const anchor = findSignupAnchor(root);
    if (!anchor) return null;

    if (!anchor.getAttribute("href")) {
        anchor.setAttribute("href", SIGNUP_HREF);
    }
    if (!anchor.textContent || !anchor.textContent.trim()) {
        anchor.textContent = "Sign Up";
    }
    if (anchor.dataset.signupAttached === "true") {
        return anchor;
    }
    anchor.dataset.signupAttached = "true";

    const onClick = (event) => {
        const targetHref = anchor.getAttribute("href") || SIGNUP_HREF;
        emitTelemetry({ href: targetHref, source: "cta-signup" });

        const allowSpa = options.spa !== false && supportsHistoryNavigation();
        if (allowSpa) {
            event.preventDefault();
            try {
                window.history.pushState({ cta: "signup" }, "", targetHref);
                window.dispatchEvent(new PopStateEvent("popstate", { state: { cta: "signup" } }));
            } catch {
                window.location.href = targetHref;
            }
        }
    };

    anchor.addEventListener("click", onClick);
    return anchor;
}
