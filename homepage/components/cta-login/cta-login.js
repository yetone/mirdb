export const LOGIN_HREF = "/login";
export const LOGIN_TELEMETRY_EVENT = "cta:login:click";

export function createLoginAnchor({ href = LOGIN_HREF, label = "Log In" } = {}) {
    const anchor = document.createElement("a");
    anchor.className = "btn btn--secondary cta-login";
    anchor.dataset.cta = "login";
    anchor.setAttribute("aria-label", `Log in to MirDB`);
    anchor.href = href && href.length > 0 ? href : LOGIN_HREF;
    anchor.textContent = label;
    return anchor;
}

export function findLoginAnchor(root) {
    if (!root) return null;
    if (root.matches && root.matches('a[data-cta="login"]')) return root;
    return root.querySelector('a[data-cta="login"]');
}

function emitTelemetry(detail) {
    try {
        const ev = new CustomEvent(LOGIN_TELEMETRY_EVENT, { detail, bubbles: true });
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

export function attachLoginHandler(root, options = {}) {
    const anchor = findLoginAnchor(root);
    if (!anchor) return null;

    if (!anchor.getAttribute("href")) {
        anchor.setAttribute("href", LOGIN_HREF);
    }
    if (!anchor.textContent || !anchor.textContent.trim()) {
        anchor.textContent = "Log In";
    }
    if (anchor.dataset.loginAttached === "true") {
        return anchor;
    }
    anchor.dataset.loginAttached = "true";

    const onClick = (event) => {
        const targetHref = anchor.getAttribute("href") || LOGIN_HREF;
        emitTelemetry({ href: targetHref, source: "cta-login" });

        const allowSpa = options.spa !== false && supportsHistoryNavigation();
        if (allowSpa) {
            event.preventDefault();
            try {
                window.history.pushState({ cta: "login" }, "", targetHref);
                window.dispatchEvent(new PopStateEvent("popstate", { state: { cta: "login" } }));
            } catch {
                window.location.href = targetHref;
            }
        }
    };

    anchor.addEventListener("click", onClick);
    return anchor;
}
