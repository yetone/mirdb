import { beforeEach } from "vitest";

beforeEach(() => {
    document.head.innerHTML = "";
    document.body.innerHTML = "";
    if (typeof window !== "undefined" && window.history && window.history.pushState) {
        window.history.pushState({}, "", "/");
    }
});

if (typeof globalThis.matchMedia === "undefined") {
    globalThis.matchMedia = (query) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: () => {},
        removeListener: () => {},
        addEventListener: () => {},
        removeEventListener: () => {},
        dispatchEvent: () => false,
    });
}
