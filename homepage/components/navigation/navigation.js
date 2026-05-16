/**
 * Navigation behaviour — Scenario 5.
 *
 * Expected exports:
 *   - initNavigation(): void
 *     Toggles aria-expanded on burger; closes nav on outside click / Esc.
 */

export function initNavigation() {
    const burger = document.querySelector(".site-header__burger");
    if (!burger) {
        return;
    }

    if (burger.dataset.navInitialized === "true") {
        return;
    }
    burger.dataset.navInitialized = "true";

    const controlsId = burger.getAttribute("aria-controls");
    let nav = null;

    if (controlsId) {
        nav = document.getElementById(controlsId);
        if (!nav) {
            console.warn(
                `Navigation: aria-controls="${controlsId}" does not match any element in the DOM.`
            );
            return;
        }
    }

    function toggle() {
        const expanded = burger.getAttribute("aria-expanded") === "true";
        const newState = !expanded;
        burger.setAttribute("aria-expanded", String(newState));
        if (nav) {
            nav.classList.toggle("is-open", newState);
        }
    }

    function close() {
        burger.setAttribute("aria-expanded", "false");
        if (nav) {
            nav.classList.remove("is-open");
        }
    }

    burger.addEventListener("click", toggle);

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && burger.getAttribute("aria-expanded") === "true") {
            close();
        }
    });

    document.addEventListener("click", (event) => {
        if (burger.getAttribute("aria-expanded") !== "true") {
            return;
        }
        const header = document.querySelector(".site-header");
        if (header && !header.contains(event.target)) {
            close();
        }
    });
}
