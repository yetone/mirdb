/**
 * Footer dynamic year renderer — Scenario 6.
 *
 * Expected exports:
 *   - initFooter(root: HTMLElement): void
 *     Injects the current year into .site-footer__year elements.
 *     Validates social links and warns if none are configured.
 */

export function initFooter(root = document) {
    const yearEls = root.querySelectorAll(".site-footer__year");
    const currentYear = new Date().getFullYear().toString();
    yearEls.forEach((el) => {
        el.textContent = currentYear;
    });

    const socialLinksEl = root.querySelector(".site-footer__social-links");
    if (socialLinksEl) {
        const links = socialLinksEl.querySelectorAll("a");
        if (links.length === 0) {
            console.warn("[footer] No social links configured.");
        }
    }
}

export function getFooterYear() {
    return new Date().getFullYear().toString();
}
