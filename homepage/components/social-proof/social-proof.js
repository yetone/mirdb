/*
 * Social proof renderer — Scenario 9 (PRD REQ-8).
 *
 * Given a `<section class="social-proof">` element and a data object of the
 * shape `{ testimonials, badges, userCount }`, populates the section's
 * children. Treats the section as a *progressive* enhancement: when no data
 * is supplied it is hidden via the [hidden] attribute so no empty block
 * ships.
 */

function hasContent(data) {
    if (!data || typeof data !== "object") return false;
    const testimonials = Array.isArray(data.testimonials) ? data.testimonials : [];
    const badges = Array.isArray(data.badges) ? data.badges : [];
    const userCount = data.userCount;
    const hasUserCount =
        typeof userCount === "number" && Number.isFinite(userCount) && userCount > 0;
    return testimonials.length > 0 || badges.length > 0 || hasUserCount;
}

function formatUserCount(n) {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1).replace(/\.0$/, "")}M+ users`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1).replace(/\.0$/, "")}K+ users`;
    return `${n}+ users`;
}

function buildTestimonialItem(testimonial) {
    const li = document.createElement("li");
    li.className = "social-proof__testimonial";

    const body = document.createElement("p");
    body.className = "body";
    body.textContent = String(testimonial.body || "");

    const author = document.createElement("p");
    author.className = "author";
    author.textContent = String(testimonial.author || "");

    li.appendChild(body);
    li.appendChild(author);
    return li;
}

function buildBadgeItem(badge) {
    const li = document.createElement("li");
    li.className = "social-proof__badge";

    const img = document.createElement("img");
    const src = String(badge.src || badge.logo || "");
    const alt = String(badge.alt || badge.name || "").trim();
    if (!alt) {
        console.warn(
            `[social-proof] badge "${src || "(unknown)"}" is missing alt text; using fallback`
        );
    }
    img.setAttribute("src", src);
    img.setAttribute("alt", alt || "Trust badge");
    img.setAttribute("loading", "lazy");
    if (badge.width) img.setAttribute("width", String(badge.width));
    if (badge.height) img.setAttribute("height", String(badge.height));

    li.appendChild(img);
    return li;
}

export function renderSocialProof(section, data = {}) {
    if (!section) {
        throw new Error("renderSocialProof requires a target section element");
    }
    const userCountEl = section.querySelector(".social-proof__user-count");
    const testimonialsList = section.querySelector(".social-proof__testimonials");
    const badgesList = section.querySelector(".social-proof__badges");

    if (testimonialsList) testimonialsList.replaceChildren();
    if (badgesList) badgesList.replaceChildren();
    if (userCountEl) {
        userCountEl.textContent = "";
        userCountEl.setAttribute("data-empty", "true");
    }

    if (!hasContent(data)) {
        section.setAttribute("hidden", "");
        section.setAttribute("aria-hidden", "true");
        return { rendered: false, testimonials: 0, badges: 0, userCount: false };
    }

    section.removeAttribute("hidden");
    section.removeAttribute("aria-hidden");

    const testimonials = Array.isArray(data.testimonials) ? data.testimonials : [];
    for (const t of testimonials) {
        if (!t || typeof t !== "object") continue;
        if (testimonialsList) testimonialsList.appendChild(buildTestimonialItem(t));
    }

    const badges = Array.isArray(data.badges) ? data.badges : [];
    for (const b of badges) {
        if (!b || typeof b !== "object") continue;
        if (badgesList) badgesList.appendChild(buildBadgeItem(b));
    }

    if (
        userCountEl &&
        typeof data.userCount === "number" &&
        Number.isFinite(data.userCount) &&
        data.userCount > 0
    ) {
        userCountEl.textContent = formatUserCount(data.userCount);
        userCountEl.setAttribute("data-empty", "false");
    }

    return {
        rendered: true,
        testimonials: testimonials.length,
        badges: badges.length,
        userCount: typeof data.userCount === "number" && data.userCount > 0,
    };
}

export { hasContent, formatUserCount };
