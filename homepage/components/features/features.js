/*
 * Features section renderer — Scenario 2.
 *
 * Builds a list of <li class="feature-card"> children inside the supplied
 * grid element. Each card contains an icon, an <h3 class="feature-card__title">,
 * and a <p class="feature-card__description">.
 *
 * Duplicate ids are filtered out with a console.warn.
 */

export function dedupeFeatures(features) {
    const seen = new Set();
    const result = [];
    const duplicates = [];
    for (const feature of features) {
        if (!feature || typeof feature.id !== "string") {
            continue;
        }
        if (seen.has(feature.id)) {
            duplicates.push(feature.id);
            continue;
        }
        seen.add(feature.id);
        result.push(feature);
    }
    if (duplicates.length > 0) {
        console.warn(
            `[features] dropped ${duplicates.length} duplicate id(s): ${duplicates.join(", ")}`
        );
    }
    return result;
}

function createCard(feature) {
    const card = document.createElement("li");
    card.className = "feature-card";
    card.setAttribute("data-feature-id", feature.id);

    const iconWrap = document.createElement("span");
    iconWrap.className = "feature-card__icon";
    iconWrap.setAttribute("aria-hidden", "true");
    iconWrap.innerHTML = feature.icon;

    const title = document.createElement("h3");
    title.className = "feature-card__title";
    title.textContent = feature.title;

    const description = document.createElement("p");
    description.className = "feature-card__description";
    description.textContent = feature.description;

    card.appendChild(iconWrap);
    card.appendChild(title);
    card.appendChild(description);
    return card;
}

export function renderFeatures(grid, features) {
    if (!grid) {
        throw new Error("renderFeatures requires a target grid element");
    }
    const unique = dedupeFeatures(Array.isArray(features) ? features : []);
    grid.replaceChildren();
    for (const feature of unique) {
        grid.appendChild(createCard(feature));
    }
    return unique.length;
}
