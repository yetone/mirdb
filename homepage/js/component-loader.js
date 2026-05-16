export async function loadComponent(name, target) {
    if (!name || !target) {
        throw new Error("loadComponent requires a name and a target element");
    }
    const response = await fetch(`components/${name}/${name}.html`);
    if (!response.ok) {
        throw new Error(`Failed to load component "${name}": ${response.status}`);
    }
    const html = await response.text();
    target.innerHTML = html;
}

export async function loadAllComponents(root = document) {
    const placeholders = Array.from(root.querySelectorAll("[data-component]"));
    await Promise.all(placeholders.map(async el => {
        await loadComponent(el.dataset.component, el);
        // Recursively load any nested components that were injected
        const nested = Array.from(el.querySelectorAll("[data-component]"));
        await Promise.all(nested.map(nestedEl => loadComponent(nestedEl.dataset.component, nestedEl)));
    }));
}
