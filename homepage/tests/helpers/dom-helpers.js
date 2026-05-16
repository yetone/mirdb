import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const homepageRoot = resolve(here, "..", "..");

export async function loadComponentMarkup(name) {
    const file = resolve(homepageRoot, "components", name, `${name}.html`);
    return readFile(file, "utf8");
}

export async function mountComponent(name, target) {
    const markup = await loadComponentMarkup(name);
    target.innerHTML = markup;
    return target;
}

export function simulateBreakpoint(width) {
    Object.defineProperty(window, "innerWidth", { configurable: true, value: width });
    window.dispatchEvent(new Event("resize"));
}
