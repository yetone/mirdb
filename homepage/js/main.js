import { loadAllComponents } from "./component-loader.js";
import { attachSignupHandler } from "../components/cta-signup/cta-signup.js";
import { renderFeatures } from "../components/features/features.js";
import { FEATURES } from "../components/features/features.data.js";

async function bootstrap() {
    await loadAllComponents(document);

    document.querySelectorAll('[data-component="cta-signup"]').forEach(slot => {
        attachSignupHandler(slot);
    });

    const featuresSlot = document.querySelector('[data-component="features"] .features__grid');
    if (featuresSlot) {
        renderFeatures(featuresSlot, FEATURES);
    }

    document.dispatchEvent(new CustomEvent("homepage:ready"));
}

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootstrap);
} else {
    bootstrap();
}
