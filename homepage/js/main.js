import { loadAllComponents } from "./component-loader.js";
import { attachSignupHandler } from "../components/cta-signup/cta-signup.js";
import { attachLoginHandler } from "../components/cta-login/cta-login.js";
import { renderFeatures } from "../components/features/features.js";
import { FEATURES } from "../components/features/features.data.js";
import { initFooter } from "../components/footer/footer.js";

async function bootstrap() {
    await loadAllComponents(document);

    document.querySelectorAll('[data-component="cta-signup"]').forEach(slot => {
        attachSignupHandler(slot);
    });

    document.querySelectorAll('[data-component="cta-login"]').forEach(slot => {
        attachLoginHandler(slot);
    });

    const featuresSlot = document.querySelector('[data-component="features"] .features__grid');
    if (featuresSlot) {
        renderFeatures(featuresSlot, FEATURES);
    }

    const footerSlot = document.querySelector('[data-component="footer"]');
    if (footerSlot) {
        initFooter(footerSlot);
    }

    document.dispatchEvent(new CustomEvent("homepage:ready"));
}

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootstrap);
} else {
    bootstrap();
}
