import { loadAllComponents } from "./component-loader.js";
import { attachSignupHandler } from "../components/cta-signup/cta-signup.js";

async function bootstrap() {
    await loadAllComponents(document);
    document.querySelectorAll('[data-component="cta-signup"]').forEach(slot => {
        attachSignupHandler(slot);
    });
    document.dispatchEvent(new CustomEvent("homepage:ready"));
}

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootstrap);
} else {
    bootstrap();
}
