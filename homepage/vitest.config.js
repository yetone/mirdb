import { defineConfig } from "vitest/config";

export default defineConfig({
    test: {
        environment: "jsdom",
        globals: false,
        setupFiles: ["./tests/setup.js"],
        include: ["tests/**/*.test.js"],
        exclude: ["tests/cross-browser/**", "node_modules/**"],
    },
});
