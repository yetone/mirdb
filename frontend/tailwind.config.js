/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {},
  },
  plugins: [require("daisyui")],
  daisyui: {
    themes: [
      {
        light: {
          ...require("daisyui/src/theming/themes")["light"],
          // Override secondary to ensure WCAG AA compliance (4.5:1 contrast)
          // #7c3aed with white (#ffffff) achieves 4.84:1 contrast ratio
          secondary: "#7c3aed",
          "secondary-content": "#ffffff",
        },
      },
      {
        dark: {
          ...require("daisyui/src/theming/themes")["dark"],
          // Override secondary to ensure WCAG AA compliance
          secondary: "#a78bfa",
          "secondary-content": "#1f2937",
        },
      },
      {
        cyberpunk: {
          ...require("daisyui/src/theming/themes")["cyberpunk"],
          // Override secondary to ensure WCAG AA compliance (4.5:1 contrast)
          // #7c3aed with white (#ffffff) achieves 4.84:1 contrast ratio
          secondary: "#7c3aed",
          "secondary-content": "#ffffff",
        },
      },
      {
        synthwave: {
          ...require("daisyui/src/theming/themes")["synthwave"],
          // Override secondary to ensure WCAG AA compliance
          secondary: "#9333ea",
          "secondary-content": "#ffffff",
        },
      },
      {
        retro: {
          ...require("daisyui/src/theming/themes")["retro"],
          // Override secondary to ensure WCAG AA compliance
          secondary: "#7c3aed",
          "secondary-content": "#ffffff",
        },
      },
      {
        valentine: {
          ...require("daisyui/src/theming/themes")["valentine"],
          // Override secondary to ensure WCAG AA compliance (was #ff00d3 with #fff8fd text = 3.24:1)
          secondary: "#be185d",
          "secondary-content": "#ffffff",
        },
      },
      {
        night: {
          ...require("daisyui/src/theming/themes")["night"],
          // Override secondary to ensure WCAG AA compliance (4.5:1 contrast)
          // #7c3aed with white (#ffffff) achieves 4.84:1 contrast ratio
          secondary: "#7c3aed",
          "secondary-content": "#ffffff",
        },
      },
    ],
  },
};
