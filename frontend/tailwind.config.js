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
      "light",
      {
        dark: {
          ...require("daisyui/src/theming/themes")["dark"],
          // Fix primary button contrast for WCAG AA compliance (4.5:1 ratio)
          // Darker primary color with white text achieves better contrast
          primary: "#4a50c7",
          "primary-content": "#ffffff",
        },
      },
      "cyberpunk",
      "synthwave",
    ],
  },
}
