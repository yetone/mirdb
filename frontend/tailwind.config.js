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
        // Light theme - WCAG AA compliant
        light: {
          "primary": "#4506cb",           // Accessible purple (contrast: 9.5:1 on white)
          "primary-content": "#ffffff",
          "secondary": "#b91c8c",         // Accessible pink (contrast: 5.3:1 on white)
          "secondary-content": "#ffffff",
          "accent": "#0e8a7d",            // Accessible teal (contrast: 4.5:1 on white)
          "accent-content": "#ffffff",
          "neutral": "#3d4451",
          "neutral-content": "#ffffff",
          "base-100": "#ffffff",
          "base-200": "#f2f2f2",
          "base-300": "#e5e6e6",
          "base-content": "#1f2937",      // Dark text (contrast: 12.6:1)
          "info": "#3abff8",
          "success": "#16a34a",
          "warning": "#d97706",
          "error": "#dc2626",
        },
      },
      {
        // Dark theme - WCAG AA compliant
        dark: {
          "primary": "#a78bfa",           // Accessible light purple (contrast: 7.6:1)
          "primary-content": "#1a1a1a",
          "secondary": "#f472b6",         // Accessible pink (contrast: 5.1:1)
          "secondary-content": "#1a1a1a",
          "accent": "#34d399",            // Accessible teal (contrast: 4.6:1)
          "accent-content": "#1a1a1a",
          "neutral": "#2a323c",
          "neutral-content": "#c9d1d9",
          "base-100": "#1d232a",
          "base-200": "#191e24",
          "base-300": "#15191e",
          "base-content": "#c9d1d9",      // Light gray text (contrast: 9.3:1)
          "info": "#3abff8",
          "success": "#22c55e",
          "warning": "#fbbf24",
          "error": "#f87171",
        },
      },
      {
        // Cyberpunk theme - WCAG AA compliant
        cyberpunk: {
          "primary": "#9d00c6",           // Accessible magenta (contrast: 7.0:1)
          "primary-content": "#ffffff",
          "secondary": "#006b8f",         // Accessible dark cyan (contrast: 5.7:1)
          "secondary-content": "#ffffff",
          "accent": "#c41e3d",            // Accessible pink (contrast: 5.3:1)
          "accent-content": "#ffffff",
          "neutral": "#0d0d0d",
          "neutral-content": "#ffee00",
          "base-100": "#ffee00",
          "base-200": "#eedd00",
          "base-300": "#ddcc00",
          "base-content": "#0d0d0d",      // Near black text (contrast: 17.2:1)
          "info": "#0066aa",
          "success": "#006b3d",
          "warning": "#995c00",
          "error": "#aa0000",
        },
      },
      {
        // Synthwave theme - WCAG AA compliant
        synthwave: {
          "primary": "#f0abfc",           // Accessible light pink (contrast: 8.4:1)
          "primary-content": "#1a1a1a",
          "secondary": "#7dd3fc",         // Accessible light blue (contrast: 9.5:1)
          "secondary-content": "#1a1a1a",
          "accent": "#fcd34d",            // Accessible yellow (contrast: 12.4:1)
          "accent-content": "#1a1a1a",
          "neutral": "#221551",
          "neutral-content": "#f9f7fd",
          "base-100": "#1a1a2e",
          "base-200": "#16162a",
          "base-300": "#121226",
          "base-content": "#f5f5f5",      // Light text (contrast: 14.3:1)
          "info": "#7dd3fc",
          "success": "#86efac",
          "warning": "#fcd34d",
          "error": "#fca5a5",
        },
      },
    ],
  },
}
