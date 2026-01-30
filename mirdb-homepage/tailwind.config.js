/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{html,js,ts}'],
  theme: {
    extend: {
      fontFamily: {
        mono: ['Consolas', 'Monaco', 'Andale Mono', 'Ubuntu Mono', 'monospace'],
      },
      colors: {
        terminal: {
          bg: '#1e1e1e',
          text: '#d4d4d4',
          command: '#569cd6',
          key: '#9cdcfe',
          value: '#ce9178',
          number: '#b5cea8',
          response: '#6a9955',
        },
      },
    },
  },
  plugins: [],
};
