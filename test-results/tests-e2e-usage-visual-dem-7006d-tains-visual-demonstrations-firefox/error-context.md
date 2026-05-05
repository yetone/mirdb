# Instructions

- Following Playwright test failed.
- Explain why, be concise, respect Playwright best practices.
- Provide a snippet of code with the fix, if possible.

# Test info

- Name: tests/e2e/usage/visual-demos.spec.js >> Usage section contains visual demonstrations
- Location: tests/e2e/usage/visual-demos.spec.js:3:1

# Error details

```
Error: browserType.launch: 
╔══════════════════════════════════════════════════════╗
║ Host system is missing dependencies to run browsers. ║
║ Please install them with the following command:      ║
║                                                      ║
║     sudo npx playwright install-deps                 ║
║                                                      ║
║ Alternatively, use apt:                              ║
║     sudo apt-get install libxcb-shm0\                ║
║         libx11-xcb1\                                 ║
║         libxrandr2\                                  ║
║         libxcomposite1\                              ║
║         libxcursor1\                                 ║
║         libxdamage1\                                 ║
║         libxi6\                                      ║
║         libxfixes3\                                  ║
║         libgtk-3-0\                                  ║
║         libpangocairo-1.0-0\                         ║
║         libpango-1.0-0\                              ║
║         libatk1.0-0\                                 ║
║         libcairo-gobject2\                           ║
║         libcairo2\                                   ║
║         libgdk-pixbuf-2.0-0\                         ║
║         libglib2.0-0\                                ║
║         libxrender1\                                 ║
║         libasound2\                                  ║
║         libdbus-1-3                                  ║
║                                                      ║
║ <3 Playwright Team                                   ║
╚══════════════════════════════════════════════════════╝
```