/*
 * Owner: first builder.
 * Bootstraps the homepage on DOMContentLoaded.
 * Imports and initializes:
 *   - initTheme() from ./theme.js
 *   - initCopyButtons() from ./copy.js
 *   - initNavigation() from ./navigation.js
 * MUST tolerate missing modules gracefully so each scenario builder can ship independently.
 */

document.addEventListener('DOMContentLoaded', async () => {
  // Theme toggle (Scenario 8)
  try {
    const { initTheme } = await import('./theme.js');
    if (initTheme) initTheme();
  } catch {
    /* theme.js not yet available */
  }

  // Copy buttons (Scenario 3)
  try {
    const { initCopyButtons } = await import('./copy.js');
    if (initCopyButtons) initCopyButtons();
  } catch {
    /* copy.js not yet available */
  }

  // Navigation (Scenario 13)
  try {
    const { initNavigation } = await import('./navigation.js');
    if (initNavigation) initNavigation();
  } catch {
    /* navigation.js not yet available */
  }
});
