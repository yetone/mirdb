/**
 * Main JavaScript for MirDB Homepage
 * Owner: Scenario 3 - Code Examples (primary)
 *
 * Expected functionality:
 * - Tab switching for code examples
 * - Copy-to-clipboard functionality
 * - Progressive enhancement (works without JS for core content)
 *
 * NOTE: Core content must be accessible without JavaScript
 */

// Tab switching functionality
document.addEventListener('DOMContentLoaded', function() {
  // Initialize tab panels
  const tabButtons = document.querySelectorAll('[role="tab"]');
  const tabPanels = document.querySelectorAll('[role="tabpanel"]');

  tabButtons.forEach(button => {
    button.addEventListener('click', () => {
      const targetId = button.getAttribute('aria-controls');

      // Deactivate all tabs
      tabButtons.forEach(btn => {
        btn.setAttribute('aria-selected', 'false');
        btn.classList.remove('bg-primary', 'text-white');
        btn.classList.add('bg-gray-700', 'text-gray-300');
      });

      // Hide all panels
      tabPanels.forEach(panel => {
        panel.classList.add('hidden');
      });

      // Activate clicked tab
      button.setAttribute('aria-selected', 'true');
      button.classList.remove('bg-gray-700', 'text-gray-300');
      button.classList.add('bg-primary', 'text-white');

      // Show corresponding panel
      const targetPanel = document.getElementById(targetId);
      if (targetPanel) {
        targetPanel.classList.remove('hidden');
      }
    });
  });

  // Copy to clipboard functionality
  const copyButtons = document.querySelectorAll('.copy-button');
  copyButtons.forEach(button => {
    button.addEventListener('click', async () => {
      const targetId = button.getAttribute('data-copy-target');
      const codeBlock = document.getElementById(targetId);

      if (codeBlock) {
        try {
          await navigator.clipboard.writeText(codeBlock.textContent);
          const originalText = button.textContent;
          button.textContent = 'Copied!';
          setTimeout(() => {
            button.textContent = originalText;
          }, 2000);
        } catch (err) {
          console.error('Failed to copy:', err);
        }
      }
    });
  });
});
