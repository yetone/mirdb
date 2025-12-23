// Theme toggle functionality
(function() {
  const themeToggle = document.querySelector('.theme-toggle');
  const themeIcon = document.querySelector('.theme-icon');

  // Check for saved theme preference or default to system preference
  const savedTheme = localStorage.getItem('theme');
  const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

  function setTheme(isDark) {
    document.documentElement.setAttribute('data-theme', isDark ? 'dark' : 'light');
    themeIcon.textContent = isDark ? '☀️' : '🌙';
    localStorage.setItem('theme', isDark ? 'dark' : 'light');
  }

  // Initialize theme
  if (savedTheme) {
    setTheme(savedTheme === 'dark');
  } else {
    setTheme(systemPrefersDark);
  }

  // Toggle theme on button click
  themeToggle.addEventListener('click', function() {
    const currentTheme = document.documentElement.getAttribute('data-theme');
    setTheme(currentTheme !== 'dark');
  });

  // Listen for system theme changes
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', function(e) {
    if (!localStorage.getItem('theme')) {
      setTheme(e.matches);
    }
  });
})();

// Copy to clipboard functionality
(function() {
  const copyButtons = document.querySelectorAll('.copy-button');

  copyButtons.forEach(function(button) {
    button.addEventListener('click', async function() {
      // Find the associated code element within the same wrapper
      const wrapper = button.closest('.code-block-wrapper');
      const codeElement = wrapper ? wrapper.querySelector('code') : null;

      if (!codeElement) {
        return;
      }

      // Get just the text content without HTML tags (strip line numbers and spans)
      const codeText = codeElement.textContent;

      try {
        await navigator.clipboard.writeText(codeText);

        // Update button to show success
        const originalText = button.textContent;
        const originalAriaLabel = button.getAttribute('aria-label');

        button.textContent = 'Copied!';
        button.setAttribute('aria-label', 'Code copied to clipboard');
        button.setAttribute('data-copied', 'true');
        button.classList.add('copied');

        // Reset button after 2 seconds
        setTimeout(function() {
          button.textContent = originalText;
          button.setAttribute('aria-label', originalAriaLabel);
          button.removeAttribute('data-copied');
          button.classList.remove('copied');
        }, 2000);
      } catch (err) {
        // Fallback for browsers that don't support clipboard API
        const textArea = document.createElement('textarea');
        textArea.value = codeText;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        document.body.appendChild(textArea);
        textArea.select();

        try {
          document.execCommand('copy');
          button.textContent = 'Copied!';
          button.setAttribute('data-copied', 'true');
          button.classList.add('copied');

          setTimeout(function() {
            button.textContent = 'Copy';
            button.removeAttribute('data-copied');
            button.classList.remove('copied');
          }, 2000);
        } catch (fallbackErr) {
          button.textContent = 'Error';
          setTimeout(function() {
            button.textContent = 'Copy';
          }, 2000);
        }

        document.body.removeChild(textArea);
      }
    });
  });
})();
