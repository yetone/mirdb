/**
 * Core JavaScript for MirDB Homepage.
 *
 * Created by the first scenario builder.
 * Contains:
 * - DOMContentLoaded initialization
 * - Smooth scroll for anchor links
 * - Copy-to-clipboard functionality for code blocks
 */

document.addEventListener('DOMContentLoaded', function() {
  initSmoothScroll();
});

function initSmoothScroll() {
  document.querySelectorAll('a[href^="#"]').forEach(function(anchor) {
    anchor.addEventListener('click', function(e) {
      e.preventDefault();
      var target = document.querySelector(this.getAttribute('href'));
      if (target) {
        target.scrollIntoView({ behavior: 'smooth' });
      }
    });
  });
}
