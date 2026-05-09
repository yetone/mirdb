/*
 * Owner: Scenario 13 - Navigation & Smooth Scroll.
 * Expected exports:
 *   initNavigation(): void
 *     Wires anchor clicks to scrollIntoView({ behavior: 'smooth' }).
 *     Wires #hamburger to toggle nav-list visibility and aria-expanded.
 *     Adds 'scrolled' class to nav after window.scrollY > 32 for sticky styling.
 */

export function initNavigation() {
  const navbar = document.querySelector('#navbar');
  const hamburger = document.querySelector('#hamburger');
  const navList = document.querySelector('#nav-list');

  // Smooth scroll for anchor links within the nav
  document.querySelectorAll('nav a[href^="#"]').forEach((anchor) => {
    anchor.addEventListener('click', function onAnchorClick(event) {
      const href = this.getAttribute('href');
      if (href === '#') {
        return;
      }
      event.preventDefault();
      const target = document.querySelector(href);
      if (target) {
        target.scrollIntoView({ behavior: 'smooth' });
      }
    });
  });

  // Sticky nav: add 'scrolled' class when scrollY > 32
  function handleScroll() {
    if (window.scrollY > 32) {
      navbar.classList.add('scrolled');
    } else {
      navbar.classList.remove('scrolled');
    }
  }

  window.addEventListener('scroll', handleScroll);

  // Hamburger toggle for mobile nav
  if (hamburger && navList) {
    hamburger.addEventListener('click', function onHamburgerClick() {
      const isExpanded = hamburger.getAttribute('aria-expanded') === 'true';
      hamburger.setAttribute('aria-expanded', String(!isExpanded));
      navList.classList.toggle('open');
    });
  }

  // Close mobile menu on Escape key
  document.addEventListener('keydown', function onKeyDown(event) {
    if (event.key === 'Escape' && hamburger && navList) {
      hamburger.setAttribute('aria-expanded', 'false');
      navList.classList.remove('open');
    }
  });
}
