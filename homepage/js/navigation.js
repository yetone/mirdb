/**
 * Navigation Module
 * Owner: Scenario 6 - Navigation & Header
 */

export function initNavigation() {
  const toggle = document.getElementById('nav-toggle');

  if (toggle) {
    toggle.addEventListener('click', () => {
      const navLinks = document.querySelector('.nav-links');
      if (navLinks) {
        navLinks.classList.toggle('mobile-open');
      }
    });
  }

  // Smooth scroll for navigation links
  document.querySelectorAll('a[href^="#"]').forEach(link => {
    link.addEventListener('click', (e) => {
      const targetId = link.getAttribute('href');
      if (targetId && targetId !== '#') {
        e.preventDefault();
        scrollToSection(targetId.slice(1));
      }
    });
  });
}

export function openMobileMenu() {
  const navLinks = document.querySelector('.nav-links');
  if (navLinks) {
    navLinks.classList.add('mobile-open');
  }
}

export function closeMobileMenu() {
  const navLinks = document.querySelector('.nav-links');
  if (navLinks) {
    navLinks.classList.remove('mobile-open');
  }
}

export function scrollToSection(sectionId) {
  const section = document.getElementById(sectionId);
  if (section) {
    section.scrollIntoView({ behavior: 'smooth' });
  }
}
