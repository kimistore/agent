/* Kimistore site — progressive enhancement only.
   The page is fully readable with JS disabled. */

(function () {
  'use strict';

  document.documentElement.classList.remove('no-js');

  var reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  /* ---------------- Sticky nav state ---------------- */
  var nav = document.getElementById('nav');
  var onScroll = function () {
    nav.classList.toggle('is-stuck', window.scrollY > 8);
  };
  onScroll();
  window.addEventListener('scroll', onScroll, { passive: true });

  /* ---------------- Mobile menu ---------------- */
  var toggle = document.getElementById('navToggle');
  var menu = document.getElementById('mobileMenu');

  var setMenu = function (open) {
    toggle.setAttribute('aria-expanded', String(open));
    menu.classList.toggle('is-open', open);
  };

  toggle.addEventListener('click', function () {
    setMenu(toggle.getAttribute('aria-expanded') !== 'true');
  });

  menu.addEventListener('click', function (e) {
    if (e.target.tagName === 'A') setMenu(false);
  });

  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && toggle.getAttribute('aria-expanded') === 'true') {
      setMenu(false);
      toggle.focus();
    }
  });

  window.addEventListener('resize', function () {
    if (window.innerWidth > 1020) setMenu(false);
  });

  /* ---------------- Scroll reveal ---------------- */
  var reveals = Array.prototype.slice.call(document.querySelectorAll('.reveal'));

  if (reduced || !('IntersectionObserver' in window)) {
    reveals.forEach(function (el) { el.classList.add('is-in'); });
  } else {
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry, i) {
        if (!entry.isIntersecting) return;
        // stagger siblings that enter in the same frame
        var delay = Math.min(i * 70, 280);
        setTimeout(function () { entry.target.classList.add('is-in'); }, delay);
        io.unobserve(entry.target);
      });
    }, { rootMargin: '0px 0px -8% 0px', threshold: 0.08 });

    reveals.forEach(function (el) { io.observe(el); });
  }

  /* ---------------- Code tabs ---------------- */
  var tabGroups = document.querySelectorAll('.code-block__tabs');

  Array.prototype.forEach.call(tabGroups, function (group) {
    var block = group.closest('.code-block');
    var tabs = Array.prototype.slice.call(group.querySelectorAll('.tab'));

    var select = function (tab) {
      tabs.forEach(function (t) {
        var active = t === tab;
        t.classList.toggle('is-active', active);
        t.setAttribute('aria-selected', String(active));
      });
      Array.prototype.forEach.call(block.querySelectorAll('.code-block__panel'), function (p) {
        p.classList.toggle('is-active', p.dataset.panel === tab.dataset.tab);
      });
    };

    tabs.forEach(function (tab, i) {
      tab.addEventListener('click', function () { select(tab); });
      // keyboard: arrow keys move between tabs
      tab.addEventListener('keydown', function (e) {
        var next = null;
        if (e.key === 'ArrowRight') next = tabs[(i + 1) % tabs.length];
        if (e.key === 'ArrowLeft')  next = tabs[(i - 1 + tabs.length) % tabs.length];
        if (e.key === 'Home')       next = tabs[0];
        if (e.key === 'End')        next = tabs[tabs.length - 1];
        if (!next) return;
        e.preventDefault();
        next.focus();
        select(next);
      });
    });
  });

  /* ---------------- Typing loop in hero terminal ---------------- */
  var demo = document.getElementById('typeDemo');
  if (demo && !reduced) {
    var full = demo.innerHTML;
    demo.innerHTML = '';

    var cursor = document.createElement('span');
    cursor.textContent = '▍';
    cursor.style.color = '#34d399';

    // Fast-forward through the static text, then leave it alone.
    var hold = setTimeout(function () {
      demo.innerHTML = full;
      demo.appendChild(cursor);
    }, 2400);

    window.addEventListener('pagehide', function () { clearTimeout(hold); });
  }
})();
