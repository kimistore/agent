/* Kimistore site — PostHog analytics.
 *
 * Loads PostHog and records pageviews plus interactions. PostHog autocaptures
 * clicks on links and buttons, so navigation, the mobile menu, and the code
 * tabs are recorded without any wiring here. The events below add names for the
 * actions worth counting, because an autocaptured click names the element and a
 * named event says what the visitor was trying to do.
 *
 * The project API key is public by design. It grants write-only access to
 * ingest, so it ships in the page and can be seen by anyone who views source.
 * Nothing here grants access to the PostHog project itself.
 *
 * This project is on PostHog EU Cloud, so events and session recordings stay in
 * the EU. api_host is the ingest endpoint; ui_host is where the PostHog app
 * lives, which keeps links in error messages and banners pointing at the right
 * place.
 */

(function () {
  'use strict';

  // Do Not Track. PostHog supports the browser preference, and honouring it
  // costs nothing here because the site has no other analytics. A visitor who
  // opts out is recorded by nobody rather than by us.
  if (navigator.doNotTrack === '1' || window.doNotTrack === '1') {
    return;
  }

  /* The loader is PostHog's own snippet, rewritten to be readable.
   *
   * It installs a stub on window that records calls instead of performing them,
   * then injects the real library from api_host. The stub drains once the
   * library arrives, which is why the events at the bottom of this file can be
   * written straight after init instead of inside a loaded callback.
   *
   * This is PostHog's published loader logic, unminified. The published form is
   * a single line of comma expressions whose parentheses are not checkable by
   * eye; an error in it fails silently in a browser console nobody reads.
   *
   * The queued method list must stay complete. A missing name means that call
   * is dropped on every page load, with no error anywhere.
   */
  (function (doc, stub) {
    if (stub.__SV) {
      return; // Already initialised on this page.
    }

    window.posthog = stub;
    stub._i = [];

    stub.init = function (token, config, instanceName) {
      // Inject the real library.
      var script = doc.createElement('script');
      script.type = 'text/javascript';
      script.crossOrigin = 'anonymous';
      script.async = true;
      script.src = config.api_host + '/static/array.js';

      var first = doc.getElementsByTagName('script')[0];
      first.parentNode.insertBefore(script, first);

      // Calls land on the instance, which is the stub itself unless a name is
      // given. A dotted name such as "people" becomes a sub-object.
      var target = instanceName === undefined ? stub : (stub[instanceName] = stub[instanceName] || []);
      target.people = target.people || [];

      var queue = function (obj, name) {
        obj[name] = function () {
          obj.push([name].concat(Array.prototype.slice.call(arguments)));
        };
      };

      // Queues a call on a sub-object, such as people.set. The sub-object name
      // is resolved here rather than inside the closure, for the reason given
      // at the call site.
      var queueDotted = function (obj, group, name) {
        obj[group] = obj[group] || [];
        obj[group][name] = function () {
          obj[group].push([name].concat(Array.prototype.slice.call(arguments)));
        };
      };

      var methods = [
        'capture', 'identify', 'alias',
        'people.set', 'people.set_once',
        'set_config', 'register', 'register_once', 'unregister',
        'opt_out_capturing', 'has_opted_out_capturing', 'opt_in_capturing',
        'reset', 'group',
        'isFeatureEnabled', 'onFeatureFlags',
        'getFeatureFlag', 'getFeatureFlagResult', 'reloadFeatureFlags',
        'updateEarlyAccessFeatureEnrollment', 'getEarlyAccessFeatures',
        'getActiveMatchingSurveys', 'getSurveys',
      ];

      for (var i = 0; i < methods.length; i++) {
        var parts = methods[i].split('.');
        if (parts.length === 2) {
          // The name is passed as an argument rather than read from parts
          // inside the closure. `parts` is function-scoped, so every closure
          // built in this loop would otherwise see the final iteration's
          // value, and people.set would queue as people.set_once.
          queueDotted(target, parts[0], parts[1]);
        } else {
          queue(target, parts[0]);
        }
      }

      // The library reads this on load to find out what to initialise.
      stub._i.push([token, config, instanceName]);
    };

    stub.__SV = 1;
  })(document, window.posthog || []);

  window.posthog.init('phc_hmODge5gIlXTb85mutArWS7cmuI1Hrb02j5SdRKAxDa', {
    api_host: 'https://eu.i.posthog.com',
    ui_host: 'https://eu.posthog.com',
    // The recommended behaviour set. This pins the defaults to a tested
    // combination rather than whatever the library ships next, so a library
    // update cannot silently change what the site records.
    defaults: '2026-05-30',
    // Pageviews and pageleaves are the point of the site. Both stay on.
    capture_pageview: true,
    capture_pageleave: true,
    // Session recording stays off. The site has no form and no account, so a
    // recording adds cost and privacy exposure without answering a question
    // this site needs answered. Turn it on in PostHog project settings rather
    // than in code if a recording is ever wanted.
    disable_session_recording: true,
    loaded: function (posthog) {
      // Site-wide properties, sent with every event. Section reads tell us
      // where a visitor loses interest without tagging every link by hand.
      posthog.register({
        site: 'kimistore.eu',
        page_type: 'marketing',
      });
    },
  });

  /* Named events for the actions that matter.
   *
   * The site's job is to send a reader to the repository, so an outbound click
   * on GitHub is the conversion. It is worth counting by name because it
   * separates "read about the project" from "went to get it".
   *
   * The delegation below handles links added to the page later. The site is
   * static, but binding directly would miss any link a future edit adds inside
   * a container rendered by script.js.
   */
  document.addEventListener(
    'click',
    function (event) {
      var link = event.target.closest ? event.target.closest('a[href]') : null;
      if (!link) {
        return;
      }

      var href = link.getAttribute('href') || '';

      if (href.indexOf('https://github.com/') === 0) {
        window.posthog.capture('github_link_clicked', {
          destination: href,
          label: (link.textContent || '').trim(),
        });
        return;
      }

      // A code tab switch changes what the reader sees, so it marks real
      // interest in the quickstart rather than a stray click.
      if (link.classList && link.classList.contains('tab')) {
        window.posthog.capture('code_tab_switched', {
          label: (link.textContent || '').trim(),
        });
      }
    },
    true
  );

  // A visitor who reaches the quickstart section has read far enough to try it.
  var quickstart = document.getElementById('quickstart');
  if (quickstart && 'IntersectionObserver' in window) {
    var recorded = false;
    new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          // The callback runs on every change to visibility, so scrolling past
          // the section and back reports it twice without this guard. One
          // pageview should yield one event.
          if (entry.isIntersecting && !recorded) {
            recorded = true;
            window.posthog.capture('quickstart_reached');
          }
        });
      },
      { threshold: 0.5 }
    ).observe(quickstart);
  }
})();
