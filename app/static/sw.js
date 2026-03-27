const CACHE_NAME = 'permitlookup-v6';
const STATIC_ASSETS = [
    '/static/manifest.json',
    '/static/icon-192.png',
    '/static/icon-512.png'
];

// Install — cache only icons/manifest, NOT the HTML page
self.addEventListener('install', e => {
    e.waitUntil(
        caches.open(CACHE_NAME)
            .then(cache => cache.addAll(STATIC_ASSETS))
            .then(() => self.skipWaiting())
    );
});

// Activate — clean ALL old caches aggressively
self.addEventListener('activate', e => {
    e.waitUntil(
        caches.keys().then(keys =>
            Promise.all(keys.map(k => caches.delete(k)))
        ).then(() => self.clients.claim())
    );
});

// Fetch — ALWAYS go to network for HTML and API, cache only static assets
self.addEventListener('fetch', e => {
    const url = new URL(e.request.url);
    if (e.request.method !== 'GET') return;

    // API calls and HTML — always network, never cache
    if (url.pathname === '/' || url.pathname.startsWith('/v1/')) {
        return;  // Let the browser handle it normally (network only)
    }

    // Static assets only — cache-first
    if (url.pathname.startsWith('/static/')) {
        e.respondWith(
            caches.match(e.request).then(cached => {
                if (cached) return cached;
                return fetch(e.request);
            })
        );
    }
});
