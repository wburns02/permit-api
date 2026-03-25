const CACHE_NAME = 'permitlookup-v1';
const STATIC_ASSETS = [
    '/',
    '/static/manifest.json',
    '/static/icon-192.png',
    '/static/icon-512.png'
];

// Install — cache shell assets
self.addEventListener('install', e => {
    e.waitUntil(
        caches.open(CACHE_NAME)
            .then(cache => cache.addAll(STATIC_ASSETS))
            .then(() => self.skipWaiting())
    );
});

// Activate — clean old caches
self.addEventListener('activate', e => {
    e.waitUntil(
        caches.keys().then(keys =>
            Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
        ).then(() => self.clients.claim())
    );
});

// Fetch — network-first for API, cache-first for static
self.addEventListener('fetch', e => {
    const url = new URL(e.request.url);

    // Skip non-GET requests
    if (e.request.method !== 'GET') return;

    // API calls (/v1/*) — network first, fall back to cache
    if (url.pathname.startsWith('/v1/')) {
        e.respondWith(
            fetch(e.request)
                .then(resp => {
                    const clone = resp.clone();
                    caches.open(CACHE_NAME).then(cache => cache.put(e.request, clone));
                    return resp;
                })
                .catch(() => caches.match(e.request))
        );
        return;
    }

    // Static assets — cache first, fall back to network
    e.respondWith(
        caches.match(e.request).then(cached => {
            if (cached) return cached;
            return fetch(e.request).then(resp => {
                const clone = resp.clone();
                caches.open(CACHE_NAME).then(cache => cache.put(e.request, clone));
                return resp;
            });
        })
    );
});
