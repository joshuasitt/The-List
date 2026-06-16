// Kill switch — this service worker unregisters itself and wipes all caches.
// Replaces the old caching service worker that caused stale blank-screen loads.
self.addEventListener('install', () => self.skipWaiting())

self.addEventListener('activate', e => {
  e.waitUntil((async () => {
    const keys = await caches.keys()
    await Promise.all(keys.map(k => caches.delete(k)))
    await self.registration.unregister()
    const clients = await self.clients.matchAll()
    clients.forEach(c => c.navigate(c.url))
  })())
})

// Always go straight to the network — never serve from cache.
self.addEventListener('fetch', e => {
  e.respondWith(fetch(e.request))
})
