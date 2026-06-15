const CACHE = 'the-list-v2'
const PRECACHE = ['/']

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(PRECACHE)).then(() => self.skipWaiting()))
})

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(k => k !== CACHE).map(k => caches.delete(k))
    )).then(() => self.clients.claim())
  )
})

self.addEventListener('fetch', e => {
  // Always go to network for API calls
  if (e.request.url.includes('/api/')) {
    e.respondWith(fetch(e.request))
    return
  }
  // Network-first for the app page (HTML / navigation) so new code always loads.
  // Fall back to cache only if offline.
  if (e.request.mode === 'navigate' || e.request.url.endsWith('/')) {
    e.respondWith(
      fetch(e.request).then(res => {
        if (res.ok) {
          const clone = res.clone()
          caches.open(CACHE).then(c => c.put(e.request, clone))
        }
        return res
      }).catch(() => caches.match(e.request).then(r => r || caches.match('/')))
    )
    return
  }
  // Cache-first for static assets (icons, etc.)
  e.respondWith(
    caches.match(e.request).then(r => r || fetch(e.request).then(res => {
      if (res.ok && e.request.method === 'GET') {
        const clone = res.clone()
        caches.open(CACHE).then(c => c.put(e.request, clone))
      }
      return res
    }))
  )
})
