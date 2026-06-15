export const cache: Record<string, { data: any; timestamp: number; ttl: number }> = {};

// Garbage Collection interval to prevent memory leak
setInterval(() => {
  const now = Date.now();
  for (const key in cache) {
    if (now - cache[key].timestamp > cache[key].ttl) {
      delete cache[key];
    }
  }
}, 60000); // 1 minute

export function getCachedData(key: string, ttl: number) {
  const cached = cache[key];
  if (cached && Date.now() - cached.timestamp < ttl) {
    return cached.data;
  }
  return null;
}

export function setCachedData(key: string, data: any, ttl: number = 300000) {
  cache[key] = { data, timestamp: Date.now(), ttl };
}

export function clearCache() {
  for (const key in cache) {
    delete cache[key];
  }
}
