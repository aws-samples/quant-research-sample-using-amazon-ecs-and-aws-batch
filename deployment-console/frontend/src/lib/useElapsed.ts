import { useEffect, useState } from "react";

/**
 * Returns elapsed seconds between start and (finish ?? now), ticking while live.
 * Returns 0 when no build has started.
 */
export function useElapsed(startedAt?: number, finishedAt?: number): number {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    if (!startedAt || finishedAt) return;
    const t = setInterval(() => setNow(Date.now()), 100);
    return () => clearInterval(t);
  }, [startedAt, finishedAt]);

  if (!startedAt) return 0;
  return Math.max(0, ((finishedAt ?? now) - startedAt) / 1000);
}
