import { useEffect } from "react";
import confetti from "canvas-confetti";
import { useReducedMotion } from "motion/react";

const AWS_COLORS = ["#FF9900", "#01A88D", "#2BC253", "#0972D3"];

/**
 * Fires exactly once when `fire` flips true (the success moment).
 * Honors prefers-reduced-motion by skipping the burst.
 */
export function Confetti({ fire }: { fire: boolean }) {
  const reduce = useReducedMotion();

  useEffect(() => {
    if (!fire || reduce) return;
    const end = Date.now() + 700;
    const shoot = () => {
      confetti({
        particleCount: 6,
        angle: 60,
        spread: 60,
        origin: { x: 0, y: 0.7 },
        colors: AWS_COLORS,
        scalar: 0.9,
      });
      confetti({
        particleCount: 6,
        angle: 120,
        spread: 60,
        origin: { x: 1, y: 0.7 },
        colors: AWS_COLORS,
        scalar: 0.9,
      });
      if (Date.now() < end) requestAnimationFrame(shoot);
    };
    // One satisfying central pop + side streamers.
    confetti({
      particleCount: 90,
      spread: 100,
      startVelocity: 42,
      origin: { y: 0.55 },
      colors: AWS_COLORS,
    });
    shoot();
  }, [fire, reduce]);

  return null;
}
