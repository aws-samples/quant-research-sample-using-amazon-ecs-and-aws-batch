import { motion, useReducedMotion } from "motion/react";

/**
 * Subtle animated radial gradients + faint grid behind the whole app.
 * Pure transform/opacity animation; degrades to a static gradient when reduced.
 */
export function GradientBackdrop() {
  const reduce = useReducedMotion();
  return (
    <div className="pointer-events-none fixed inset-0 -z-10 overflow-hidden bg-surface-0">
      {/* faint grid */}
      <div
        className="absolute inset-0 opacity-[0.5]"
        style={{
          backgroundImage:
            "linear-gradient(rgba(255,255,255,0.035) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.035) 1px, transparent 1px)",
          backgroundSize: "44px 44px",
          maskImage: "radial-gradient(ellipse 80% 70% at 50% 35%, black, transparent 75%)",
        }}
      />
      <motion.div
        aria-hidden
        className="absolute -left-1/4 -top-1/4 h-[70vh] w-[70vh] rounded-full blur-3xl"
        style={{ background: "radial-gradient(circle, rgba(255,153,0,0.16), transparent 60%)" }}
        animate={reduce ? undefined : { x: [0, 60, 0], y: [0, 40, 0] }}
        transition={{ duration: 22, repeat: Infinity, ease: "easeInOut" }}
      />
      <motion.div
        aria-hidden
        className="absolute -right-1/4 top-1/3 h-[60vh] w-[60vh] rounded-full blur-3xl"
        style={{ background: "radial-gradient(circle, rgba(1,168,141,0.16), transparent 60%)" }}
        animate={reduce ? undefined : { x: [0, -50, 0], y: [0, -30, 0] }}
        transition={{ duration: 26, repeat: Infinity, ease: "easeInOut" }}
      />
      <motion.div
        aria-hidden
        className="absolute bottom-0 left-1/3 h-[50vh] w-[50vh] rounded-full blur-3xl"
        style={{ background: "radial-gradient(circle, rgba(9,114,211,0.12), transparent 60%)" }}
        animate={reduce ? undefined : { x: [0, 40, 0], y: [0, -40, 0] }}
        transition={{ duration: 30, repeat: Infinity, ease: "easeInOut" }}
      />
    </div>
  );
}
