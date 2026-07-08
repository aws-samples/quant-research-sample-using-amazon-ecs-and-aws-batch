import { AnimatePresence, motion } from "motion/react";
import { Check } from "lucide-react";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import { stackCount } from "@/lib/defaults";
import { AnimatedNumber } from "@/components/fx/AnimatedNumber";
import { cn } from "@/lib/cn";

/**
 * The full configuration detail — stack count, deployment type, AZ, expiry,
 * FSx, and the option toggles. Rendered inside the collapsible ConfigSummary
 * bar at the top of the Deploy view (the standalone Config tab was removed).
 */
export function ConfigDetail() {
  const config = useDeploymentStore((s) => s.config);
  const validated = useDeploymentStore((s) => s.validated);
  const n = stackCount(config);

  const toggles: { label: string; on: boolean }[] = [
    { label: "FSx for Lustre", on: config.app_with_fsx },
    { label: "CodePipeline", on: config.app_with_codepipeline },
    { label: "S3 Express", on: config.app_with_s3express },
  ];

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-start justify-between">
        <div>
          <div className="text-xs uppercase tracking-wide text-text-lo">Stacks to deploy</div>
          <div className="flex items-baseline gap-1.5">
            <AnimatedNumber value={n} className="text-4xl font-semibold text-aws-orange" />
            <span className="text-sm text-text-mid">stacks</span>
          </div>
        </div>
        <AnimatePresence>
          {validated && (
            <motion.div
              initial={{ opacity: 0, scale: 0.6, y: -4 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0 }}
              transition={{ type: "spring", stiffness: 380, damping: 20 }}
              className="flex items-center gap-1 rounded-full border border-aws-green/40 bg-aws-green/10 px-2.5 py-1 text-[12px] font-medium text-aws-green"
            >
              <Check size={13} strokeWidth={3} /> validated
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* Deployment type */}
      <Field label="Deployment type">
        <span className="font-mono text-[13px] text-text-hi">{config.batch.deployment_type}</span>
      </Field>

      <Field label="Availability zone">
        <span className="font-mono text-[13px] text-text-hi">{config.availability_zone.name}</span>
      </Field>

      <Field label="S3 object expiry">
        <span className="font-mono text-[13px] text-text-hi">
          {config.s3.object_expiration_in_days} days
        </span>
      </Field>

      {config.app_with_fsx && (
        <Field label="FSx capacity">
          <span className="font-mono text-[13px] text-text-hi">
            {config.fsx.storage_capacity_gib} GiB · {config.fsx.deployment_type}
          </span>
        </Field>
      )}

      <div className="mt-1 grid grid-cols-1 gap-2">
        {toggles.map((t, i) => (
          <Toggle key={t.label} label={t.label} on={t.on} delay={i * 0.05} />
        ))}
      </div>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <motion.div
      layout
      initial={{ opacity: 0, x: -8 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ type: "spring", stiffness: 300, damping: 26 }}
      className="flex items-center justify-between rounded-xl border border-white/8 bg-surface-1 px-3 py-2.5"
    >
      <span className="text-[12.5px] text-text-mid">{label}</span>
      {children}
    </motion.div>
  );
}

function Toggle({ label, on, delay }: { label: string; on: boolean; delay: number }) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay, type: "spring", stiffness: 300, damping: 24 }}
      className={cn(
        "flex items-center justify-between rounded-xl border px-3 py-2.5 transition-colors",
        on ? "border-aws-teal/40 bg-aws-teal/10" : "border-white/8 bg-surface-1",
      )}
    >
      <span className={cn("text-[12.5px]", on ? "text-text-hi" : "text-text-mid")}>{label}</span>
      <span
        className={cn(
          "relative h-5 w-9 rounded-full transition-colors",
          on ? "bg-aws-teal" : "bg-white/15",
        )}
      >
        <motion.span
          layout
          transition={{ type: "spring", stiffness: 500, damping: 30 }}
          className={cn(
            "absolute top-0.5 h-4 w-4 rounded-full bg-white",
            on ? "left-[18px]" : "left-0.5",
          )}
        />
      </span>
    </motion.div>
  );
}
