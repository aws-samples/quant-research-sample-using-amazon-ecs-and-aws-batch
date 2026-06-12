import { motion } from "motion/react";
import { Rocket, Settings2, X } from "lucide-react";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import type { ConfigOverride } from "@/lib/client/types";
import { stackCount } from "@/lib/defaults";
import { confirmDeploy } from "@/lib/driver";
import { AnimatedNumber } from "@/components/fx/AnimatedNumber";
import { cn } from "@/lib/cn";

const DEPLOYMENT_TYPES: { value: ConfigOverride["batch"]["deployment_type"]; label: string }[] = [
  { value: "SINGLE_NODE", label: "Single-node · CPU" },
  { value: "MULTI_NODE", label: "Multi-node · GPU" },
  { value: "ALL", label: "All · CPU + GPU" },
];

const AZ_OPTIONS = ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d"];
const FSX_CAPACITIES = [1200, 2400, 4800, 7200];

/**
 * Inline, client-side configuration form rendered in the chat. Edits the live
 * proposed config in place — predefined options become dropdowns, booleans
 * become toggles, scalars become number inputs — and the canvas Config view +
 * architecture diagram update reactively as the operator tweaks. A confirm /
 * cancel footer kicks off (or aborts) the deployment without leaving the chat.
 */
export function InlineConfigForm({ willFail }: { willFail?: boolean }) {
  const config = useDeploymentStore((s) => s.config);
  const patch = useDeploymentStore((s) => s.patchConfig);
  const addSystem = useDeploymentStore((s) => s.addSystemMessage);
  const n = stackCount(config);

  return (
    <motion.div
      initial={{ opacity: 0, y: 8, scale: 0.98 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ type: "spring", stiffness: 260, damping: 24 }}
      className="overflow-hidden rounded-2xl border border-white/10 bg-surface-0/60 backdrop-blur-sm"
    >
      <div className="flex items-center gap-1.5 border-b border-white/8 px-3 py-2 text-[10.5px] font-semibold uppercase tracking-widest text-aws-teal">
        <Settings2 className="h-3.5 w-3.5" />
        Deployment options
      </div>

      <div className="flex flex-col gap-2.5 p-3">
        <SelectField
          label="Deployment type"
          value={config.batch.deployment_type}
          options={DEPLOYMENT_TYPES}
          onChange={(v) => patch({ batch: { deployment_type: v } })}
        />

        <SelectField
          label="Availability zone"
          value={config.availability_zone.name}
          options={AZ_OPTIONS.map((v) => ({ value: v, label: v }))}
          onChange={(v) => patch({ availability_zone: { name: v } })}
        />

        <NumberField
          label="S3 object expiry"
          suffix="days"
          value={config.s3.object_expiration_in_days}
          min={1}
          max={3650}
          onChange={(v) => patch({ s3: { object_expiration_in_days: v } })}
        />

        <ToggleField
          label="FSx for Lustre"
          on={config.app_with_fsx}
          onChange={(on) => patch({ app_with_fsx: on })}
        />
        {config.app_with_fsx && (
          <SelectField
            label="FSx capacity"
            value={String(config.fsx.storage_capacity_gib)}
            options={FSX_CAPACITIES.map((v) => ({ value: String(v), label: `${v} GiB` }))}
            onChange={(v) =>
              patch({ fsx: { ...config.fsx, storage_capacity_gib: Number(v) } })
            }
          />
        )}
        <ToggleField
          label="CodePipeline (CI/CD)"
          on={config.app_with_codepipeline}
          onChange={(on) => patch({ app_with_codepipeline: on })}
        />
        <ToggleField
          label="S3 Express One Zone"
          on={config.app_with_s3express}
          onChange={(on) => patch({ app_with_s3express: on })}
        />
      </div>

      {/* Confirm / cancel footer */}
      <div className="flex items-center gap-2 border-t border-white/8 bg-surface-1/40 px-3 py-2.5">
        <div className="mr-auto text-[11.5px] text-text-mid">
          <AnimatedNumber value={n} className="font-semibold text-aws-orange" /> stacks
        </div>
        <button
          onClick={() => addSystem("Deployment cancelled.")}
          className="flex items-center justify-center gap-1.5 rounded-lg border border-white/12 px-3 py-1.5 text-[12.5px] font-medium text-text-mid transition-colors hover:text-text-hi"
        >
          <X size={13} /> Cancel
        </button>
        <button
          onClick={() => void confirmDeploy(Boolean(willFail))}
          className="flex items-center justify-center gap-1.5 rounded-lg bg-aws-orange px-3.5 py-1.5 text-[12.5px] font-semibold text-anchor transition-colors hover:bg-aws-orange-2"
        >
          <Rocket size={13} /> Deploy
        </button>
      </div>
    </motion.div>
  );
}

function FieldShell({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="flex items-center justify-between gap-3">
      <span className="text-[12.5px] text-text-mid">{label}</span>
      {children}
    </label>
  );
}

function SelectField<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: T;
  options: { value: T; label: string }[];
  onChange: (v: T) => void;
}) {
  return (
    <FieldShell label={label}>
      <div className="relative">
        <select
          value={value}
          onChange={(e) => onChange(e.target.value as T)}
          className={cn(
            "appearance-none rounded-lg border border-white/12 bg-surface-1 py-1.5 pl-3 pr-7",
            "text-[12.5px] font-medium text-text-hi transition-colors",
            "hover:border-white/20 focus:border-aws-orange/60 focus:outline-none",
          )}
        >
          {options.map((o) => (
            <option key={o.value} value={o.value} className="bg-surface-1 text-text-hi">
              {o.label}
            </option>
          ))}
        </select>
        <span className="pointer-events-none absolute right-2 top-1/2 -translate-y-1/2 text-[9px] text-text-lo">
          ▾
        </span>
      </div>
    </FieldShell>
  );
}

function NumberField({
  label,
  value,
  suffix,
  min,
  max,
  onChange,
}: {
  label: string;
  value: number;
  suffix?: string;
  min?: number;
  max?: number;
  onChange: (v: number) => void;
}) {
  return (
    <FieldShell label={label}>
      <div className="flex items-center gap-1.5 rounded-lg border border-white/12 bg-surface-1 px-2.5 py-1 focus-within:border-aws-orange/60">
        <input
          type="number"
          value={value}
          min={min}
          max={max}
          onChange={(e) => {
            const v = Number(e.target.value);
            if (Number.isFinite(v)) onChange(Math.max(min ?? -Infinity, Math.min(max ?? Infinity, v)));
          }}
          className="w-14 bg-transparent text-right text-[12.5px] font-medium text-text-hi focus:outline-none"
        />
        {suffix && <span className="text-[11px] text-text-lo">{suffix}</span>}
      </div>
    </FieldShell>
  );
}

function ToggleField({
  label,
  on,
  onChange,
}: {
  label: string;
  on: boolean;
  onChange: (on: boolean) => void;
}) {
  return (
    <FieldShell label={label}>
      <button
        type="button"
        role="switch"
        aria-checked={on}
        onClick={() => onChange(!on)}
        className={cn(
          "relative h-5 w-9 shrink-0 rounded-full transition-colors",
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
      </button>
    </FieldShell>
  );
}
