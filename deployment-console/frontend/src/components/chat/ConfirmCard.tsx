import { motion } from "motion/react";
import { Rocket, X } from "lucide-react";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import { stackCount } from "@/lib/defaults";
import { confirmDeploy } from "@/lib/driver";
import { AnimatedNumber } from "@/components/fx/AnimatedNumber";

/** Distinct confirm card rendered before a deployment kicks off. */
export function ConfirmCard() {
  const config = useDeploymentStore((s) => s.config);
  const willFail = useDeploymentStore((s) => s.pendingWillFail);
  const addSystem = useDeploymentStore((s) => s.addSystemMessage);
  const n = stackCount(config);

  return (
    <motion.div
      initial={{ opacity: 0, y: 8, scale: 0.97 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ type: "spring", stiffness: 280, damping: 22 }}
      className="rounded-2xl border border-aws-orange/30 bg-gradient-to-b from-aws-orange/10 to-transparent p-3.5"
    >
      <div className="text-[13px] font-semibold text-text-hi">Ready to deploy</div>
      <div className="mt-1 text-[12.5px] text-text-mid">
        This will run <span className="font-mono text-aws-orange">cdk deploy</span> for{" "}
        <AnimatedNumber value={n} className="font-semibold text-text-hi" /> stacks via CodeBuild.
      </div>
      <div className="mt-3 flex gap-2">
        <button
          onClick={() => void confirmDeploy(willFail)}
          className="flex flex-1 items-center justify-center gap-1.5 rounded-lg bg-aws-orange px-3 py-2 text-[13px] font-semibold text-anchor transition-colors hover:bg-aws-orange-2"
        >
          <Rocket size={14} /> Deploy
        </button>
        <button
          onClick={() => addSystem("Deployment cancelled.")}
          className="flex items-center justify-center gap-1.5 rounded-lg border border-white/12 px-3 py-2 text-[13px] font-medium text-text-mid transition-colors hover:text-text-hi"
        >
          <X size={14} /> Cancel
        </button>
      </div>
    </motion.div>
  );
}
