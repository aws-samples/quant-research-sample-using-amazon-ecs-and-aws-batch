# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""The GPU catalogue: every family, instance type and launch-template shape the fleet can build.

Hard-wired on purpose. parameters.json only SELECTS from this catalogue (families, instance
types, regions); what a template attaches, how big its disks are and which image a queue's
job definitions run are properties of the hardware and live here, once.

NAMING  <prefix>-<chip>-<family>-<size>-<gpus>g-<cards>c[-noefa]
  rt-h100-p5-48xl-8g-32c          launch template
  rt-h100-p5-48xl-8g-32c-ce       compute environment
  rt-h100-p5-48xl-8g-32c-queue    job queue
The trailing card count is what the template ATTACHES. It is in the name so a template that
under-provisions the EFA fabric (1 of 32 cards) is visible from its name; such a node runs
collectives over TCP at a fraction of link speed with every health signal green.

This module has no CDK dependency: the raw-EC2 path passes `launch_template_data` straight
to run_instances / create_fleet, so both paths launch from the same description.
"""
import base64
import ipaddress
import json
import pathlib
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

HERE = pathlib.Path(__file__).resolve().parent
OFFERINGS = HERE / "offerings.json"

#: The regions of the recommended open-weight fine-tuning configuration. GPU spot capacity
#: is per AZ and scarce; racing every region that sells the hardware is what makes a whole
#: 8-GPU node obtainable within an hour, so the recommendation is all of them.
RECOMMENDED_REGIONS = (
    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
    "ca-central-1", "sa-east-1",
    "eu-north-1", "eu-central-1", "eu-west-1", "eu-west-2", "eu-west-3",
    "ap-northeast-1", "ap-northeast-2", "ap-northeast-3",
    "ap-south-1", "ap-southeast-1", "ap-southeast-2", "ap-southeast-3",
)

IMAGE_TYPE = "ECS_AL2023_NVIDIA"
AMI_SSM_PARAMETER = "/aws/service/ecs/optimized-ami/amazon-linux-2023/gpu/recommended/image_id"

#: Raw-EC2 nodes carry this tag. The age terminator, the hunter policy and the job role's
#: terminate / SendCommand grants are all scoped to it, never to every instance.
HUNT_TAG_KEY = "rt-gpu:managed-by"
HUNT_TAG_VALUE = "hunt"
TTL_TAG_KEY = "rt-gpu:ttl-hours"

INTERFACE_ENDPOINTS = ("ecr.api", "ecr.dkr", "ecs", "ecs-agent", "ecs-telemetry", "logs",
                       "ssm", "ssmmessages", "ec2messages")

NVME_POOL = "/mnt/nvme"
DATA_DEVICE_NAME = "/dev/sdb"
DATA_MOUNT = f"{NVME_POOL}/ebs-data"
DATA_GIB = 3072
DATA_GIB_CHECKPOINT = 4096
DATA_IOPS = 6000
DATA_THROUGHPUT = 1000

#: Instance types AWS Batch lists in its catalogue but cannot run: Volta has no driver in
#: the ECS_AL2023_NVIDIA AMI, so a compute environment naming it settles INVALID.
BATCH_INCOMPATIBLE = {"*": ("p3dn.24xlarge",), "eu-north-1": ("p5.4xlarge",)}

#: Measured at launch, where it contradicts the EC2 API: p6-b300 advertises EFA everywhere
#: and these regions refuse the interface at run_instances. They get a one-card template.
EFA_REFUSED_REGIONS = {"p6-b300.48xlarge": ("ap-northeast-2", "ap-southeast-3")}

#: Second templates for a row: (cards, efa). The 16-card NON-EFA p6-b300 tests the
#: multi-NIC data path (parallel downloads ride every ENI) without a fabric. It attaches the
#: full card count and says `-noefa` in its name, so it is not an under-provisioned fabric.
EXTRA_SHAPES = {"p6-b300.48xlarge": ((16, False),)}
NOEFA_SUFFIX = "-noefa"


# ------------------------------------------------------------------------------ boot shell
# The application layer's disk: a dedicated gp3 volume mounted INSIDE the NVMe pool, never
# a bigger root. A job that fills the root takes the ECS agent down with it. The guards
# matter: mkfs on the wrong device destroys a running node, so a device qualifies only when
# it is EBS, not the root disk, unpartitioned, unmounted and without a filesystem.
DATA_VOLUME_SETUP = f"""
# --- EBS data volume: the application layer's disk, never the root volume ---
mkdir -p {NVME_POOL}
_root_src=$(findmnt -no SOURCE / 2>/dev/null || echo "")
_root_disk=$(lsblk -no PKNAME "$_root_src" 2>/dev/null || echo "")
[ -z "$_root_disk" ] && _root_disk=$(basename "$_root_src" 2>/dev/null || echo "")
for dev in $(lsblk -d -n -o NAME,MODEL | grep 'Amazon Elastic Block Store' | awk '{{print $1}}'); do
  [ "$dev" = "$_root_disk" ] && continue
  if [ "$(lsblk -n -o NAME /dev/$dev 2>/dev/null | wc -l | tr -d ' ')" != "1" ]; then
    echo "ebs-data: /dev/$dev has partitions, leaving alone"
    continue
  fi
  if [ -n "$(lsblk -n -o MOUNTPOINT /dev/$dev | tr -d ' \\n')" ]; then
    echo "ebs-data: /dev/$dev already mounted, leaving alone"
    continue
  fi
  if blkid -p -o value -s TYPE /dev/$dev >/dev/null 2>&1 \\
     && [ -n "$(blkid -p -o value -s TYPE /dev/$dev 2>/dev/null)" ]; then
    echo "ebs-data: /dev/$dev has a filesystem, mounting as-is"
  else
    mkfs.xfs -f /dev/$dev || continue
  fi
  mkdir -p {DATA_MOUNT}
  mount -o noatime /dev/$dev {DATA_MOUNT} || true
  break
done
df -h {DATA_MOUNT} > /var/log/ebs_data_volume.txt 2>&1
"""

# Instance store joins the same pool. A mounted drive is left alone, a drive with a
# filesystem is mounted as-is, only a raw drive is formatted.
NVME_SETUP = """
mkdir -p /mnt/nvme
for dev in $(lsblk -d -n -o NAME,MODEL | grep 'Amazon EC2 NVMe Instance Storage' | awk '{print $1}'); do
  if [ -n "$(lsblk -n -o MOUNTPOINT /dev/$dev | tr -d ' \\n')" ]; then
    echo "nvme: /dev/$dev already mounted, leaving alone"
    continue
  fi
  if ! blkid -p -o value -s TYPE /dev/$dev >/dev/null 2>&1; then
    mkfs.xfs -f /dev/$dev || continue
  fi
  mkdir -p /mnt/nvme/$dev
  mount -o noatime,nodiscard /dev/$dev /mnt/nvme/$dev \
    || mount -o noatime /dev/$dev /mnt/nvme/$dev || true
done
chmod 777 /mnt/nvme /mnt/nvme/* 2>/dev/null || true
df -h /mnt/nvme/* > /var/log/nvme_pool.txt 2>&1
touch /var/log/nvme_userdata.done
"""

# Docker's image and container layers go to the pool too.
DOCKER_DATA_ROOT = """
first=$(ls -d /mnt/nvme/nvme* 2>/dev/null | head -1)
if [ -n "$first" ]; then
  mkdir -p "$first/docker" /etc/docker
  python3 - "$first/docker" <<'PY' || true
import json, sys
p = "/etc/docker/daemon.json"
try:
    d = json.load(open(p))
except Exception:
    d = {}
d["data-root"] = sys.argv[1]
json.dump(d, open(p, "w"))
PY
  systemctl restart docker || true
fi
"""

# The EFA installer, only where a fabric device is present.
EFA_SETUP_GUARDED = """
if [ -d /sys/class/infiniband ] || [ -n "$(lspci 2>/dev/null | grep -i 'EFA')" ]; then
  curl -fsSL -o /tmp/efa.tar.gz \\
    https://efa-installer.amazonaws.com/aws-efa-installer-latest.tar.gz
  tar -xf /tmp/efa.tar.gz -C /tmp
  cd /tmp/aws-efa-installer && ./efa_installer.sh -y --minimal
  modprobe efa
  fi_info -p efa > /var/log/efa_fi_info.txt 2>&1
  ls -l /dev/infiniband > /var/log/efa_devices.txt 2>&1
  touch /var/log/efa_userdata.done
fi
"""


def mime_userdata(*parts: str) -> str:
    """AWS Batch merges its own part (ECS_CLUSTER) into the template's user data and needs
    multipart/mixed to do it. A bare script blocks the merge: the node boots, never joins the
    cluster, and the job sits RUNNABLE, which looks exactly like no spot capacity. cloud-init
    (the raw-EC2 path) parses multipart natively."""
    body = "#!/bin/bash\nset -x\n" + "".join(parts).replace("#!/bin/bash\n", "").lstrip("\n")
    return (
        'Content-Type: multipart/mixed; boundary="==BOUNDARY=="\n'
        "MIME-Version: 1.0\n"
        "\n"
        "--==BOUNDARY==\n"
        "MIME-Version: 1.0\n"
        'Content-Type: text/x-shellscript; charset="us-ascii"\n'
        "\n"
        f"{body}\n"
        "--==BOUNDARY==--\n"
    )


# -------------------------------------------------------------------------------- families
@dataclass(frozen=True)
class GpuFamily:
    """One GPU chip. Everything a template needs that is not the network-card count."""

    slug: str
    gpu_name: str
    #: P-family types draw on the P spot quota and run multi-node collectives over EFA
    p_family: bool
    #: training image built for this chip: `hopper` (CUDA 12.4, sm_80/sm_90) or `blackwell`
    #: (CUDA 12.8, sm_100). A Hopper image on Blackwell reports zero devices without raising.
    arch: str = "hopper"
    install_efa_driver: bool = False
    data_gib: int = DATA_GIB
    root_gib: int = 500
    root_iops: int = 6000
    root_throughput: int = 600

    def user_data(self, efa: bool) -> str:
        parts = [EFA_SETUP_GUARDED] if (self.install_efa_driver and efa) else []
        # the data volume first, so the pool's chmod covers it too
        parts += [DATA_VOLUME_SETUP, NVME_SETUP, DOCKER_DATA_ROOT]
        return base64.b64encode(mime_userdata(*parts).encode()).decode()

    def block_devices(self) -> List[dict]:
        """Root + the data volume; both are scratch and deleted with the instance."""
        return [
            {"DeviceName": "/dev/xvda",
             "Ebs": {"DeleteOnTermination": True, "VolumeSize": self.root_gib,
                     "VolumeType": "gp3", "Iops": self.root_iops,
                     "Throughput": self.root_throughput, "Encrypted": True}},
            {"DeviceName": DATA_DEVICE_NAME,
             "Ebs": {"DeleteOnTermination": True, "VolumeSize": self.data_gib,
                     "VolumeType": "gp3", "Iops": DATA_IOPS,
                     "Throughput": DATA_THROUGHPUT, "Encrypted": True}},
        ]


# The five families that hold a sharded checkpoint on disk take the larger data volume.
FAMILIES: Dict[str, GpuFamily] = {f.slug: f for f in (
    GpuFamily("a100", "A100", p_family=True, install_efa_driver=True, data_gib=DATA_GIB_CHECKPOINT),
    GpuFamily("h100", "H100", p_family=True, install_efa_driver=True, data_gib=DATA_GIB_CHECKPOINT),
    GpuFamily("h200", "H200", p_family=True, install_efa_driver=True, data_gib=DATA_GIB_CHECKPOINT),
    GpuFamily("b200", "B200", p_family=True, arch="blackwell", install_efa_driver=True,
              data_gib=DATA_GIB_CHECKPOINT),
    GpuFamily("b300", "B300", p_family=True, arch="blackwell", install_efa_driver=True,
              data_gib=DATA_GIB_CHECKPOINT),
    GpuFamily("l4", "L4", p_family=False),
    GpuFamily("l40s", "L40S", p_family=False),
)}


# --------------------------------------------------------------------------- instance types
@dataclass(frozen=True)
class InstanceType:
    """One row, as describe_instance_types reports it. `cards` / `max_efa` / `efa` are EC2
    facts; how many cards a template attaches is decided by `cards_attached`."""

    itype: str
    chip: str
    gpus: int
    gpu_mem_gib: int
    vcpu: int
    ram_gib: int
    cards: int
    max_efa: Optional[int]
    efa: bool

    @property
    def family(self) -> GpuFamily:
        return FAMILIES[self.chip]

    @property
    def fam(self) -> str:
        """`p6-b200.48xlarge` -> `p6` (the chip slug already says b200)"""
        f = self.itype.split(".", 1)[0]
        return f.split("-", 1)[0] if f.startswith("p6-") else f

    @property
    def size(self) -> str:
        """`48xlarge` -> `48xl`, `xlarge` -> `1xl`: Batch names allow no dots"""
        s = self.itype.split(".", 1)[1]
        return {"metal": "mtl", "large": "lg", "xlarge": "1xl"}.get(s, s.replace("xlarge", "xl"))


def _t(itype, chip, gpus, gpu_mem_gib, vcpu, ram_gib, cards, max_efa, efa):
    return InstanceType(itype, chip, gpus, gpu_mem_gib, vcpu, ram_gib, cards, max_efa, efa)


INSTANCE_TYPES: Dict[str, InstanceType] = {t.itype: t for t in (
    _t("p4d.24xlarge", "a100", 8, 320, 96, 1152, 4, 4, True),
    _t("p4de.24xlarge", "a100", 8, 640, 96, 1152, 4, 4, True),
    _t("p5.4xlarge", "h100", 1, 80, 16, 256, 1, 1, True),
    _t("p5.48xlarge", "h100", 8, 640, 192, 2048, 32, 32, True),
    _t("p5e.48xlarge", "h200", 8, 1128, 192, 2048, 32, 32, True),
    _t("p5en.48xlarge", "h200", 8, 1128, 192, 2048, 16, 16, True),
    _t("p6-b200.48xlarge", "b200", 8, 1432, 192, 2048, 8, 8, True),
    # 17 network cards, 16 EFA interfaces: asking for 17 EFA interfaces is rejected
    _t("p6-b300.48xlarge", "b300", 8, 2149, 192, 4096, 17, 16, True),
    _t("g6.xlarge", "l4", 1, 22, 4, 16, 1, None, False),
    _t("g6.2xlarge", "l4", 1, 22, 8, 32, 1, None, False),
    _t("g6.4xlarge", "l4", 1, 22, 16, 64, 1, None, False),
    _t("g6.8xlarge", "l4", 1, 22, 32, 128, 1, 1, True),
    _t("g6.16xlarge", "l4", 1, 22, 64, 256, 1, 1, True),
    _t("g6.12xlarge", "l4", 4, 89, 48, 192, 1, 1, True),
    _t("g6.24xlarge", "l4", 4, 89, 96, 384, 1, 1, True),
    _t("g6.48xlarge", "l4", 8, 179, 192, 768, 1, 1, True),
    _t("gr6.4xlarge", "l4", 1, 22, 16, 128, 1, None, False),
    _t("gr6.8xlarge", "l4", 1, 22, 32, 256, 1, 1, True),
    _t("g6e.xlarge", "l40s", 1, 45, 4, 32, 1, None, False),
    _t("g6e.2xlarge", "l40s", 1, 45, 8, 64, 1, None, False),
    _t("g6e.4xlarge", "l40s", 1, 45, 16, 128, 1, None, False),
    _t("g6e.8xlarge", "l40s", 1, 45, 32, 256, 1, 1, True),
    _t("g6e.16xlarge", "l40s", 1, 45, 64, 512, 1, 1, True),
    _t("g6e.12xlarge", "l40s", 4, 179, 48, 384, 1, 1, True),
    _t("g6e.24xlarge", "l40s", 4, 179, 96, 768, 2, 2, True),
    _t("g6e.48xlarge", "l40s", 8, 358, 192, 1536, 4, 4, True),
)}


# ---------------------------------------------------------------------------------- shapes
def cards_attached(t: InstanceType) -> int:
    """Every card that can carry EFA, bounded by the EFA-interface limit. One card when the
    type has no EFA or the family installs no EFA driver: an EFA device without libfabric
    falls back to TCP silently, and a multi-card node forfeits its public IP."""
    if not t.efa or not t.family.install_efa_driver:
        return 1
    return min(t.cards, t.max_efa or t.cards)


def efa_in(t: InstanceType, region: str) -> bool:
    return (t.efa and t.family.install_efa_driver
            and region not in EFA_REFUSED_REGIONS.get(t.itype, ()))


def attached_in(t: InstanceType, region: str) -> int:
    return cards_attached(t) if efa_in(t, region) else 1


@dataclass(frozen=True)
class Shape:
    """One launch template (and, where Batch can host it, one CE + queue)."""

    itype: InstanceType
    cards: int
    efa: bool
    extra: bool = False

    def name(self, prefix: str) -> str:
        t = self.itype
        suffix = NOEFA_SUFFIX if (self.extra and not self.efa and self.cards > 1) else ""
        return f"{prefix}-{t.chip}-{t.fam}-{t.size}-{t.gpus}g-{self.cards}c{suffix}"

    def ce_name(self, prefix: str) -> str:
        return self.name(prefix) + "-ce"

    def queue_name(self, prefix: str) -> str:
        return self.name(prefix) + "-queue"

    @property
    def arch(self) -> str:
        return self.itype.family.arch


def shapes_in(t: InstanceType, region: str) -> List[Shape]:
    """The primary shape, then any EXTRA_SHAPES of the row."""
    out = [Shape(t, attached_in(t, region), efa_in(t, region))]
    out += [Shape(t, c, e, extra=True) for c, e in EXTRA_SHAPES.get(t.itype, ())]
    return out


def launch_template_data(shape: Shape, security_group_id: str,
                         instance_profile_name: str, tags: Optional[dict] = None) -> dict:
    """EC2-API-shaped template body: no SubnetId (the CE or create_fleet supplies it, so one
    template serves every AZ) and no ImageId (Batch picks the ECS GPU AMI; the raw-EC2 path
    reads AMI_SSM_PARAMETER). Only a one-card template takes a public IP: EC2 rejects one
    alongside several network interfaces."""
    nics = []
    for i in range(shape.cards):
        n = {"DeviceIndex": 0, "NetworkCardIndex": i, "Groups": [security_group_id],
             "DeleteOnTermination": True}
        if shape.efa:
            n["InterfaceType"] = "efa"
        nics.append(n)
    if shape.cards == 1:
        nics[0]["AssociatePublicIpAddress"] = True
    fam = shape.itype.family
    data = {"IamInstanceProfile": {"Name": instance_profile_name},
            "BlockDeviceMappings": fam.block_devices(),
            "NetworkInterfaces": nics,
            "MetadataOptions": {"HttpTokens": "required", "HttpPutResponseHopLimit": 2},
            "UserData": fam.user_data(shape.efa)}
    if tags:
        data["TagSpecifications"] = [{"ResourceType": "instance",
                                      "Tags": [{"Key": k, "Value": v} for k, v in tags.items()]}]
    return data


def shape_tag(t: InstanceType) -> str:
    """Compact JSON describing the hardware, stamped on every node as a tag."""
    return json.dumps({"chip": t.chip, "fam": t.fam, "size": t.size, "gpus": t.gpus,
                       "gpumem": t.gpu_mem_gib, "vcpu": t.vcpu, "vcpu_gpu": t.vcpu // t.gpus,
                       "ram": t.ram_gib, "ram_vcpu": t.ram_gib // t.vcpu,
                       "ram_gpu": t.ram_gib // t.gpus, "cards": t.cards, "efa": t.efa},
                      separators=(",", ":"))


# --------------------------------------------------------------------------------- regions
ENABLED, DISABLED, TEMPLATE_ONLY, ABSENT = "enabled", "disabled", "template-only", "absent"


def load_offerings(path: pathlib.Path = OFFERINGS) -> dict:
    return json.loads(path.read_text())["regions"]


def batch_accepts(itype: str, region: str, offerings: dict) -> bool:
    excluded = set(BATCH_INCOMPATIBLE.get("*", ())) | set(BATCH_INCOMPATIBLE.get(region, ()))
    return itype in offerings.get(region, {}).get("batch", []) and itype not in excluded


def region_state(t: InstanceType, region: str, offerings: dict,
                 exclude_az_ids: Tuple[str, ...] = ()) -> Tuple[str, Tuple[str, ...]]:
    """(state, AZ IDs to place into) for one type in one region.

    enabled        Batch accepts the type and >=1 usable AZ offers it: template + CE + queue
    disabled       Batch accepts it, no usable AZ offers it: template + CE + DISABLED queue
    template-only  offered here, not in Batch's catalogue: template only (raw-EC2 path)
    absent         neither: nothing
    """
    d = offerings.get(region, {})
    azs = tuple(a for a in d.get("types", {}).get(t.itype, ()) if a not in exclude_az_ids)
    if batch_accepts(t.itype, region, offerings):
        return (ENABLED, azs) if azs else (DISABLED, ())
    return (TEMPLATE_ONLY, azs) if azs else (ABSENT, ())


def region_az_ids(region: str, offerings: dict, exclude_az_ids: Tuple[str, ...] = ()) -> List[str]:
    return [a for a in offerings.get(region, {}).get("az_ids", []) if a not in exclude_az_ids]


def subnet_cidrs(vpc_cidr: str, index: int) -> Tuple[str, str]:
    """(public, private) /20 for the index-th AZ ID. The index is a position in the SORTED
    AZ ID list, so an existing subnet's CIDR never moves (a changed CIDR is a replacement)."""
    subnets = list(ipaddress.ip_network(vpc_cidr).subnets(new_prefix=20))
    half = len(subnets) // 2
    if index >= half:
        raise ValueError(f"{vpc_cidr} holds {half} AZs of /20 subnet pairs; AZ {index} does not fit")
    return str(subnets[index]), str(subnets[half + index])


# ---------------------------------------------------------------------------- job sizing
#: Benchmark documents: the whole node less the slice the ECS agent keeps. 1/16 of the
#: vCPUs (180 of 192 on p5.48xlarge places, 192 does not) and 90 % of advertised RAM.
BENCH_VCPU_HEADROOM_FRACTION = 1 / 16
BENCH_MEMORY_FRACTION = 0.90
BENCH_SHM_MIB = 8192
BENCH_TIMEOUT_S = 3 * 3600


def bench_resources(t: InstanceType) -> Dict[str, int]:
    headroom = max(1, int(t.vcpu * BENCH_VCPU_HEADROOM_FRACTION))
    return {"gpus": t.gpus, "vcpu": max(1, t.vcpu - headroom),
            "memory_mib": int(t.ram_gib * 1024 * BENCH_MEMORY_FRACTION)}


#: Fine-tuning / scoring documents, one per (shape, arch). Asks that demonstrably place on
#: the smallest node of each shape; /dev/shm raised for 8 torchrun ranks (NCCL's shared
#: memory transport fails as a bus error or an init hang at docker's 64 MiB default).
SFT_SHAPES = {
    "1gpu": {"gpu": 1, "vcpu": 16, "memory": 240000, "shm_mib": 0, "arches": ("hopper", "blackwell")},
    "8gpu": {"gpu": 8, "vcpu": 180, "memory": 1900000, "shm_mib": 65536, "arches": ("hopper", "blackwell")},
    "1gpu-g": {"gpu": 1, "vcpu": 8, "memory": 28000, "shm_mib": 0, "arches": ("hopper",)},
    "2gpu-g": {"gpu": 2, "vcpu": 16, "memory": 56000, "shm_mib": 0, "arches": ("hopper",)},
}
SFT_TIMEOUT_S = 24 * 3600


def bench_job_definition_name(shape: Shape, prefix: str) -> str:
    return f"bench-gpu-{shape.name(prefix)[len(prefix) + 1:]}-{shape.arch}-jd"


def sft_job_definition_name(sft_shape: str, arch: str) -> str:
    return f"sft-{sft_shape}-{arch}-jd"


# ------------------------------------------------------------------------------- selection
def select(families=(), instance_types=()) -> List[str]:
    """The instance types a preset deploys: every type of the named families, plus any named
    individually. Unknown names fail synth rather than silently deploying less."""
    unknown_f = sorted(set(families) - set(FAMILIES))
    unknown_t = sorted(set(instance_types) - set(INSTANCE_TYPES))
    if unknown_f or unknown_t:
        raise ValueError(f"not in the GPU catalogue: families {unknown_f}, types {unknown_t}; "
                         f"known families {sorted(FAMILIES)}")
    chosen = {t for t, it in INSTANCE_TYPES.items() if it.chip in set(families)}
    chosen |= set(instance_types)
    return [t for t in INSTANCE_TYPES if t in chosen]
