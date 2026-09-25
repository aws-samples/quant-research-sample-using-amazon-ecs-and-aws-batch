# Fleet runtime image: the model downloader and the per-shape benchmark, baked in.
#
# Every GPU job in the fleet starts FROM this image, so the weight transport (region-local
# replica resolution, multi-card fan-out, NVMe pool) and the EFA userspace are the same on
# every node. The bench job definitions run it directly; training images build FROM it.
#
#   routes (argv[1], see gpu_fleet_entry.py):
#     bench_gpu     per-shape attestation: inventory, compute/HBM/collective probes, download
#     fetch_model   weights from the region-local replica (or the source) to a local directory
#     stage_model   Hugging Face -> the source weight bucket, no local disk
#   builds (same file, different --build-arg, see gpu_fleet.image_builds in parameters.json):
#     Hopper     torch 2.6 / CUDA 12.4
#     Blackwell  torch 2.8 / CUDA 12.8 (runs on Hopper too)
#
# BUILD CONTEXT IS THIS DIRECTORY: `docker build -f Dockerfile .`
ARG BASE=pytorch/pytorch:2.6.0-cuda12.4-cudnn9-runtime
FROM ${BASE}
ARG TORCH_SERIES=2.6
ARG CODE_VERSION=dev

# gcc: triton JIT-compiles a C module at import time and the runtime base has no compiler.
# curl: the EFA installer. iproute2: bench_gpu's interface inventory.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates gcc libc6-dev git iproute2 \
    && rm -rf /var/lib/apt/lists/*

# EFA userspace (libfabric + aws-ofi-nccl) for NCCL over the p5/p6 fabric. The kernel driver
# comes from the host AMI; --skip-kmod because a container cannot build kernel modules.
# Harmless on shapes without EFA.
RUN apt-get update \
    && cd /tmp \
    && curl -sO https://efa-installer.amazonaws.com/aws-efa-installer-latest.tar.gz \
    && tar xf aws-efa-installer-latest.tar.gz \
    && cd aws-efa-installer \
    && ./efa_installer.sh -y --skip-kmod --skip-limit-conf --no-verify \
    && cd / && rm -rf /tmp/aws-efa-installer* /var/lib/apt/lists/*

# aws-ofi-nccl dlopens the UNVERSIONED soname "libcudart.so", but the torch wheels ship only
# libcudart.so.12. Without the link the plugin fails to initialise and NCCL silently falls back
# to TCP sockets (~16 Gbps against a 3200 Gbps fabric) while every status signal stays green.
# LD_LIBRARY_PATH alone does not fix it; the soname must exist.
RUN CUDART_DIR="$(python -c 'import nvidia.cuda_runtime, os; print(os.path.join(os.path.dirname(nvidia.cuda_runtime.__file__), "lib"))')" \
    && ln -sf "$CUDART_DIR/libcudart.so.12" "$CUDART_DIR/libcudart.so" \
    && echo "$CUDART_DIR" > /etc/ld.so.conf.d/zz-cudart.conf \
    && ldconfig \
    && python -c "import ctypes; ctypes.CDLL('libcudart.so'); print('BUILD CHECK: dlopen libcudart.so OK')"
ENV LD_LIBRARY_PATH=/opt/amazon/efa/lib:/opt/amazon/openmpi/lib:$LD_LIBRARY_PATH
ENV PATH=/opt/amazon/efa/bin:$PATH

COPY model_transport /tmp/model_transport
RUN pip install --no-cache-dir "/tmp/model_transport[stage]" && rm -rf /tmp/model_transport

COPY local_disk.py bench_gpu.py gpu_fleet_entry.py /opt/gpu_fleet/
RUN install -m 0755 /opt/gpu_fleet/gpu_fleet_entry.py /usr/local/bin/gpu-fleet
ENV PYTHONPATH=/opt/gpu_fleet

# The build fails rather than ship an image whose torch drifted from the requested series, or
# whose baked-in tools do not import.
RUN python -c "import torch, sys; v = torch.__version__; \
print('BUILD CHECK: torch', v); sys.exit(0 if v.startswith('${TORCH_SERIES}.') else 1)" \
    && python -c "import model_transport, local_disk, bench_gpu; print('BUILD CHECK: fleet tools import OK')"

ENV PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
# stdout is block-buffered in a non-tty container; progress would not reach CloudWatch.
ENV PYTHONUNBUFFERED=1
ENV GPU_FLEET_CODE_VERSION=${CODE_VERSION}

WORKDIR /app
ENTRYPOINT ["gpu-fleet"]
