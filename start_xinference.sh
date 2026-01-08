#!/bin/bash

# ==================================================
# Xinference Local Deployment Script
# Model: Qwen2.5-32B-Instruct
# Purpose: Start Xinference service and load the model
# ==================================================

# --- Configuration Section ---
# Please modify according to your local model path
MODEL_PATH="./save/Qwen2.5-32B-Instruct/full/Qwen2.5-32B-sft"

# API listening port
XINFERENCE_PORT=9997

# Model name (custom name for display)
MODEL_NAME="Qwen2.5-32B-sft"

# GPU count (auto-detected, can manually set if needed)
GPU_COUNT=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)

echo "🚀 Starting deployment of Qwen2.5-32B-Instruct model..."
echo "Model path: $MODEL_PATH"
echo "Detected GPU count: $GPU_COUNT"
echo "Xinference port: $XINFERENCE_PORT"

# --- Step 1: Start Xinference master service ---
echo -e "\n--- Step 1: Start Xinference service ---"

# Start Xinference master service in background
nohup xinference-local -H 0.0.0.0 -p $XINFERENCE_PORT > xinference_master.log 2>&1 &
MASTER_PID=$!
echo "Xinference service started. PID: $MASTER_PID, logs: xinference_master.log"

# Wait for the service to initialize (10 seconds)
echo "Waiting for service initialization..."
sleep 10

# --- Step 2: Register and launch the model ---
echo -e "\n--- Step 2: Register and launch the model ---"

# Note:
# --model-name: custom model name
# --model-path: local model folder
# --size-in-billions: model size (32B)
# --n-gpu: GPU allocation ("auto" or a number)
# --context-length: max context length
# If VRAM is limited, try quantization such as q4_0 or q8_0.

xinference launch \
  --model-name $MODEL_NAME \
  --model-path $MODEL_PATH \
  --size-in-billions 32 \
  --model-format pytorch \
  --quantization none \
  --n-gpu "auto" \
  --context-length 32768

# --- Final Info ---
echo -e "\n=================================================="
echo "✅ Deployment complete!"
echo "🌐 Xinference Dashboard: http://localhost:$XINFERENCE_PORT"
echo "💡 Model loading may take a few minutes. Check logs for status."
echo "📝 Log file: xinference_master.log"
echo "=================================================="

# Keep printing logs
tail -f xinference_master.log
