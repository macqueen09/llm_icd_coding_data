# Accurate and Explainable ICD-10 Coding Through Multi-Stage Model Adaptation and Evidence-Guided Verification

This repository contains the implementation for the paper: **"Accurate and Explainable ICD-10 Coding Through Multi-Stage Model Adaptation and Evidence-Guided Verification"**. The project focuses on fine-tuning Qwen2.5 models for ICD-10 coding tasks using supervised fine-tuning (SFT) and Chain-of-Thought (CoT) reasoning, with medical domain-specific data.

## Table of Contents
1. [Data Description](#1-data-description)
2. [Model Setup](#2-model-setup)
3. [Training and Inference](#3-training-and-inference)
4. [Environment Setup](#4-environment-setup)
5. [Notes](#5-notes)

---

## Data Description
- **`CoT_raw/llm_data_train_cot.csv`**: Raw Chain-of-Thought (CoT) data in CSV format. Requires preprocessing before training.
- **`ICD10-coding/`**:
  - **`ICD_Code_QA_new.json`**: ICD-10 coding question-answer pairs for training.
  - **`ICD_Full_CoT_{Train/Test}.json`**: ICD-10 training and test data with full CoT reasoning.
- **`Medical/`**: Medical domain-specific data organized by specialties (e.g., Cardiology, Endocrinology). Each file contains cases in JSONL format.

## Model Setup
- **Model Requirements**: Download Qwen2.5 model weights (either `Qwen2.5-32B-Instruct` or `Qwen2.5-72B-Instruct`) and place them in the `model/` directory.
- **Fine-Tuning Configuration**:
  - **`config/SFT.yaml`**: Main SFT configuration file.
  - **`config/ds_z3_offload_config.json`**: DeepSpeed zero-3 offload configuration for memory-efficient training.

## Environment Setup
```bash
pip install -r requirements.txt
```


## Training and Inference
###  Running SFT Training
```bash
bash run_sft.sh
```
This script performs:
1. Load base model.
2. Apply SFT configuration.
3. Train on specified datasets.
4. Save fine-tuned model to `save/`.

### Deploying the Model
```bash
bash start_xinference.sh
```
This script deploys the model using xInference for production inference.

### Running Inference
Test the fine-tuned model:
```bash
python inference.py
```

