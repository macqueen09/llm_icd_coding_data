# Accurate and Explainable ICD-10-CM Coding Through Multi-Stage Model Adaptation and Evidence-Guided Verification


## Description
This repository contains the implementation for the paper: **"Accurate and Explainable ICD-10-CM Coding Through Multi-Stage Model Adaptation and Evidence-Guided Verification"**. The project focuses on fine-tuning Qwen2.5 models for ICD-10-CM coding tasks using supervised fine-tuning (SFT) and Chain-of-Thought (CoT) reasoning, with medical domain-specific data.

## Archival DOI
PeerJ requires a DOI for author-created code repositories. The archived software release is available through Zenodo at DOI: [10.5281/zenodo.20272769](https://doi.org/10.5281/zenodo.20272769).


##  Dataset Information
* **MIMIC-IV (v2.2)**: A large-scale EHR database from Beth Israel Deaconess Medical Center. 
**DOI**: [10.13026/6mm1-ek67](https://doi.org/10.13026/6mm1-ek67)
**Access**: Requires credentialed access via [PhysioNet](https://physionet.org/content/mimiciv/2.2/).

* **ICD-10-CM**: The official clinical modification for diagnosis coding. [Source: CDC](https://www.cdc.gov/nchs/icd/icd-10-cm/index.html).
* **MedBench**: A comprehensive benchmark for medical LLMs. [Source: OpenCompass](https://medbench.opencompass.org.cn).
* **Intermediate Training Set (`llm_data_train_cot.csv`)**: Contains 155,541 training samples with CoT reasoning. All PHI has been removed to comply with HIPAA.



## Code Information
- `get_mimic4_cot_data.py`: Script for extracting diagnostic evidence and formatting CoT prompts.
- `insert_cot_to_mysql.py`: Handles high-concurrency data storage for large-scale training.
- `inference.py`: Implementation of the evidence-guided verification and performance evaluation (Macro-F1, Micro-F1).
- `run_sft.sh`: Shell script for Supervised Fine-Tuning using DeepSpeed ZeRO-3.
- `prompts/`: Prompt templates for CoT generation, ICD-10-CM inference, RAG-based verification, and representative prompt augmentation.
- `rag/`: Retrieval configuration and document metadata schema for the RAG-based verifier. Copyrighted or restricted full-text guideline and textbook sources are not redistributed.



## Usage Instructions
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


## Requirements
### Environment Requirements
```bash
pip install -r requirements.txt
```
### Model Requirements
- Download Qwen2.5 model weights (either `Qwen2.5-32B-Instruct` or `Qwen2.5-72B-Instruct`) and place them in the `model/` directory.

- Fine-Turning Configuration:
  - `config/SFT.yaml`: Main SFT configuration file.
  - `config/ds_z3_offload_config.json`: DeepSpeed ZeRO-3 configuration for memory-efficient training.


## Methodology
The framework consists of three key components as detailed in the paper:
- **Multi-Stage Adaptation**: Step-wise fine-tuning from general medical logic to specific coding tasks.
- **Chain-of-Thought (CoT) Reasoning**: Synthesizing expert-like reasoning paths to explain the linkage between clinical notes and ICD-10-CM codes.
- **Verification Mechanism**: A specialized module that validates the consistency between the predicted code and the extracted evidence from clinical documents.
- **RAG Reproducibility Boundary**: The repository documents retrieval settings and metadata fields under `rag/`, while users must obtain restricted MIMIC-IV, guideline, or textbook source materials through the appropriate access channels.


## License & Data Use Agreement

### Code License
The source code in this repository is licensed under the **MIT License**. This permits free use, modification, and distribution for academic and commercial purposes, provided that the original copyright notice and this permission notice are included. See the [license](./license) file for the full text.

### Data Use Agreement & Privacy Compliance
This project involves the use of third-party clinical datasets. Access and use must comply with the following terms:

- **MIMIC-IV (v2.2) Database**: This research utilizes the MIMIC-IV database. Users are required to obtain their own credentialed access through [PhysioNet](https://physionet.org/) by completing the necessary CITI training. Redistribution of the raw database is strictly prohibited.
- **Processed Dataset (`llm_data_train_cot.csv`)**: 
    - The provided CSV file contains intermediate training data (Chain-of-Thought reasoning) derived from MIMIC-IV clinical notes.
    - **De-identification**: All data has been strictly de-identified in accordance with HIPAA standards. All Protected Health Information (PHI), such as names, specific dates, and contact information, has been masked (e.g., using `___`).
    - **Usage**: This processed file is provided solely for the purpose of reproducing the results presented in the paper. It must not be used to attempt to re-identify any individuals or for any purposes outside of the PhysioNet Data Use Agreement.

### Third-Party Resources
Other resources, such as **MedBench** and **ICD-10-CM** official classification files, remain the property of their respective owners and are subject to their specific licensing terms.


<!-- ## Citations
Please cite our paper if you find the code or methodology useful:

```bibtex
@article{qiu2025accurate,
  title={Accurate and Explainable ICD-10 Coding Through Multi-Stage Model Adaptation and Evidence-Guided Verification},
  author={Qiu, Jianhua and Qin, Zheng and Ma, Kaili and Hu, Pin and Zhang, Yeling and Xie, Xiaotong and Zhao, Wei and Wei, Xin},
  journal={PeerJ Computer Science},
  year={2025},
  note={Manuscript #228d3d, In Revision}
} -->
