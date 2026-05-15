# Prompt Templates

This directory documents the prompt templates used or mirrored by the ICD-10-CM coding pipeline. The templates are provided to improve reproducibility of the manuscript revision and correspond to the prompts embedded in `get_mimic4_cot_data.py`, `vllm_offline_v3.py`, and `inference.py`.

The placeholders below are used consistently:

- `{clinical_note}`: de-identified MIMIC-IV discharge note text.
- `{icd_code_list}`: reference ICD-10-CM code-title pairs from the training labels.
- `{predicted_codes}`: ICD-10-CM codes predicted by the coding model.
- `{cot_rationales}`: generated coding rationales associated with predicted codes.
- `{retrieved_evidence}`: retrieved coding or clinical reference passages used by the verifier.

Files:

- `cot_generation_prompt.md`: prompt used to generate weak-supervision rationales for the stage-two SFT training split.
- `icd_inference_prompt.md`: prompt used for inference-time code reasoning and output formatting.
- `rag_verifier_prompt.md`: manuscript-level verifier prompt template for evidence-consistency filtering.
- `prompt_augmentation_templates.md`: representative ICD coding instruction variants used to reduce dependence on a single rigid prompt form.
