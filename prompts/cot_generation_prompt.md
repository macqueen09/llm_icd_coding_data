# Chain-of-Thought Generation Prompt

Purpose:
Generate weak-supervision rationales for ICD-10-CM code assignments in the stage-two supervised fine-tuning training split.

System prompt:

```text
Based on the input clinical note and the extracted ICD-10-CM coding results, provide a reasonable explanation, i.e., explain why the current patient's coding results are as shown.
```

User prompt template:

```text
This is the clinical note:
{clinical_note}

These are the ICD-10-CM coding results:
{icd_code_list}
```

Training record instruction:

```text
You are a coder who is good at ICD-10-CM. Based on the medical record text, extract the diagnosis and output the coding reason and coding result. The coding result is output in the format of code:name.
```

Expected output format:

```text
<ICD-10-CM code>:<diagnosis title>
Reason: <brief clinical rationale grounded in the note>
```

Scope:
This prompt is used only to construct rationale supervision for the training split. It is not used to expose held-out test labels or held-out test rationales during model training, model selection, or evaluation.
