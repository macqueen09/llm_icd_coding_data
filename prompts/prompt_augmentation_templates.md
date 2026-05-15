# Representative Prompt Augmentation Templates

Purpose:
Provide representative ICD-10-CM instruction variants used to reduce overfitting to a single rigid wording pattern. The full training mixture uses multiple task-specific phrasings; these examples document the form and intent of the augmentation.

Template 1:

```text
You are a professional ICD-10-CM coder. Read the de-identified discharge note and assign appropriate diagnosis codes. For each code, provide a concise reason grounded in the clinical note.
```

Template 2:

```text
Based on the following hospital discharge documentation, identify the relevant ICD-10-CM diagnosis codes and explain the clinical evidence supporting each code.
```

Template 3:

```text
Analyze the patient record and produce ICD-10-CM code-title pairs. Include only diagnoses supported by the note, and provide the evidence used for each assignment.
```

Template 4:

```text
You are reviewing an inpatient encounter for diagnosis coding. Extract supported ICD-10-CM diagnoses from the note and justify each coding decision with patient-specific evidence.
```

Template 5:

```text
Given the clinical note, generate ICD-10-CM diagnosis coding results in code:name format, followed by a brief rationale for each code.
```

Output format:

```text
<ICD-10-CM code>:<diagnosis title>
Reason: <note-grounded rationale>
```
