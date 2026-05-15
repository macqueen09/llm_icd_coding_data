# RAG Retrieval Configuration and Metadata

This directory documents the reproducibility boundary of the evidence-guided verification module described in the manuscript.

The repository does not redistribute copyrighted guideline or textbook full text. It instead records the retrieval configuration and the metadata schema used to describe source documents and document chunks. Users should obtain restricted or copyrighted source documents through their own institutional or public access channels before rebuilding the index.

Files:

- `retrieval_config.json`: representative retrieval settings for the verifier, including encoder, index type, similarity metric, and top-k retrieval.
- `document_metadata_schema.json`: metadata fields used to track each source document and each retrievable chunk.

Source categories used by the manuscript:

- WHO ICD-10 hierarchy as the base international disease taxonomy.
- U.S. ICD-10-CM official coding guidelines as the operational morbidity coding rules for MIMIC-IV diagnosis coding.
- Publicly available or institutionally licensed clinical practice guidelines.
- Standard clinical textbooks used only as background references; full text is not redistributed here.
