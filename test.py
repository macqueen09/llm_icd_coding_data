# SPDX-License-Identifier: Apache-2.0

from vllm import LLM, SamplingParams
from vllm.distributed import cleanup_dist_env_and_memory

# NOTE: This is just a running example. For benchmarking purpose,
# please see benchmarks/benchmark_prefix_caching.py

import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"


# Common prefix.
prefix = (
    "You are an expert school principal, skilled in effectively managing "
    "faculty and staff. Draft 10-15 questions for a potential first grade "
    "Head Teacher for my K-12, all-girls', independent school that emphasizes "
    "community, joyful discovery, and life-long learning. The candidate is "
    "coming in for a first-round panel interview for a 8th grade Math "
    "teaching role. They have 5 years of previous teaching experience "
    "as an assistant teacher at a co-ed, public school with experience "
    "in middle school math teaching. Based on these information, fulfill "
    "the following paragraph: ")

# Sample prompts.
prompts = [
    "Hello, my name is",
    "The president of the United States is",
    "The capital of France is",
    "The future of AI is",
    "What do you like to do in your free time?",
    "Where did you grow up?",
    "What is your favorite book or movie and why?",
    "How do you think technology will change education in the next decade?",
    "Can you explain the concept of photosynthesis in simple terms?",
    "What are the key factors that influenced the outcome of the American Civil War?",
    "What motivated you to pursue a career in this field?",
    "How do you handle stress and pressure at work?",
    "What skills do you think are essential for success in business today?",
    "What strategies do you use to improve your English vocabulary?",
    "How do you practice listening skills in a foreign language?",
    "Can you share your experience with learning a new language?",
    "What are some traditional dishes from your country?",
    "How do cultural differences affect communication in a global workplace?",
    "What are some common customs or traditions during festivals in your culture?",
    "How do you think artificial intelligence will impact jobs in the future?",
    "What are your thoughts on renewable energy sources?",
    "How has technology changed the way we communicate?",
    "What steps can individuals take to reduce their carbon footprint?",
    "How do you think climate change will affect future generations?",
    "What are some effective ways to conserve water?"    
]

generating_prompts = [prefix + prompt for prompt in prompts]

# Create a sampling params object.
sampling_params = SamplingParams(temperature=0.0)

import time
def main():
    # Create an LLM without prefix caching as a baseline.
    model_path = '/supercloud/llm-data/hugmodels/modelscope/ddd/Qwen/Qwen2.5-32B-Instruct'
    regular_llm = LLM(model=model_path, gpu_memory_utilization=0.4)

    print("Results without `enable_prefix_caching`")

    # ruff: noqa: E501
    # Generate texts from the prompts. The output is a list of RequestOutput objects
    # that contain the prompt, generated text, and other information.
    outputs = regular_llm.generate(generating_prompts, sampling_params)

    regular_generated_texts = []
    # Print the outputs.
    print("-" * 50)
    for output in outputs:
        prompt = output.prompt
        generated_text = output.outputs[0].text
        regular_generated_texts.append(generated_text)
        time.sleep(1)
        #print(f"Prompt: {prompt!r}\nGenerated text: {generated_text!r}")
        #print("-" * 50)

    # Destroy the LLM object and free up the GPU memory.
    del regular_llm
    cleanup_dist_env_and_memory()


    ##################################################
    ################################################## 第二种方法，加入了enable_prefix_caching
    ##################################################
    print("\n\n\n")
    print("start method 2")
    print("\n\n\n")
    # Create an LLM with prefix caching enabled.
    prefix_cached_llm = LLM(model=model_path,
                            enable_prefix_caching=True,
                            gpu_memory_utilization=0.4)

    # Warmup so that the shared prompt's KV cache is computed.
    prefix_cached_llm.generate(generating_prompts[0], sampling_params)

    # Generate with prefix caching.
    outputs = prefix_cached_llm.generate(generating_prompts, sampling_params)

    print("Results with `enable_prefix_caching`")

    cached_generated_texts = []
    # Print the outputs. You should see the same outputs as before.
    print("-" * 50)
    for output in outputs:
        prompt = output.prompt
        generated_text = output.outputs[0].text
        cached_generated_texts.append(generated_text)
        #print(f"Prompt: {prompt!r}\nGenerated text: {generated_text!r}")
        #print("-" * 50)

    # Compare the results and display the speedup
    generated_same = all([
        regular_generated_texts[i] == cached_generated_texts[i]
        for i in range(len(prompts))
    ])
    print(f"Generated answers are the same: {generated_same}")


if __name__ == "__main__":
    main()
